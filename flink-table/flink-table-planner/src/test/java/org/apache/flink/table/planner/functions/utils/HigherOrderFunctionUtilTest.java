/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.functions.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the closed-lambda invariant enforced by {@link HigherOrderFunctionUtil#liftCaptures} and
 * {@link HigherOrderFunctionUtil#checkClosed}: after capture lifting every {@link RexLambda} is
 * closed and 0-based, so downstream code generation and compiled-plan serialization never see a
 * lambda with a free outer column, an out-of-range parameter reference or a captured OVER
 * window/sub-query.
 */
class HigherOrderFunctionUtilTest {

    /**
     * Stands in for a higher-order function. Capture lifting is the same for every operator that
     * carries a lambda operand, and the built-ins are bridged to their runtime class, so no raw
     * operator is available without a planner context.
     */
    private static final SqlFunction HIGHER_ORDER_FUNCTION =
            new SqlFunction(
                    "HIGHER_ORDER_FUNCTION",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.ANY,
                    SqlFunctionCategory.SYSTEM);

    private final SqlTypeFactoryImpl typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    private final RelDataType arrayType = typeFactory.createArrayType(intType, -1);

    @Test
    void testLiftCapturesProducesClosedLambda() {
        // HIGHER_ORDER_FUNCTION(array#0, x -> x + col#1) — the lambda body closes over outer column
        // #1.
        final RexNode array = rexBuilder.makeInputRef(arrayType, 0);
        final RexNode outerColumn = rexBuilder.makeInputRef(intType, 1);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode body = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, element, outerColumn);
        final RexNode lambda = rexBuilder.makeLambdaCall(body, Collections.singletonList(element));
        final RexCall call =
                (RexCall)
                        rexBuilder.makeCall(
                                arrayType, HIGHER_ORDER_FUNCTION, Arrays.asList(array, lambda));

        final RexCall lifted = HigherOrderFunctionUtil.liftCaptures(call, rexBuilder);

        // The outer column is hoisted to a trailing operand and the lambda gains a matching
        // parameter, so the result is closed (liftCaptures already asserts this via checkClosed).
        assertThat(lifted.getOperands()).hasSize(3);
        assertThat(lifted.getOperands().get(2)).isEqualTo(outerColumn);
        final RexLambda liftedLambda = (RexLambda) lifted.getOperands().get(1);
        assertThat(liftedLambda.getParameters()).hasSize(2);
        assertThatCode(() -> HigherOrderFunctionUtil.checkClosed(lifted))
                .doesNotThrowAnyException();
    }

    @Test
    void testCheckClosedRejectsFreeOuterColumn() {
        final RexNode outerColumn = rexBuilder.makeInputRef(intType, 1);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode body = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, element, outerColumn);
        final RexCall call = higherOrderCall(body, element);

        assertThatThrownBy(() -> HigherOrderFunctionUtil.checkClosed(call))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("references outer column")
                .hasMessageContaining("not closed");
    }

    @Test
    void testCheckClosedRejectsOutOfRangeParameterReference() {
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexLambdaRef outOfRange = new RexLambdaRef(1, "y", intType);
        final RexNode body = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, element, outOfRange);
        final RexCall call = higherOrderCall(body, element);

        assertThatThrownBy(() -> HigherOrderFunctionUtil.checkClosed(call))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("references parameter #1")
                .hasMessageContaining("not one of its own parameters [0]")
                .hasMessageContaining("not closed");
    }

    @Test
    void testCheckClosedRejectsNonZeroBasedParameters() {
        final RexLambdaRef element = new RexLambdaRef(3, "x", intType);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        element,
                        rexBuilder.makeExactLiteral(BigDecimal.ONE, intType));
        final RexCall call = higherOrderCall(body, element);

        assertThatThrownBy(() -> HigherOrderFunctionUtil.checkClosed(call))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not in positional form");
    }

    private RexCall higherOrderCall(RexNode lambdaBody, RexLambdaRef... parameters) {
        final RexNode array = rexBuilder.makeInputRef(arrayType, 0);
        final RexNode lambda = rexBuilder.makeLambdaCall(lambdaBody, Arrays.asList(parameters));
        return (RexCall)
                rexBuilder.makeCall(arrayType, HIGHER_ORDER_FUNCTION, Arrays.asList(array, lambda));
    }

    @Test
    void testCaptureDedupMergesIdenticalOverWindows() {
        // HIGHER_ORDER_FUNCTION(a, x -> (w + x) + w) where both w are the *same* OVER window: the
        // two
        // occurrences share a single lifted capture, so exactly one trailing operand is appended.
        final RexNode window = sumOver(1, false);
        final RexNode windowAgain = sumOver(1, false);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, window, element),
                        windowAgain);
        final RexCall call = higherOrderCall(body, element);

        final RexCall lifted = HigherOrderFunctionUtil.liftCaptures(call, rexBuilder);

        // One capture: array operand + closed lambda + a single trailing window operand.
        assertThat(lifted.getOperands()).hasSize(3);
        assertThat(lifted.getOperands().get(2)).isEqualTo(window);
        assertThat(((RexLambda) lifted.getOperands().get(1)).getParameters()).hasSize(2);
    }

    @Test
    void testCaptureDedupKeepsDistinctOverWindowsSeparate() {
        // HIGHER_ORDER_FUNCTION(a, x -> (w1 + x) + w2) where w1 and w2 are two windows that differ
        // only
        // in
        // order direction (ASC vs DESC). They are semantically distinct, so they must NOT be merged
        // into one capture -- both are appended as trailing operands.
        final RexNode ascWindow = sumOver(1, false);
        final RexNode descWindow = sumOver(1, true);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, ascWindow, element),
                        descWindow);
        final RexCall call = higherOrderCall(body, element);

        final RexCall lifted = HigherOrderFunctionUtil.liftCaptures(call, rexBuilder);

        // Two captures: array operand + closed lambda + two distinct trailing window operands.
        assertThat(lifted.getOperands()).hasSize(4);
        assertThat(lifted.getOperands().subList(2, 4))
                .containsExactlyInAnyOrder(ascWindow, descWindow);
        assertThat(((RexLambda) lifted.getOperands().get(1)).getParameters()).hasSize(3);
    }

    /**
     * {@code SUM(col#0) OVER (PARTITION BY col#0 ORDER BY col#1 [DESC])} — a whole-query window.
     */
    private RexNode sumOver(int orderColumn, boolean descending) {
        final RexNode partitionKey = rexBuilder.makeInputRef(intType, 0);
        final RexNode orderExpr = rexBuilder.makeInputRef(intType, orderColumn);
        final Set<SqlKind> directions =
                descending ? Collections.singleton(SqlKind.DESCENDING) : Collections.emptySet();
        return rexBuilder.makeOver(
                intType,
                SqlStdOperatorTable.SUM,
                Collections.singletonList(rexBuilder.makeInputRef(intType, 0)),
                Collections.singletonList(partitionKey),
                com.google.common.collect.ImmutableList.of(
                        new RexFieldCollation(orderExpr, directions)),
                RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW,
                true,
                true,
                false,
                false,
                false);
    }
}
