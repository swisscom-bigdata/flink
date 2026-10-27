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

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rel.core.CorrelationId;
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
import java.util.List;
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
    void testCorrelationIsLiftedOnlyWhenRequested() {
        // HIGHER_ORDER_FUNCTION(array#0, r -> r.f + $cor0.base): the body reads a field of its own
        // ROW parameter and a field of a correlation variable.
        final RelDataType rowType = typeFactory.builder().add("f", intType).build();
        final RelDataType correlationType = typeFactory.builder().add("base", intType).build();
        final RexLambdaRef element = new RexLambdaRef(0, "r", rowType);
        final RexNode correlationField =
                rexBuilder.makeFieldAccess(
                        rexBuilder.makeCorrel(correlationType, new CorrelationId(0)),
                        "base",
                        false);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rexBuilder.makeFieldAccess(element, "f", false),
                        correlationField);
        final RexCall call = higherOrderCall(body, element);

        // Lifting a single expression, such as a join condition during conversion, leaves the
        // correlation in the body; the lifting of the whole query that follows takes care of it.
        assertThat(call.accept(HigherOrderFunctionUtil.captureLiftingShuttle(rexBuilder)))
                .isEqualTo(call);

        // Lifting the whole query turns it into a trailing operand; the field of the parameter
        // stays.
        final RexCall lifted =
                (RexCall)
                        call.accept(
                                HigherOrderFunctionUtil.captureLiftingShuttle(rexBuilder, true));
        assertThat(lifted.getOperands()).hasSize(3);
        assertThat(lifted.getOperands().get(2)).isEqualTo(correlationField);
        final RexLambda liftedLambda = (RexLambda) lifted.getOperands().get(1);
        assertThat(liftedLambda.getParameters())
                .extracting(RexLambdaRef::getName)
                .containsExactly("r", "cap$cor$0");
        assertThat(liftedLambda.getExpression().toString()).contains("r.f").doesNotContain("$cor0");
    }

    @Test
    void testReliftKeepsEarlierCaptures() {
        // HIGHER_ORDER_FUNCTION(array#0, (x, cap#B) -> x + cap + col#1, col#2): an earlier lifting
        // captured col#2, and a later rewrite (such as decorrelation) put col#1 into the body.
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexLambdaRef earlierCapture =
                new RexLambdaRef(
                        HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE, "cap$2", intType);
        final RexNode newColumn = rexBuilder.makeInputRef(intType, 1);
        final RexNode earlierOperand = rexBuilder.makeInputRef(intType, 2);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, element, earlierCapture),
                        newColumn);
        final RexCall call =
                (RexCall)
                        rexBuilder.makeCall(
                                arrayType,
                                HIGHER_ORDER_FUNCTION,
                                Arrays.asList(
                                        rexBuilder.makeInputRef(arrayType, 0),
                                        rexBuilder.makeLambdaCall(
                                                body, Arrays.asList(element, earlierCapture)),
                                        earlierOperand));

        final RexCall lifted = HigherOrderFunctionUtil.liftCaptures(call, rexBuilder);

        assertThat(lifted.getOperands().subList(2, 4)).containsExactly(earlierOperand, newColumn);
        assertThat(((RexLambda) lifted.getOperands().get(1)).getParameters())
                .extracting(p -> p.getName() + "#" + p.getIndex())
                .containsExactly(
                        "x#0",
                        "cap$2#" + HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE,
                        "cap$1#" + (HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE + 1));
    }

    @Test
    void testReliftKeepsCaptureOperandsGroupedByLambda() {
        // Two lambdas, each with an earlier capture: col#2 for the first and col#4 for the second.
        // A new capture of the first lambda, col#3, joins its group before that of the second.
        final int base = HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE;
        final RexLambdaRef x = new RexLambdaRef(0, "x", intType);
        final RexLambdaRef firstCapture = new RexLambdaRef(base, "cap$2", intType);
        final RexLambdaRef y = new RexLambdaRef(0, "y", intType);
        final RexLambdaRef secondCapture = new RexLambdaRef(base, "cap$4", intType);
        final RexNode first =
                rexBuilder.makeLambdaCall(
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.PLUS,
                                rexBuilder.makeCall(SqlStdOperatorTable.PLUS, x, firstCapture),
                                rexBuilder.makeInputRef(intType, 3)),
                        Arrays.asList(x, firstCapture));
        final RexNode second =
                rexBuilder.makeLambdaCall(
                        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, y, secondCapture),
                        Arrays.asList(y, secondCapture));
        final RexCall call =
                (RexCall)
                        rexBuilder.makeCall(
                                arrayType,
                                HIGHER_ORDER_FUNCTION,
                                Arrays.asList(
                                        rexBuilder.makeInputRef(arrayType, 0),
                                        first,
                                        second,
                                        rexBuilder.makeInputRef(intType, 2),
                                        rexBuilder.makeInputRef(intType, 4)));

        final RexCall lifted = HigherOrderFunctionUtil.liftCaptures(call, rexBuilder);

        assertThat(lifted.getOperands().subList(3, 6))
                .containsExactly(
                        rexBuilder.makeInputRef(intType, 2),
                        rexBuilder.makeInputRef(intType, 3),
                        rexBuilder.makeInputRef(intType, 4));
        assertThat(lifted.getOperands().get(2)).isEqualTo(second);
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

    @Test
    void testProctimeIsLiftedOncePerRow() {
        // HIGHER_ORDER_FUNCTION(array#0, x -> x > 0 AND PROCTIME() = PROCTIME()): only the operator
        // materializes the processing time, so PROCTIME() is lifted like an OVER window, and both
        // occurrences share one capture.
        final RexNode proctime = proctime();
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.GREATER_THAN,
                                element,
                                rexBuilder.makeExactLiteral(BigDecimal.ZERO, intType)),
                        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, proctime, proctime));

        final RexCall lifted =
                HigherOrderFunctionUtil.liftCaptures(higherOrderCall(body, element), rexBuilder);

        assertThat(lifted.getOperands()).hasSize(3);
        assertThat(lifted.getOperands().get(2)).isEqualTo(proctime);
        assertThat(((RexLambda) lifted.getOperands().get(1)).getParameters())
                .extracting(RexLambdaRef::getName)
                .containsExactly("x", "cap$proctime$0");
    }

    @Test
    void testCurrentTimeFunctionIsLiftedOncePerRow() {
        // HIGHER_ORDER_FUNCTION(array#0, x -> CURRENT_TIMESTAMP): the operator takes the current
        // time once per row, so the call is lifted like PROCTIME().
        final RexNode currentTimestamp =
                rexBuilder.makeCall(
                        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
                        SqlStdOperatorTable.CURRENT_TIMESTAMP,
                        Collections.emptyList());
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);

        final RexCall lifted =
                HigherOrderFunctionUtil.liftCaptures(
                        higherOrderCall(currentTimestamp, element), rexBuilder);

        assertThat(lifted.getOperands()).hasSize(3);
        assertThat(lifted.getOperands().get(2)).isEqualTo(currentTimestamp);
        assertThat(((RexLambda) lifted.getOperands().get(1)).getParameters())
                .extracting(RexLambdaRef::getName)
                .containsExactly("x", "cap$time$0");
    }

    @Test
    void testCheckClosedRejectsProctimeInBody() {
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexCall call = higherOrderCall(proctime(), element);

        assertThatThrownBy(() -> HigherOrderFunctionUtil.checkClosed(call))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("still contains PROCTIME()")
                .hasMessageContaining("not closed");
    }

    private RexNode proctime() {
        return rexBuilder.makeCall(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
                FlinkSqlOperatorTable.PROCTIME,
                Collections.emptyList());
    }

    private RexCall higherOrderCall(RexNode lambdaBody, RexLambdaRef... parameters) {
        final RexNode array = rexBuilder.makeInputRef(arrayType, 0);
        final RexNode lambda = rexBuilder.makeLambdaCall(lambdaBody, Arrays.asList(parameters));
        return (RexCall)
                rexBuilder.makeCall(arrayType, HIGHER_ORDER_FUNCTION, Arrays.asList(array, lambda));
    }

    @Test
    void testWrittenFormPutsCapturesBackIntoNestedLambdas() {
        // HIGHER_ORDER_FUNCTION(a, x -> HIGHER_ORDER_FUNCTION(a, y -> x + y + col#1)): the inner
        // lambda captures x and col#1, and the outer one captures a and col#1.
        final RexLambdaRef x = new RexLambdaRef(0, "x", intType);
        final RexLambdaRef y = new RexLambdaRef(1, "y", intType);
        final RexNode innerBody =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, x, y),
                        rexBuilder.makeInputRef(intType, 1));
        final RexNode lifted =
                higherOrderCall(higherOrderCall(innerBody, y), x)
                        .accept(HigherOrderFunctionUtil.captureLiftingShuttle(rexBuilder));
        assertThat(lifted.toString()).contains("cap$x", "cap$1");

        assertThat(HigherOrderFunctionUtil.toWrittenForm(lifted, rexBuilder).toString())
                .isEqualTo(
                        "HIGHER_ORDER_FUNCTION($0, (x) -> "
                                + "HIGHER_ORDER_FUNCTION($0, (y) -> +(+(x, y), $1)))");
    }

    @Test
    void testWrittenFormDropsCastOfCaptureToTheTypeOfItsValue() {
        // HIGHER_ORDER_FUNCTION($0, (x, cap$1) -> CAST(cap$1 AS BIGINT), CAST($1 AS BIGINT)): the
        // value of the capture already has the type of the cast, as a captured time attribute has
        // after it is converted to a timestamp.
        final RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        final RexNode value = rexBuilder.makeCast(bigintType, rexBuilder.makeInputRef(intType, 1));
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexLambdaRef capture =
                new RexLambdaRef(
                        HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE, "cap$1", intType);
        final RexNode lambda =
                rexBuilder.makeLambdaCall(
                        rexBuilder.makeAbstractCast(bigintType, capture, false),
                        Arrays.asList(element, capture));
        final RexNode call =
                rexBuilder.makeCall(
                        arrayType,
                        HIGHER_ORDER_FUNCTION,
                        Arrays.asList(rexBuilder.makeInputRef(arrayType, 0), lambda, value));

        assertThat(HigherOrderFunctionUtil.toWrittenForm(call, rexBuilder).toString())
                .isEqualTo("HIGHER_ORDER_FUNCTION($0, (x) -> CAST($1):BIGINT NOT NULL)");
    }

    @Test
    void testWrittenFormShowsCorrelationFieldNamedLikeAParameter() {
        // HIGHER_ORDER_FUNCTION(a, x -> x + $cor0.x): the field after "$cor0." cannot be read as
        // the parameter x, so nothing is renamed.
        final RelDataType correlationType = typeFactory.builder().add("x", intType).build();
        final RexNode correlationField =
                rexBuilder.makeFieldAccess(
                        rexBuilder.makeCorrel(correlationType, new CorrelationId(0)), "x", false);
        final RexLambdaRef x = new RexLambdaRef(0, "x", intType);
        final RexNode lifted =
                higherOrderCall(
                                rexBuilder.makeCall(SqlStdOperatorTable.PLUS, x, correlationField),
                                x)
                        .accept(HigherOrderFunctionUtil.captureLiftingShuttle(rexBuilder, true));

        assertThat(HigherOrderFunctionUtil.toWrittenForm(lifted, rexBuilder).toString())
                .isEqualTo("HIGHER_ORDER_FUNCTION($0, (x) -> +(x, $cor0.x))");
    }

    @Test
    void testWrittenParameterNamesRenameOnlyConflictingParameters() {
        // (x, y, cap) -> HIGHER_ORDER_FUNCTION(cap, x_1 -> x_1 + x + y): a nested lambda declares
        // x_1, so x_1 is not a free name for x.
        final RexLambdaRef x = new RexLambdaRef(0, "x", intType);
        final RexLambdaRef y = new RexLambdaRef(1, "y", intType);
        final RexLambdaRef capture =
                new RexLambdaRef(
                        HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE, "cap$2", arrayType);
        final RexLambdaRef nested = new RexLambdaRef(0, "x_1", intType);
        final RexNode nestedLambda =
                rexBuilder.makeLambdaCall(
                        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, nested, nested),
                        Collections.singletonList(nested));
        final RexLambda lambda =
                (RexLambda)
                        rexBuilder.makeLambdaCall(
                                rexBuilder.makeCall(
                                        arrayType,
                                        HIGHER_ORDER_FUNCTION,
                                        Arrays.asList(capture, nestedLambda)),
                                Arrays.asList(x, y, capture));

        assertThat(HigherOrderFunctionUtil.writtenParameterNames(lambda, List.of("base")))
                .containsExactly("x", "y");
        assertThat(HigherOrderFunctionUtil.writtenParameterNames(lambda, List.of("x")))
                .containsExactly("x_2", "y");
        assertThat(HigherOrderFunctionUtil.writtenParameterNames(lambda, List.of("y + y_1")))
                .containsExactly("x", "y_2");
        assertThat(
                        HigherOrderFunctionUtil.writtenParameterNames(
                                lambda, List.of("$cor0.x", "r.y", "xs", "$0")))
                .containsExactly("x", "y");
    }

    @Test
    void testCaptureDedupMergesIdenticalOverWindows() {
        // HIGHER_ORDER_FUNCTION(a, x -> (w + x) + w) where both w are the *same* OVER window: the
        // two occurrences share a single lifted capture, so exactly one trailing operand is
        // appended.
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
        // only in order direction (ASC vs DESC). They are semantically distinct, so they must NOT
        // be merged into one capture -- both are appended as trailing operands.
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
