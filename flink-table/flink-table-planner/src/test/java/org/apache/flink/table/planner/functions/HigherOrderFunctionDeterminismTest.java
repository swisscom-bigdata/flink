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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the determinism analysis descends into the body of a higher-order function's lambda,
 * so a non-deterministic call inside the lambda makes the enclosing call non-deterministic.
 * Otherwise unsafe optimizations (CSE, filter push-down, result caching) could change results.
 *
 * <p>Covers all entry points, since Calcite's {@link
 * org.apache.calcite.rex.RexVisitorImpl#visitLambda} does not visit the body and each of them
 * traverses the expression tree on its own.
 *
 * <p>Scope: these are unit tests of the predicates over a hand-built {@link RexNode}. That a
 * planner rule actually consults them for a real query is pinned separately by {@code
 * NonDeterministicUpdateAnalyzerTest#testOverAggregateWithNonDeterminismInLambdaBody}; do not read
 * a green run here as an end-to-end guarantee.
 */
class HigherOrderFunctionDeterminismTest {

    /**
     * Stands in for a higher-order function. The subject of these tests is the descent into a
     * lambda operand, which is the same for every operator, so the call is built from a test
     * operator rather than from a particular built-in: those are bridged to their runtime class and
     * a bridged function is not available without a planner context.
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

    @Test
    void testCallIsDeterministicWithDeterministicLambda() {
        final RexNode call = higherOrderCall(deterministicLambdaBody());
        assertThat(RexUtil.isDeterministic(call)).isTrue();
    }

    @Test
    void testCallIsNonDeterministicWithNonDeterministicLambda() {
        final RexNode call = higherOrderCall(nonDeterministicLambdaBody());
        assertThat(RexUtil.isDeterministic(call)).isFalse();
    }

    @Test
    void testNonDeterministicCallNameOfLambdaBody() {
        assertThat(
                        FlinkRexUtil.getNonDeterministicCallName(
                                higherOrderCall(deterministicLambdaBody())))
                .isEmpty();
        assertThat(
                        FlinkRexUtil.getNonDeterministicCallName(
                                higherOrderCall(nonDeterministicLambdaBody())))
                .hasValue(FlinkSqlOperatorTable.RAND.getName());
    }

    /**
     * {@link ShortcutUtils#isDeterministicThroughProgram} resolves {@link RexLocalRef}s itself
     * instead of delegating to {@link RexUtil}, so it needs its own lambda descent -- including
     * through a local reference standing for the enclosing call.
     */
    @Test
    void testDeterminismThroughProgramOfLambdaBody() {
        final RexNode deterministic = higherOrderCall(deterministicLambdaBody());
        final RexNode nonDeterministic = higherOrderCall(nonDeterministicLambdaBody());
        final List<RexNode> exprs = Arrays.asList(deterministic, nonDeterministic);

        assertThat(ShortcutUtils.isDeterministicThroughProgram(deterministic, null)).isTrue();
        assertThat(ShortcutUtils.isDeterministicThroughProgram(nonDeterministic, null)).isFalse();
        assertThat(ShortcutUtils.isDeterministicThroughProgram(deterministic, exprs)).isTrue();
        assertThat(ShortcutUtils.isDeterministicThroughProgram(nonDeterministic, exprs)).isFalse();
        assertThat(
                        ShortcutUtils.isDeterministicThroughProgram(
                                new RexLocalRef(1, nonDeterministic.getType()), exprs))
                .isFalse();
    }

    /** {@code x -> x + 1} — a deterministic body. */
    private RexNode deterministicLambdaBody() {
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        return rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                element,
                rexBuilder.makeExactLiteral(BigDecimal.ONE, intType));
    }

    /** {@code x -> RAND()} — a non-deterministic body. */
    private RexNode nonDeterministicLambdaBody() {
        final RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        return rexBuilder.makeCall(doubleType, FlinkSqlOperatorTable.RAND, Collections.emptyList());
    }

    /**
     * Builds {@code HIGHER_ORDER_FUNCTION(<array>, <lambda>)} with an explicit result type to
     * bypass operand type inference, which is not the subject of this test.
     */
    private RexNode higherOrderCall(RexNode lambdaBody) {
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        final RelDataType arrayType = typeFactory.createArrayType(intType, -1);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode lambda =
                rexBuilder.makeLambdaCall(lambdaBody, Collections.singletonList(element));
        final RexNode array = rexBuilder.makeInputRef(arrayType, 0);
        return rexBuilder.makeCall(arrayType, HIGHER_ORDER_FUNCTION, Arrays.asList(array, lambda));
    }
}
