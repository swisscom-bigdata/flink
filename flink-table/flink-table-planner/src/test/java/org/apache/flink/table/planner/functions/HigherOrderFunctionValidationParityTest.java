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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.RefinableLambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;
import org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parity harness ensuring that every built-in higher-order function rejects an invalid argument at
 * <b>validation time on both surfaces</b> — SQL and the Java Table API. Both now reach the same
 * {@link org.apache.flink.table.types.inference.InputTypeStrategy}, but they reach it along
 * different routes — SQL through Calcite's validator, the Table API through expression resolution —
 * and each route derives the lambda's parameter types itself, so a rule can still hold on one
 * surface and not on the other. Each {@link Case} below therefore carries an equivalent SQL string
 * and Table API expression for the same invalid call and asserts that both throw {@link
 * ValidationException}.
 *
 * <p>Every higher-order built-in must appear in {@link #cases()}; {@code
 * HigherOrderFunctionCoverageTest} enforces that. Adding a new argument rule should add a matching
 * negative case here, which forces the rule to be enforced on both surfaces.
 */
class HigherOrderFunctionValidationParityTest {

    private static final String REDUCER_MISMATCH =
            "The reducer of ARRAY_REDUCE must return a type assignable to the accumulator type ";

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW t AS SELECT "
                        + "ARRAY[1, 2, 3] AS arr_int, "
                        + "ARRAY['a', 'b'] AS arr_str, "
                        + "CAST(5 AS INT) AS an_int");
    }

    static Stream<Case> cases() {
        return Stream.of(
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_FILTER,
                        "predicate must be BOOLEAN",
                        "ARRAY_FILTER(arr_int, x -> x + 1)",
                        $("arr_int").arrayFilter(x -> x.plus(1)),
                        "BOOLEAN"),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_TRANSFORM,
                        "first argument must be an array",
                        "ARRAY_TRANSFORM(an_int, x -> x + 1)",
                        $("an_int").arrayTransform(x -> x.plus(1)),
                        // no lambda parameter type can be derived, so both surfaces report the
                        // call with the function's signature
                        "ARRAY_TRANSFORM(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->ANY)"),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_TRANSFORM,
                        "lambda argument must be a lambda expression",
                        "ARRAY_TRANSFORM(arr_int, an_int)",
                        call("ARRAY_TRANSFORM", $("arr_int"), $("an_int")),
                        "The argument at position 1 must be a lambda expression."),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "reducer result must be assignable to the accumulator",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS INT), (acc, x) -> acc > x)",
                        $("arr_int").arrayReduce(0, (acc, x) -> acc.isGreater(x)),
                        REDUCER_MISMATCH + "INT NOT NULL, but its body returns BOOLEAN NOT NULL."),
                // The accumulator is only widened without loss. Adding a DECIMAL with scale 18 to
                // a DECIMAL(38, 18) needs 39 digits, so the sum keeps only scale 17; the common
                // type DECIMAL(38, 17) would round every accumulated value.
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a DECIMAL reducer result that loses scale",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS DECIMAL(38, 18)), "
                                + "(acc, x) -> acc + CAST(x AS DECIMAL(20, 18)))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(0).cast(DataTypes.DECIMAL(38, 18)),
                                        (acc, x) -> acc.plus(x.cast(DataTypes.DECIMAL(20, 18)))),
                        REDUCER_MISMATCH
                                + "DECIMAL(38, 18) NOT NULL, but its body returns "
                                + "DECIMAL(38, 17) NOT NULL. The accumulator cannot be widened to "
                                + "hold the result without loss. Cast the reducer's result to "
                                + "DECIMAL(38, 18)."),
                // Widened once to DECIMAL(38, 18), after which the re-resolved body loses scale as
                // above; the message names both the widened and the initial type.
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a reducer result that does not fit the widened accumulator",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS DECIMAL(20, 18)), "
                                + "(acc, x) -> acc + CAST(x AS DECIMAL(20, 18)))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(0).cast(DataTypes.DECIMAL(20, 18)),
                                        (acc, x) -> acc.plus(x.cast(DataTypes.DECIMAL(20, 18)))),
                        REDUCER_MISMATCH
                                + "DECIMAL(38, 18) NOT NULL (widened from the initial value's type "
                                + "DECIMAL(20, 18) NOT NULL), but its body returns "
                                + "DECIMAL(38, 17) NOT NULL. The accumulator cannot be widened to "
                                + "hold the result without loss. Cast the reducer's result to "
                                + "DECIMAL(38, 18)."),
                // Their common type is DOUBLE, but the accumulator is not widened across type
                // families: an exact numeric accumulator does not silently become approximate.
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a DOUBLE reducer result for an INT accumulator",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS INT), (acc, x) -> CAST(x AS DOUBLE))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(0).cast(DataTypes.INT()),
                                        (acc, x) -> x.cast(DataTypes.DOUBLE())),
                        REDUCER_MISMATCH
                                + "INT NOT NULL, but its body returns DOUBLE NOT NULL. The "
                                + "accumulator is only widened within its type family. Cast the "
                                + "initial value to DOUBLE to accumulate in that type, or cast the "
                                + "reducer's result to INT."),
                // Calcite merges INT and TIME, which Flink cannot cast between at all; without
                // this rule the call would only fail during code generation.
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects an INT reducer result for a TIME accumulator",
                        "ARRAY_REDUCE(arr_int, TIME '12:00:00', (acc, x) -> x)",
                        $("arr_int").arrayReduce(lit(LocalTime.of(12, 0)), (acc, x) -> x),
                        REDUCER_MISMATCH + "TIME(0) NOT NULL, but its body returns INT NOT NULL."),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a TIMESTAMP_LTZ reducer result for a TIMESTAMP accumulator",
                        "ARRAY_REDUCE(arr_int, CAST(TIMESTAMP '2020-01-01 00:00:00' AS"
                                + " TIMESTAMP(3)), (acc, x) -> CAST(acc AS TIMESTAMP_LTZ(3)))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(LocalDateTime.of(2020, 1, 1, 0, 0))
                                                .cast(DataTypes.TIMESTAMP(3)),
                                        (acc, x) -> acc.cast(DataTypes.TIMESTAMP_LTZ(3))),
                        REDUCER_MISMATCH
                                + "TIMESTAMP(3) NOT NULL, but its body returns TIMESTAMP_LTZ(3) NOT NULL."),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_ZIP_WITH,
                        "second argument must be an array",
                        "ARRAY_ZIP_WITH(arr_int, an_int, (x, y) -> x)",
                        $("arr_int").arrayZipWith($("an_int"), (x, y) -> x),
                        "ARRAY_ZIP_WITH(array1 ARRAY, array2 ARRAY, "
                                + "lambda FUNCTION(ARRAY1_ELEMENT_TYPE, ARRAY2_ELEMENT_TYPE)->ANY)"),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_ZIP_WITH,
                        "lambda argument must be a lambda expression",
                        "ARRAY_ZIP_WITH(arr_int, arr_int, an_int)",
                        call("ARRAY_ZIP_WITH", $("arr_int"), $("arr_int"), $("an_int")),
                        "The argument at position 2 must be a lambda expression."));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void testRejectedOnSqlSurface(Case testCase) {
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT " + testCase.sqlExpression + " FROM t"))
                .as("SQL surface must reject: %s", testCase)
                .isInstanceOf(ValidationException.class)
                .satisfies(testCase::assertMessage);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void testRejectedOnTableApiSurface(Case testCase) {
        final Table t = tEnv.from("t");
        assertThatThrownBy(() -> t.select(testCase.tableApiExpression))
                .as("Table API surface must reject: %s", testCase)
                .isInstanceOf(ValidationException.class)
                .satisfies(testCase::assertMessage);
    }

    static final class Case {
        private final BuiltInFunctionDefinition definition;
        private final String rule;
        private final String sqlExpression;
        private final Expression tableApiExpression;
        private final @Nullable String expectedMessageFragment;

        private Case(
                BuiltInFunctionDefinition definition,
                String rule,
                String sqlExpression,
                Expression tableApiExpression,
                @Nullable String expectedMessageFragment) {
            this.definition = definition;
            this.rule = rule;
            this.sqlExpression = sqlExpression;
            this.tableApiExpression = tableApiExpression;
            this.expectedMessageFragment = expectedMessageFragment;
        }

        BuiltInFunctionDefinition getDefinition() {
            return definition;
        }

        private void assertMessage(Throwable t) {
            if (expectedMessageFragment != null) {
                Assertions.assertThat(stringify(t)).contains(expectedMessageFragment);
            }
        }

        private static String stringify(Throwable t) {
            final StringBuilder sb = new StringBuilder();
            for (Throwable cause = t; cause != null; cause = cause.getCause()) {
                if (cause.getMessage() != null) {
                    sb.append(cause.getMessage()).append('\n');
                }
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return definition.getName() + " " + rule;
        }
    }

    /**
     * A refinement that changes the number of lambda parameters is a bug in the function's input
     * type strategy. Both surfaces must report it before re-resolving the body; SQL used to fail
     * with an index error for too few types and to ignore surplus ones.
     */
    @ParameterizedTest(name = "refined to {0} parameter type(s)")
    @ValueSource(ints = {1, 3})
    void testRefinementChangingTheLambdaArityIsRejected(int refinedArity) {
        tEnv.loadModule("arity_changing", new ArityChangingReduceModule(refinedArity));
        final String expected =
                "Invalid input type strategy of function 'ARITY_CHANGING_REDUCE'. The refined "
                        + "lambda parameter types at position 2 must have 2 parameter(s) but "
                        + refinedArity
                        + " were returned.";

        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARITY_CHANGING_REDUCE(arr_int, 0, "
                                                + "(acc, x) -> acc + x) FROM t"))
                .isInstanceOf(ValidationException.class)
                .satisfies(t -> Assertions.assertThat(Case.stringify(t)).contains(expected));
        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                call(
                                                        "ARITY_CHANGING_REDUCE",
                                                        $("arr_int"),
                                                        0,
                                                        new UnresolvedLambdaExpression(
                                                                Arrays.asList("acc", "x"),
                                                                $("acc").plus($("x"))))))
                .isInstanceOf(ValidationException.class)
                .satisfies(t -> Assertions.assertThat(Case.stringify(t)).contains(expected));
    }

    /**
     * Provides {@code ARITY_CHANGING_REDUCE}. It must be a built-in, because lambda arguments are
     * not supported by user-defined functions yet.
     */
    private static final class ArityChangingReduceModule implements Module {
        private final BuiltInFunctionDefinition definition;

        private ArityChangingReduceModule(int refinedArity) {
            this.definition =
                    BuiltInFunctionDefinition.newBuilder()
                            .name("ARITY_CHANGING_REDUCE")
                            .kind(FunctionKind.SCALAR)
                            .inputTypeStrategy(new ArityChangingRefinement(refinedArity))
                            .outputTypeStrategy(SpecificTypeStrategies.ARRAY_REDUCE)
                            .runtimeProvided()
                            .build();
        }

        @Override
        public Set<String> listFunctions() {
            return Collections.singleton(definition.getName());
        }

        @Override
        public Optional<FunctionDefinition> getFunctionDefinition(String name) {
            return definition.getName().equalsIgnoreCase(name)
                    ? Optional.of(definition)
                    : Optional.empty();
        }
    }

    /**
     * The input type strategy of {@code ARRAY_REDUCE}, except that every refinement returns the
     * given number of lambda parameter types.
     */
    private static final class ArityChangingRefinement implements RefinableLambdaInputTypeStrategy {
        private static final RefinableLambdaInputTypeStrategy ARRAY_REDUCE_INPUT =
                (RefinableLambdaInputTypeStrategy) SpecificInputTypeStrategies.ARRAY_REDUCE_INPUT;

        private final int refinedArity;

        private ArityChangingRefinement(int refinedArity) {
            this.refinedArity = refinedArity;
        }

        @Override
        public ArgumentCount getArgumentCount() {
            return ARRAY_REDUCE_INPUT.getArgumentCount();
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            return ARRAY_REDUCE_INPUT.inferInputTypes(callContext, throwOnFailure);
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return ARRAY_REDUCE_INPUT.getExpectedSignatures(definition);
        }

        @Override
        public Optional<List<DataType>> getExpectedLambdaParameterTypes(
                CallContext callContext, int argumentPos) {
            return ARRAY_REDUCE_INPUT.getExpectedLambdaParameterTypes(callContext, argumentPos);
        }

        @Override
        public Optional<List<DataType>> adjustLambdaParameterTypes(
                CallContext callContext,
                int argumentPos,
                List<DataType> currentParameterTypes,
                DataType lambdaResultType) {
            return Optional.of(Collections.nCopies(refinedArity, currentParameterTypes.get(0)));
        }
    }
}
