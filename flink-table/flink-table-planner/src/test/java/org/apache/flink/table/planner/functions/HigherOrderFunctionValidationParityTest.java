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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
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

    private TableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW t AS SELECT "
                        + "ARRAY[1, 2, 3] AS arr_int, "
                        + "ARRAY['a', 'b'] AS arr_str, "
                        + "MAP[1, 10] AS map_int_int, "
                        + "MAP['k', 10] AS map_str_int, "
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
                        BuiltInFunctionDefinitions.MAP_FILTER,
                        "predicate must be BOOLEAN",
                        "MAP_FILTER(map_int_int, (k, v) -> k + v)",
                        $("map_int_int").mapFilter((k, v) -> k.plus(v)),
                        "BOOLEAN"),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_TRANSFORM,
                        "first argument must be an array",
                        "ARRAY_TRANSFORM(an_int, x -> x + 1)",
                        $("an_int").arrayTransform(x -> x.plus(1)),
                        null),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "reducer result must be assignable to the accumulator",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS INT), (acc, x) -> acc > x)",
                        $("arr_int").arrayReduce(0, (acc, x) -> acc.isGreater(x)),
                        null),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a narrowing DECIMAL reducer result",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS DECIMAL(5, 0)), "
                                + "(acc, x) -> CAST(acc AS DECIMAL(10, 2)))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(0).cast(DataTypes.DECIMAL(5, 0)),
                                        (acc, x) -> acc.cast(DataTypes.DECIMAL(10, 2))),
                        null),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a narrowing BIGINT reducer result",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS INT), (acc, x) -> CAST(acc AS BIGINT))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(0).cast(DataTypes.INT()),
                                        (acc, x) -> acc.cast(DataTypes.BIGINT())),
                        null),
                // Calcite's leastRestrictive treats FLOAT as the widest approximate type (per the
                // SQL standard, where FLOAT may be double precision), so it would accept this and
                // silently narrow the DOUBLE body into the FLOAT accumulator. Both surfaces use
                // Flink's own type merging instead, under which the common type is DOUBLE.
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects a DOUBLE reducer result for a FLOAT accumulator",
                        "ARRAY_REDUCE(arr_int, CAST(0 AS FLOAT), (acc, x) -> CAST(x AS DOUBLE))",
                        $("arr_int")
                                .arrayReduce(
                                        lit(0).cast(DataTypes.FLOAT()),
                                        (acc, x) -> x.cast(DataTypes.DOUBLE())),
                        null),
                // Calcite merges INT and TIME, which Flink cannot cast between at all; without
                // this rule the call would only fail during code generation.
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_REDUCE,
                        "rejects an INT reducer result for a TIME accumulator",
                        "ARRAY_REDUCE(arr_int, TIME '12:00:00', (acc, x) -> x)",
                        $("arr_int").arrayReduce(lit(LocalTime.of(12, 0)), (acc, x) -> x),
                        null),
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
                        null),
                new Case(
                        BuiltInFunctionDefinitions.MAP_ZIP_WITH,
                        "requires key types with a common type",
                        "MAP_ZIP_WITH(map_int_int, map_str_int, (k, v1, v2) -> v1)",
                        $("map_int_int").mapZipWith($("map_str_int"), (k, v1, v2) -> v1),
                        null),
                new Case(
                        BuiltInFunctionDefinitions.ARRAY_ZIP_WITH,
                        "second argument must be an array",
                        "ARRAY_ZIP_WITH(arr_int, an_int, (x, y) -> x)",
                        $("arr_int").arrayZipWith($("an_int"), (x, y) -> x),
                        null),
                new Case(
                        BuiltInFunctionDefinitions.MAP_TRANSFORM_KEYS,
                        "first argument must be a map",
                        "MAP_TRANSFORM_KEYS(an_int, (k, v) -> k)",
                        $("an_int").mapTransformKeys((k, v) -> k),
                        null),
                new Case(
                        BuiltInFunctionDefinitions.MAP_TRANSFORM_VALUES,
                        "first argument must be a map",
                        "MAP_TRANSFORM_VALUES(arr_int, (k, v) -> v)",
                        $("arr_int").mapTransformValues((k, v) -> v),
                        null));
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
}
