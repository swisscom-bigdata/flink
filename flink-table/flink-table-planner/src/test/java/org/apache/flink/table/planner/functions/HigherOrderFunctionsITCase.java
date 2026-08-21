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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.math.BigDecimal;
import java.util.stream.Stream;

/**
 * Tests for the built-in array higher-order functions ({@code ARRAY_TRANSFORM}, {@code
 * ARRAY_FILTER}, {@code ARRAY_REDUCE} and {@code ARRAY_ZIP_WITH}), which take a lambda argument.
 * These run in streaming mode; {@link HigherOrderFunctionsBatchITCase} covers the same functions in
 * batch mode.
 */
class HigherOrderFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                        arrayTransformTestCases(),
                        arrayTransformNestedCaptureTestCases(),
                        arrayFilterTestCases(),
                        arrayReduceTestCases(),
                        arrayZipWithTestCases(),
                        nestedLambdaAnyReturnTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> arrayTransformTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_TRANSFORM)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3},
                                null,
                                new Integer[] {1, null, 3},
                                new String[] {"a", "bb", "ccc"},
                                100,
                                1000)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING().notNull()),
                                DataTypes.INT(),
                                DataTypes.INT())
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, x -> x * 10)",
                                new Integer[] {10, 20, 30},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL array -> NULL
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f1, x -> x * 10)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL element is passed to the lambda and kept
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f2, x -> x * 10)",
                                new Integer[] {10, null, 30},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // result element type differs from the input element type
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f3, s -> CHAR_LENGTH(s))",
                                new Integer[] {1, 2, 3},
                                DataTypes.ARRAY(DataTypes.INT().notNull()))
                        // a built-in with custom type inference (IFNULL) applied to the lambda
                        // parameter: the parameter is transiently unresolved (a plain ANY) while
                        // the
                        // enclosing ARRAY_TRANSFORM binds it, so IFNULL's operand/return inference
                        // must defer rather than fail with "Type is not supported: ANY"
                        // (f2 = {1, null, 3}).
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f2, x -> IFNULL(x, 0))",
                                new Integer[] {1, 0, 3},
                                DataTypes.ARRAY(DataTypes.INT().notNull()))
                        // non-capturing nested higher-order function
                        .testSqlResult(
                                "ARRAY_TRANSFORM(ARRAY[ARRAY[1, 2], ARRAY[3]], "
                                        + "a -> ARRAY_TRANSFORM(a, x -> x + 1))",
                                new Integer[][] {new Integer[] {2, 3}, new Integer[] {4}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull())
                        // the lambda body closes over an outer column (f4 = 100)
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, x -> x + f4)",
                                new Integer[] {101, 102, 103},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // closing over two distinct outer columns (f4 = 100, f5 = 1000)
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, x -> x + f4 + f5)",
                                new Integer[] {1101, 1102, 1103},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // a lambda parameter shadows an outer column of the same name
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, f4 -> f4 + 1)",
                                new Integer[] {2, 3, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // a two-parameter lambda is not a valid transform argument
                        .testSqlValidationError(
                                "ARRAY_TRANSFORM(f0, (x, y) -> x)",
                                "The lambda expression at position 1 expects 1 parameter(s) but 2 were provided.")
                        // a lambda parameter name that collides with the reserved capture-parameter
                        // prefix is rejected (otherwise it would be miscounted as a lifted capture)
                        .testSqlValidationError(
                                "ARRAY_TRANSFORM(f0, `cap$x` -> `cap$x` + 1)",
                                "Lambda parameter name 'cap$x' is not allowed: "
                                        + "names beginning with 'cap$' are reserved."));
    }

    private Stream<TestSetSpec> arrayTransformNestedCaptureTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_TRANSFORM)
                        .onFieldsWithData(
                                new Integer[][] {new Integer[] {1, 2}, new Integer[] {3, 4}}, 1000)
                        .andDataTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ARRAY(DataTypes.INT().notNull()).notNull()),
                                DataTypes.INT())
                        // an inner lambda closes over the enclosing lambda's parameter (both
                        // surfaces)
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))",
                                new Integer[][] {new Integer[] {2, 3}, new Integer[] {6, 7}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).notNull()))
                        // capture through a non-element-access expression over the enclosing
                        // parameter
                        .testSqlResult(
                                "ARRAY_TRANSFORM(ARRAY[ARRAY[1, 2], ARRAY[3]], "
                                        + "a -> ARRAY_TRANSFORM(a, x -> x + CARDINALITY(a)))",
                                new Integer[][] {new Integer[] {3, 4}, new Integer[] {4}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull())
                        // a two-level capture threaded through an intermediate lambda
                        .testSqlResult(
                                "ARRAY_TRANSFORM(ARRAY[ARRAY[ARRAY[1, 2]]], "
                                        + "a -> ARRAY_TRANSFORM(a, "
                                        + "b -> ARRAY_TRANSFORM(b, x -> x + CARDINALITY(a))))",
                                new Integer[][][] {new Integer[][] {new Integer[] {2, 3}}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(
                                                                DataTypes.ARRAY(
                                                                                DataTypes.INT()
                                                                                        .notNull())
                                                                        .notNull())
                                                        .notNull())
                                        .notNull())
                        // a nested capture combined with an outer-column capture (f1 = 1000)
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, a -> ARRAY_TRANSFORM(a, x -> x + a[1] + f1))",
                                new Integer[][] {
                                    new Integer[] {1002, 1003}, new Integer[] {1006, 1007}
                                },
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).notNull()))
                        // an inner parameter shadows the enclosing parameter of the same name
                        .testSqlResult(
                                "ARRAY_TRANSFORM(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], "
                                        + "a -> ARRAY_TRANSFORM(a, a -> a + 1))",
                                new Integer[][] {new Integer[] {2, 3}, new Integer[] {4, 5}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull()));
    }

    private Stream<TestSetSpec> arrayFilterTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_FILTER)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3, 4}, null, new Integer[] {1, null, 3}, 2)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.INT())
                        .testSqlResult(
                                "ARRAY_FILTER(f0, x -> x > 2)",
                                new Integer[] {3, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // the predicate closes over an outer column (f3 = 2)
                        .testSqlResult(
                                "ARRAY_FILTER(f0, x -> x > f3)",
                                new Integer[] {3, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL array -> NULL
                        .testSqlResult(
                                "ARRAY_FILTER(f1, x -> x > 2)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // a NULL predicate result excludes the element
                        .testSqlResult(
                                "ARRAY_FILTER(f2, x -> x > 0)",
                                new Integer[] {1, 3},
                                DataTypes.ARRAY(DataTypes.INT())));
    }

    private Stream<TestSetSpec> arrayReduceTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REDUCE)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3}, null, new String[] {"a", "b", "c"}, 10)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING().notNull()),
                                DataTypes.INT())
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x)", 6, DataTypes.INT())
                        // the merge lambda closes over an outer column (f3 = 10), added once per
                        // element on top of the running accumulator
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x + f3)",
                                36,
                                DataTypes.INT())
                        // NULL array -> NULL
                        .testSqlResult(
                                "ARRAY_REDUCE(f1, 0, (acc, x) -> acc + x)", null, DataTypes.INT())
                        // empty array (produced by a filter) -> initial accumulator
                        .testSqlResult(
                                "ARRAY_REDUCE(ARRAY_FILTER(f0, x -> x > 100), 100, "
                                        + "(acc, x) -> acc + x)",
                                100,
                                DataTypes.INT())
                        .testSqlResult(
                                "ARRAY_REDUCE(f2, CAST('' AS STRING), (acc, x) -> acc || x)",
                                "abc",
                                DataTypes.STRING()),
                // A reducer body whose type is assignable to, but not identical with, the
                // accumulator type is accepted and converted to the accumulator type: the
                // accumulator (and therefore the result) type stays the one given by the initial
                // value. Types are compared with Flink's own type merging, so a DOUBLE body for a
                // FLOAT accumulator is rejected instead (see
                // HigherOrderFunctionValidationParityTest).
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REDUCE)
                        .onFieldsWithData((Object) new Integer[] {1, 2, 3})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, CAST(0 AS DECIMAL(12, 2)), (acc, x) -> x)",
                                new BigDecimal("3.00"),
                                DataTypes.DECIMAL(12, 2).notNull())
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, CAST(0 AS BIGINT), (acc, x) -> x)",
                                3L,
                                DataTypes.BIGINT().notNull()),
                // A reducer body that can produce NULL makes the result nullable even over a NOT
                // NULL array and NOT NULL initial accumulator, and the NULL is surfaced at runtime
                // rather than a primitive default. The array element nullability alone would not.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REDUCE)
                        .onFieldsWithData((Object) new Integer[] {1, 2, 3})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> CAST(NULL AS INT))",
                                null,
                                DataTypes.INT().nullable())
                        // an initial accumulator wider than the elements keeps its (wider) type;
                        // the
                        // reducer body is implicitly widened to the accumulator type
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, CAST(0 AS BIGINT), (acc, x) -> acc + x)",
                                6L,
                                DataTypes.BIGINT().notNull()),
                // A conditionally nullable reducer whose accumulator becomes NULL in an early
                // iteration must be able to observe that NULL later: the accumulator parameter is
                // inferred nullable in a second pass, so `acc IS NULL` is not folded to false.
                // Here acc=0,x=1 -> NULL, then acc=NULL,x=2 -> 99. Covered on both surfaces.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REDUCE)
                        .onFieldsWithData((Object) new Integer[] {1, 2})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> CASE WHEN x = 1 THEN NULL "
                                        + "WHEN acc IS NULL THEN 99 ELSE acc END)",
                                99,
                                DataTypes.INT().nullable())
                        // a reducer that never returns NULL keeps a NOT NULL accumulator and result
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x)",
                                3,
                                DataTypes.INT().notNull())
                        // a nullable initial accumulator yields a nullable result
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, CAST(NULL AS INT), (acc, x) -> acc + x)",
                                null,
                                DataTypes.INT().nullable()),
                // ARRAY_REDUCE accumulator assignment respects all logical-type attributes, not
                // just the type root: the reducer body is checked against the accumulator via
                // common-type derivation (honoring DECIMAL precision/scale), and codegen casts each
                // body result back to the accumulator type. Here the body is cast to the
                // accumulator's DECIMAL(10, 2) so it stays assignable; the rejecting direction (a
                // wider DECIMAL body into a narrower accumulator) is asserted in the parity
                // harness.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REDUCE)
                        .onFieldsWithData((Object) new Integer[] {1, 2, 3})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, CAST(0 AS DECIMAL(10, 2)), "
                                        + "(acc, x) -> CAST(acc + CAST(x AS DECIMAL(5, 0)) "
                                        + "AS DECIMAL(10, 2)))",
                                new java.math.BigDecimal("6.00"),
                                DataTypes.DECIMAL(10, 2).notNull()));
    }

    private Stream<TestSetSpec> arrayZipWithTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_ZIP_WITH)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3}, new Integer[] {10, 20}, null, 100)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.INT())
                        // element-wise combine; the shorter array is padded with NULL, so the
                        // result length is max(len1, len2) and the padded position (index 2, where
                        // f1 has no element) passes NULL, making x + y NULL there
                        .testSqlResult(
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + y)",
                                new Integer[] {11, 22, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // the lambda closes over an outer column (f3 = 100)
                        .testSqlResult(
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + y + f3)",
                                new Integer[] {111, 122, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL array -> NULL
                        .testSqlResult(
                                "ARRAY_ZIP_WITH(f2, f1, (x, y) -> x + y)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // the combine changes the element logical type (INT -> BIGINT); the output
                        // array element type must follow the lambda body type
                        .testSqlResult(
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> CAST(x AS BIGINT))",
                                new Long[] {1L, 2L, 3L},
                                DataTypes.ARRAY(DataTypes.BIGINT())));
    }

    /**
     * A higher-order call nested in another one's lambda body closes over the enclosing lambda's
     * parameter, so the inner call's return type is still {@code ANY} when its enclosing {@code
     * ARRAY_TRANSFORM} first derives its own result type. The enclosing function must defer
     * building its {@code ARRAY<...>} result type until the inner return type resolves, otherwise
     * validation fails with "Type is not supported: ANY" (see {@code
     * TypeInferenceOperandChecker#capturesUnresolvedLambdaParameter}). Covers the
     * lambda-result-derived return strategy of {@code ARRAY_ZIP_WITH}.
     */
    private Stream<TestSetSpec> nestedLambdaAnyReturnTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_ZIP_WITH)
                        .onFieldsWithData((Object) new Integer[] {5, 6})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // the inner ARRAY_ZIP_WITH body is the enclosing element x, unresolved
                        // until
                        // x is bound; each inner call returns a two-element array of x
                        .testSqlResult(
                                "ARRAY_TRANSFORM(f0, x -> "
                                        + "ARRAY_ZIP_WITH(ARRAY[1, 2], ARRAY[3, 4], (a, b) -> x))",
                                new Integer[][] {new Integer[] {5, 5}, new Integer[] {6, 6}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull()));
    }
}
