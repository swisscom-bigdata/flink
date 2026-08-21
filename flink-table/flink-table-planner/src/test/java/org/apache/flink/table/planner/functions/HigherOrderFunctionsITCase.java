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
import org.apache.flink.util.CollectionUtil;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.ifThenElse;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.util.CollectionUtil.entry;

/**
 * Tests for the built-in array and map higher-order functions ({@code ARRAY_TRANSFORM}, {@code
 * ARRAY_FILTER}, {@code ARRAY_REDUCE}, {@code ARRAY_ZIP_WITH}, {@code MAP_FILTER}, {@code
 * MAP_TRANSFORM_KEYS}, {@code MAP_TRANSFORM_VALUES} and {@code MAP_ZIP_WITH}), which take a lambda
 * argument. Both the SQL and the Table API (host-language lambda) surfaces are covered. These run
 * in streaming mode; {@link HigherOrderFunctionsBatchITCase} covers the same functions in batch
 * mode.
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
                        mapFilterTestCases(),
                        mapTransformKeysTestCases(),
                        mapTransformValuesTestCases(),
                        mapZipWithTestCases(),
                        mapLogicalKeyEqualityTestCases(),
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
                        .testResult(
                                $("f0").arrayTransform(x -> x.times(10)),
                                "ARRAY_TRANSFORM(f0, x -> x * 10)",
                                new Integer[] {10, 20, 30},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL array -> NULL
                        .testResult(
                                $("f1").arrayTransform(x -> x.times(10)),
                                "ARRAY_TRANSFORM(f1, x -> x * 10)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL element is passed to the lambda and kept
                        .testResult(
                                $("f2").arrayTransform(x -> x.times(10)),
                                "ARRAY_TRANSFORM(f2, x -> x * 10)",
                                new Integer[] {10, null, 30},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // result element type differs from the input element type
                        .testResult(
                                $("f3").arrayTransform(s -> s.charLength()),
                                "ARRAY_TRANSFORM(f3, s -> CHAR_LENGTH(s))",
                                new Integer[] {1, 2, 3},
                                DataTypes.ARRAY(DataTypes.INT().notNull()))
                        // a built-in with custom type inference (IFNULL) applied to the lambda
                        // parameter: the parameter is transiently unresolved (a plain ANY) while
                        // the
                        // enclosing ARRAY_TRANSFORM binds it, so IFNULL's operand/return inference
                        // must defer rather than fail with "Type is not supported: ANY". Covers the
                        // SQL, Table API, and API-rendered-as-SQL surfaces (f2 = {1, null, 3}).
                        .testResult(
                                $("f2").arrayTransform(x -> x.ifNull(0)),
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
                        .testResult(
                                $("f0").arrayTransform(x -> x.plus($("f4"))),
                                "ARRAY_TRANSFORM(f0, x -> x + f4)",
                                new Integer[] {101, 102, 103},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // closing over two distinct outer columns (f4 = 100, f5 = 1000)
                        .testResult(
                                $("f0").arrayTransform(x -> x.plus($("f4")).plus($("f5"))),
                                "ARRAY_TRANSFORM(f0, x -> x + f4 + f5)",
                                new Integer[] {1101, 1102, 1103},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // a lambda parameter shadows an outer column of the same name (SQL only,
                        // since the Table API generates lambda parameter names automatically)
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
                        .testResult(
                                $("f0").arrayTransform(a -> a.arrayTransform(x -> x.plus(a.at(1)))),
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
                        .testResult(
                                $("f0").arrayTransform(
                                                a ->
                                                        a.arrayTransform(
                                                                x ->
                                                                        x.plus(a.at(1))
                                                                                .plus($("f1")))),
                                "ARRAY_TRANSFORM(f0, a -> ARRAY_TRANSFORM(a, x -> x + a[1] + f1))",
                                new Integer[][] {
                                    new Integer[] {1002, 1003}, new Integer[] {1006, 1007}
                                },
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).notNull()))
                        // an inner parameter shadows the enclosing parameter of the same name (SQL
                        // only, since the Table API generates lambda parameter names automatically)
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
                        .testResult(
                                $("f0").arrayFilter(x -> x.isGreater(2)),
                                "ARRAY_FILTER(f0, x -> x > 2)",
                                new Integer[] {3, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // the predicate closes over an outer column (f3 = 2)
                        .testResult(
                                $("f0").arrayFilter(x -> x.isGreater($("f3"))),
                                "ARRAY_FILTER(f0, x -> x > f3)",
                                new Integer[] {3, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL array -> NULL
                        .testResult(
                                $("f1").arrayFilter(x -> x.isGreater(2)),
                                "ARRAY_FILTER(f1, x -> x > 2)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // a NULL predicate result excludes the element
                        .testResult(
                                $("f2").arrayFilter(x -> x.isGreater(0)),
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
                        .testResult(
                                $("f0").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x)",
                                6,
                                DataTypes.INT())
                        // the merge lambda closes over an outer column (f3 = 10), added once per
                        // element on top of the running accumulator
                        .testResult(
                                $("f0").arrayReduce(lit(0), (acc, x) -> acc.plus(x).plus($("f3"))),
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x + f3)",
                                36,
                                DataTypes.INT())
                        // NULL array -> NULL
                        .testResult(
                                $("f1").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f1, 0, (acc, x) -> acc + x)",
                                null,
                                DataTypes.INT())
                        // empty array (produced by a filter) -> initial accumulator
                        .testResult(
                                $("f0").arrayFilter(x -> x.isGreater(100))
                                        .arrayReduce(lit(100), (acc, x) -> acc.plus(x)),
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
                        .testResult(
                                $("f0").arrayReduce(
                                                lit(new BigDecimal("0.00"))
                                                        .cast(DataTypes.DECIMAL(12, 2)),
                                                (acc, x) -> x),
                                "ARRAY_REDUCE(f0, CAST(0 AS DECIMAL(12, 2)), (acc, x) -> x)",
                                new BigDecimal("3.00"),
                                DataTypes.DECIMAL(12, 2).notNull())
                        .testResult(
                                $("f0").arrayReduce(lit(0L), (acc, x) -> x),
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
                        .testResult(
                                $("f0").arrayReduce(
                                                lit(0),
                                                (acc, x) ->
                                                        ifThenElse(
                                                                x.isEqual(1),
                                                                nullOf(DataTypes.INT()),
                                                                ifThenElse(
                                                                        acc.isNull(),
                                                                        lit(99),
                                                                        acc))),
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> CASE WHEN x = 1 THEN NULL "
                                        + "WHEN acc IS NULL THEN 99 ELSE acc END)",
                                99,
                                DataTypes.INT().nullable())
                        // a reducer that never returns NULL keeps a NOT NULL accumulator and result
                        .testResult(
                                $("f0").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x)",
                                3,
                                DataTypes.INT().notNull())
                        // a nullable initial accumulator yields a nullable result
                        .testResult(
                                $("f0").arrayReduce(
                                                nullOf(DataTypes.INT()), (acc, x) -> acc.plus(x)),
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
                        .testResult(
                                $("f0").arrayReduce(
                                                lit(0).cast(DataTypes.DECIMAL(10, 2)),
                                                (acc, x) ->
                                                        acc.plus(x.cast(DataTypes.DECIMAL(5, 0)))
                                                                .cast(DataTypes.DECIMAL(10, 2))),
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
                        .testResult(
                                $("f0").arrayZipWith($("f1"), (x, y) -> x.plus(y)),
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + y)",
                                new Integer[] {11, 22, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // the lambda closes over an outer column (f3 = 100)
                        .testResult(
                                $("f0").arrayZipWith($("f1"), (x, y) -> x.plus(y).plus($("f3"))),
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + y + f3)",
                                new Integer[] {111, 122, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // NULL array -> NULL
                        .testResult(
                                $("f2").arrayZipWith($("f1"), (x, y) -> x.plus(y)),
                                "ARRAY_ZIP_WITH(f2, f1, (x, y) -> x + y)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // the combine changes the element logical type (INT -> BIGINT); the output
                        // array element type must follow the lambda body type
                        .testResult(
                                $("f0").arrayZipWith($("f1"), (x, y) -> x.cast(DataTypes.BIGINT())),
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> CAST(x AS BIGINT))",
                                new Long[] {1L, 2L, 3L},
                                DataTypes.ARRAY(DataTypes.BIGINT())));
    }

    private Stream<TestSetSpec> mapFilterTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_FILTER)
                        .onFieldsWithData(CollectionUtil.map(entry("a", 1), entry("b", 2)), null, 1)
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.INT())
                        .testResult(
                                $("f0").mapFilter((k, v) -> v.isGreater(1)),
                                "MAP_FILTER(f0, (k, v) -> v > 1)",
                                CollectionUtil.map(entry("b", 2)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // the predicate closes over an outer column (f2 = 1)
                        .testResult(
                                $("f0").mapFilter((k, v) -> v.isGreater($("f2"))),
                                "MAP_FILTER(f0, (k, v) -> v > f2)",
                                CollectionUtil.map(entry("b", 2)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // NULL map -> NULL
                        .testResult(
                                $("f1").mapFilter((k, v) -> v.isGreater(1)),
                                "MAP_FILTER(f1, (k, v) -> v > 1)",
                                null,
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                // A predicate that evaluates to NULL (here because the value is NULL) excludes the
                // entry, matching ARRAY_FILTER. MAP_FILTER uses a separate generated loop, so it is
                // covered explicitly on both SQL and Table API.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_FILTER)
                        .onFieldsWithData(
                                CollectionUtil.map(entry("a", 1), entry("b", null), entry("c", 3)))
                        .andDataTypes(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().nullable()))
                        .testResult(
                                $("f0").mapFilter((k, v) -> v.isGreater(1)),
                                "MAP_FILTER(f0, (k, v) -> v > 1)",
                                CollectionUtil.map(entry("c", 3)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().nullable())));
    }

    private Stream<TestSetSpec> mapTransformKeysTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_TRANSFORM_KEYS)
                        .onFieldsWithData(
                                CollectionUtil.map(entry(1, "a"), entry(2, "b")), null, 100)
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                DataTypes.INT())
                        // each key is replaced by the lambda body; values unchanged
                        .testResult(
                                $("f0").mapTransformKeys((k, v) -> k.plus(10)),
                                "MAP_TRANSFORM_KEYS(f0, (k, v) -> k + 10)",
                                CollectionUtil.map(entry(11, "a"), entry(12, "b")),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        // the key transform closes over an outer column (f2 = 100)
                        .testResult(
                                $("f0").mapTransformKeys((k, v) -> k.plus($("f2"))),
                                "MAP_TRANSFORM_KEYS(f0, (k, v) -> k + f2)",
                                CollectionUtil.map(entry(101, "a"), entry(102, "b")),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        // NULL map -> NULL
                        .testResult(
                                $("f1").mapTransformKeys((k, v) -> k.plus(10)),
                                "MAP_TRANSFORM_KEYS(f1, (k, v) -> k + 10)",
                                null,
                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        // the key transform changes the key logical type (INT -> STRING); the
                        // output map key type must follow the lambda body type
                        .testResult(
                                $("f0").mapTransformKeys(
                                                (k, v) -> k.plus(10).cast(DataTypes.STRING())),
                                "MAP_TRANSFORM_KEYS(f0, (k, v) -> CAST(k + 10 AS STRING))",
                                CollectionUtil.map(entry("11", "a"), entry("12", "b")),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
    }

    private Stream<TestSetSpec> mapTransformValuesTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_TRANSFORM_VALUES)
                        .onFieldsWithData(
                                CollectionUtil.map(entry("a", 1), entry("b", 2)), null, 100)
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.INT())
                        // each value is replaced by the lambda body; keys unchanged
                        .testResult(
                                $("f0").mapTransformValues((k, v) -> v.times(10)),
                                "MAP_TRANSFORM_VALUES(f0, (k, v) -> v * 10)",
                                CollectionUtil.map(entry("a", 10), entry("b", 20)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // the value transform closes over an outer column (f2 = 100)
                        .testResult(
                                $("f0").mapTransformValues((k, v) -> v.plus($("f2"))),
                                "MAP_TRANSFORM_VALUES(f0, (k, v) -> v + f2)",
                                CollectionUtil.map(entry("a", 101), entry("b", 102)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // NULL map -> NULL
                        .testResult(
                                $("f1").mapTransformValues((k, v) -> v.times(10)),
                                "MAP_TRANSFORM_VALUES(f1, (k, v) -> v * 10)",
                                null,
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // the value transform changes the value logical type (INT -> DOUBLE); the
                        // output map value type must follow the lambda body type
                        .testResult(
                                $("f0").mapTransformValues((k, v) -> v.cast(DataTypes.DOUBLE())),
                                "MAP_TRANSFORM_VALUES(f0, (k, v) -> CAST(v AS DOUBLE))",
                                CollectionUtil.map(entry("a", 1.0), entry("b", 2.0)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE())));
    }

    private Stream<TestSetSpec> mapZipWithTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData(
                                CollectionUtil.map(entry("a", 1), entry("b", 2)),
                                CollectionUtil.map(entry("a", 10), entry("c", 30)),
                                null,
                                100,
                                CollectionUtil.map(entry("x", 1), entry("y", 2)),
                                CollectionUtil.map(entry("p", 10), entry("q", 20)),
                                CollectionUtil.map(entry("a", 100), entry("b", 200)))
                        .onFieldsWithData(
                                CollectionUtil.map(entry("a", 1), entry("b", 2)),
                                CollectionUtil.map(entry("a", 10), entry("c", 30)),
                                null,
                                100,
                                CollectionUtil.map(entry("x", 1), entry("y", 2)),
                                CollectionUtil.map(entry("p", 10), entry("q", 20)),
                                CollectionUtil.map(entry("a", 100), entry("b", 200)))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.INT(),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // merge over the union of keys; an absent key passes NULL for that side, so
                        // a key present in only one map yields a NULL value (v1 + v2 with a NULL
                        // operand is NULL)
                        .testResult(
                                $("f0").mapZipWith($("f1"), (k, v1, v2) -> v1.plus(v2)),
                                "MAP_ZIP_WITH(f0, f1, (k, v1, v2) -> v1 + v2)",
                                CollectionUtil.map(
                                        entry("a", 11), entry("b", null), entry("c", null)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // the merge lambda closes over an outer column (f3 = 100)
                        .testResult(
                                $("f0").mapZipWith(
                                                $("f1"), (k, v1, v2) -> v1.plus(v2).plus($("f3"))),
                                "MAP_ZIP_WITH(f0, f1, (k, v1, v2) -> v1 + v2 + f3)",
                                CollectionUtil.map(
                                        entry("a", 111), entry("b", null), entry("c", null)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // NULL map -> NULL
                        .testResult(
                                $("f2").mapZipWith($("f1"), (k, v1, v2) -> v1.plus(v2)),
                                "MAP_ZIP_WITH(f2, f1, (k, v1, v2) -> v1 + v2)",
                                null,
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // the merge changes the value logical type (INT -> BIGINT); the output map
                        // value type must follow the lambda body type
                        .testResult(
                                $("f0").mapZipWith(
                                                $("f1"),
                                                (k, v1, v2) -> v1.cast(DataTypes.BIGINT())),
                                "MAP_ZIP_WITH(f0, f1, (k, v1, v2) -> CAST(v1 AS BIGINT))",
                                CollectionUtil.map(
                                        entry("a", 1L), entry("b", 2L), entry("c", null)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                        // fully disjoint key sets: the result is the union of both key sets and
                        // every entry sees a NULL on exactly one side, so each map contributes its
                        // own value untouched
                        .testResult(
                                $("f4").mapZipWith($("f5"), (k, v1, v2) -> v1.ifNull(v2)),
                                "MAP_ZIP_WITH(f4, f5, (k, v1, v2) -> IFNULL(v1, v2))",
                                CollectionUtil.map(
                                        entry("x", 1),
                                        entry("y", 2),
                                        entry("p", 10),
                                        entry("q", 20)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // identical key sets: no key is absent on either side, so no NULL is passed
                        // to the lambda and the result has exactly the shared keys
                        .testResult(
                                $("f0").mapZipWith($("f6"), (k, v1, v2) -> v1.plus(v2)),
                                "MAP_ZIP_WITH(f0, f6, (k, v1, v2) -> v1 + v2)",
                                CollectionUtil.map(entry("a", 101), entry("b", 202)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData(
                                CollectionUtil.map(entry(1, 1), entry(2, 2)),
                                CollectionUtil.map(entry(1L, 10L), entry(3L, 30L)),
                                CollectionUtil.map(entry(new BigDecimal("1.20"), 1)),
                                CollectionUtil.map(entry(new BigDecimal("1.200"), 2)))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                                DataTypes.MAP(DataTypes.DECIMAL(4, 2), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.DECIMAL(6, 3), DataTypes.INT()))
                        // compatible key types are generalized to their common type (like for
                        // MAP_UNION) and both maps are cast to it, so key 1 (INT) and key 1
                        // (BIGINT) are the same key of the merged map
                        .testResult(
                                $("f0").mapZipWith(
                                                $("f1"),
                                                (k, v1, v2) ->
                                                        v1.ifNull(lit(0L))
                                                                .plus(v2.ifNull(lit(0L)))),
                                "MAP_ZIP_WITH(f0, f1, (k, v1, v2) ->"
                                        + " IFNULL(v1, CAST(0 AS BIGINT)) + IFNULL(v2, CAST(0 AS BIGINT)))",
                                CollectionUtil.map(entry(1L, 11L), entry(2L, 2L), entry(3L, 30L)),
                                DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT().notNull()))
                        // the same generalization for DECIMALs of different precision and scale:
                        // 1.20 and 1.200 become the same DECIMAL(6, 3) key
                        .testResult(
                                $("f2").mapZipWith(
                                                $("f3"),
                                                (k, v1, v2) ->
                                                        v1.ifNull(lit(0)).plus(v2.ifNull(lit(0)))),
                                "MAP_ZIP_WITH(f2, f3, (k, v1, v2) -> IFNULL(v1, 0) + IFNULL(v2, 0))",
                                CollectionUtil.map(entry(new BigDecimal("1.200"), 3)),
                                DataTypes.MAP(DataTypes.DECIMAL(6, 3), DataTypes.INT().notNull())),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData(
                                CollectionUtil.map(entry("a", 1), entry("b", 2)),
                                CollectionUtil.map(entry((String) null, 1), entry("a", 2)),
                                CollectionUtil.map(entry((String) null, 10), entry("b", 20)))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // an empty map cannot be passed as input data (fromValues cannot infer the
                        // type of an empty map literal), so it is derived with a filter that keeps
                        // nothing
                        // both maps empty: the key union is empty, so the lambda is never applied
                        .testResult(
                                $("f0").mapFilter((k, v) -> lit(false))
                                        .mapZipWith(
                                                $("f0").mapFilter((k, v) -> lit(false)),
                                                (k, v1, v2) -> v1.plus(v2)),
                                "MAP_ZIP_WITH(MAP_FILTER(f0, (k, v) -> FALSE), "
                                        + "MAP_FILTER(f0, (k, v) -> FALSE), (k, v1, v2) -> v1 + v2)",
                                CollectionUtil.map(),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // only the first map is empty: the union is the second map's key set
                        .testResult(
                                $("f0").mapFilter((k, v) -> lit(false))
                                        .mapZipWith($("f0"), (k, v1, v2) -> v1.ifNull(v2)),
                                "MAP_ZIP_WITH(MAP_FILTER(f0, (k, v) -> FALSE), f0, "
                                        + "(k, v1, v2) -> IFNULL(v1, v2))",
                                CollectionUtil.map(entry("a", 1), entry("b", 2)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // only the second map is empty: the union keeps the first map's key order
                        .testResult(
                                $("f0").mapZipWith(
                                                $("f0").mapFilter((k, v) -> lit(false)),
                                                (k, v1, v2) -> v1.ifNull(v2)),
                                "MAP_ZIP_WITH(f0, MAP_FILTER(f0, (k, v) -> FALSE), "
                                        + "(k, v1, v2) -> IFNULL(v1, v2))",
                                CollectionUtil.map(entry("a", 1), entry("b", 2)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        // a NULL key is a key like any other: it merges across both maps and is
                        // indistinguishable in the lambda from an absent key, which also passes
                        // NULL for the value
                        .testResult(
                                $("f1").mapZipWith($("f2"), (k, v1, v2) -> v1.ifNull(v2)),
                                "MAP_ZIP_WITH(f1, f2, (k, v1, v2) -> IFNULL(v1, v2))",
                                CollectionUtil.map(
                                        entry((String) null, 1), entry("a", 2), entry("b", 20)),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
    }

    /**
     * Map keys use SQL logical equality (value equality), not Java object identity. {@code
     * MAP_TRANSFORM_KEYS} duplicate detection and {@code MAP_ZIP_WITH} key union must therefore
     * treat two separately allocated but value-equal keys as the same key. This is exercised with
     * {@code BINARY} keys (distinct {@code byte[]} instances with equal content, which compare by
     * identity under Java {@code equals}), with a complex {@code ARRAY} key (which additionally
     * validates that the generated key wrapper -- see {@code
     * ExprCodeGenerator#generateMapKeyWrapperClass} -- compiles and runs for a structured key
     * type), and with signed floating zero ({@code 0.0} and {@code -0.0}, which are equal under SQL
     * equality but whose naive {@code Double#hashCode} differs -- see {@code
     * CodeGenUtils#hashCodeForType}).
     */
    private Stream<TestSetSpec> mapLogicalKeyEqualityTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_TRANSFORM_KEYS)
                        .onFieldsWithData(
                                CollectionUtil.map(
                                        entry(1, new byte[] {0x0A}), entry(2, new byte[] {0x0A})),
                                CollectionUtil.map(
                                        entry(1, new byte[] {0x0A}), entry(2, new byte[] {0x0B})))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.BYTES()),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.BYTES()))
                        // two value-equal BINARY keys (distinct byte[] instances) collide -> the
                        // transform produces a duplicate key and fails
                        .testTableApiRuntimeError(
                                $("f0").mapTransformKeys((k, v) -> v),
                                "MAP_TRANSFORM_KEYS produced a duplicate key")
                        .testSqlRuntimeError(
                                "MAP_TRANSFORM_KEYS(f0, (k, v) -> v)",
                                "MAP_TRANSFORM_KEYS produced a duplicate key")
                        // control: value-distinct BINARY keys do NOT collide -> both survive
                        .testResult(
                                $("f1").mapTransformKeys((k, v) -> v).cardinality(),
                                "CARDINALITY(MAP_TRANSFORM_KEYS(f1, (k, v) -> v))",
                                2,
                                DataTypes.INT()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData(
                                CollectionUtil.map(entry(new byte[] {0x0A}, 1)),
                                CollectionUtil.map(entry(new byte[] {0x0A}, 2)),
                                CollectionUtil.map(entry(new byte[] {0x0B}, 2)))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT()))
                        // value-equal BINARY keys in both maps merge into a single union entry
                        .testResult(
                                $("f0").mapZipWith($("f1"), (k, v1, v2) -> v1.plus(v2))
                                        .cardinality(),
                                "CARDINALITY(MAP_ZIP_WITH(f0, f1, (k, v1, v2) -> v1 + v2))",
                                1,
                                DataTypes.INT())
                        // control: value-distinct BINARY keys stay separate -> two union entries
                        .testResult(
                                $("f0").mapZipWith($("f2"), (k, v1, v2) -> v1.plus(v2))
                                        .cardinality(),
                                "CARDINALITY(MAP_ZIP_WITH(f0, f2, (k, v1, v2) -> v1 + v2))",
                                2,
                                DataTypes.INT()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_TRANSFORM_KEYS)
                        .onFieldsWithData(CollectionUtil.map(entry(1, 0.0d), entry(2, -0.0d)))
                        .andDataTypes(DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE()))
                        // 0.0 and -0.0 are logically equal keys -> the transform produces a
                        // duplicate key even though their naive Double#hashCode differs
                        .testTableApiRuntimeError(
                                $("f0").mapTransformKeys((k, v) -> v),
                                "MAP_TRANSFORM_KEYS produced a duplicate key")
                        .testSqlRuntimeError(
                                "MAP_TRANSFORM_KEYS(f0, (k, v) -> v)",
                                "MAP_TRANSFORM_KEYS produced a duplicate key"),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData(
                                CollectionUtil.map(entry(0.0d, 1)),
                                CollectionUtil.map(entry(-0.0d, 2)))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.INT()))
                        // signed-zero keys from separate maps merge into a single union entry
                        .testResult(
                                $("f0").mapZipWith($("f1"), (k, v1, v2) -> v1.plus(v2))
                                        .cardinality(),
                                "CARDINALITY(MAP_ZIP_WITH(f0, f1, (k, v1, v2) -> v1 + v2))",
                                1,
                                DataTypes.INT()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData(
                                CollectionUtil.map(entry(new Integer[] {1, 2}, 10)),
                                CollectionUtil.map(entry(new Integer[] {1, 2}, 20)))
                        .andDataTypes(
                                DataTypes.MAP(DataTypes.ARRAY(DataTypes.INT()), DataTypes.INT()),
                                DataTypes.MAP(DataTypes.ARRAY(DataTypes.INT()), DataTypes.INT()))
                        // value-equal complex (ARRAY) keys merge into a single union entry, which
                        // exercises the generated key wrapper for a structured key type
                        .testResult(
                                $("f0").mapZipWith($("f1"), (k, v1, v2) -> v1.plus(v2))
                                        .cardinality(),
                                "CARDINALITY(MAP_ZIP_WITH(f0, f1, (k, v1, v2) -> v1 + v2))",
                                1,
                                DataTypes.INT()));
    }

    /**
     * A higher-order call nested in another one's lambda body closes over the enclosing lambda's
     * parameter, so the inner call's return type is still {@code ANY} when its enclosing {@code
     * ARRAY_TRANSFORM} first derives its own result type. The enclosing function must defer
     * building its {@code ARRAY<...>}/{@code MAP<...>} result type until the inner return type
     * resolves, otherwise validation fails with "Type is not supported: ANY" (see {@code
     * TypeInferenceOperandChecker#capturesUnresolvedLambdaParameter}). Covers every
     * lambda-result-derived return strategy: {@code ARRAY_ZIP_WITH}, {@code MAP_TRANSFORM_KEYS},
     * {@code MAP_TRANSFORM_VALUES} and {@code MAP_ZIP_WITH}.
     */
    private Stream<TestSetSpec> nestedLambdaAnyReturnTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_ZIP_WITH)
                        .onFieldsWithData((Object) new Integer[] {5, 6})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // the inner ARRAY_ZIP_WITH body is the enclosing element x, unresolved
                        // until
                        // x is bound; each inner call returns a two-element array of x
                        .testResult(
                                $("f0").arrayTransform(
                                                x ->
                                                        array(1, 2)
                                                                .arrayZipWith(
                                                                        array(3, 4), (a, b) -> x)),
                                "ARRAY_TRANSFORM(f0, x -> "
                                        + "ARRAY_ZIP_WITH(ARRAY[1, 2], ARRAY[3, 4], (a, b) -> x))",
                                new Integer[][] {new Integer[] {5, 5}, new Integer[] {6, 6}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_TRANSFORM_KEYS)
                        .onFieldsWithData((Object) new Integer[] {5, 6})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // the inner MAP_TRANSFORM_KEYS replaces each key with the enclosing element
                        // x
                        .testResult(
                                $("f0").arrayTransform(
                                                x -> map(1, "a").mapTransformKeys((k, v) -> x)),
                                "ARRAY_TRANSFORM(f0, x -> "
                                        + "MAP_TRANSFORM_KEYS(MAP[1, 'a'], (k, v) -> x))",
                                new java.util.Map[] {
                                    CollectionUtil.map(entry(5, "a")),
                                    CollectionUtil.map(entry(6, "a"))
                                },
                                DataTypes.ARRAY(
                                                DataTypes.MAP(
                                                                DataTypes.INT().notNull(),
                                                                DataTypes.CHAR(1).notNull())
                                                        .notNull())
                                        .notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_TRANSFORM_VALUES)
                        .onFieldsWithData((Object) new Integer[] {5, 6})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // the inner MAP_TRANSFORM_VALUES replaces each value with the enclosing x
                        .testResult(
                                $("f0").arrayTransform(
                                                x -> map("a", 1).mapTransformValues((k, v) -> x)),
                                "ARRAY_TRANSFORM(f0, x -> "
                                        + "MAP_TRANSFORM_VALUES(MAP['a', 1], (k, v) -> x))",
                                new java.util.Map[] {
                                    CollectionUtil.map(entry("a", 5)),
                                    CollectionUtil.map(entry("a", 6))
                                },
                                DataTypes.ARRAY(
                                                DataTypes.MAP(
                                                                DataTypes.CHAR(1).notNull(),
                                                                DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MAP_ZIP_WITH)
                        .onFieldsWithData((Object) new Integer[] {5, 6})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // the inner MAP_ZIP_WITH body is the enclosing element x for every key
                        .testResult(
                                $("f0").arrayTransform(
                                                x ->
                                                        map("a", 1)
                                                                .mapZipWith(
                                                                        map("b", 2),
                                                                        (k, v1, v2) -> x)),
                                "ARRAY_TRANSFORM(f0, x -> "
                                        + "MAP_ZIP_WITH(MAP['a', 1], MAP['b', 2], "
                                        + "(k, v1, v2) -> x))",
                                new java.util.Map[] {
                                    CollectionUtil.map(entry("a", 5), entry("b", 5)),
                                    CollectionUtil.map(entry("a", 6), entry("b", 6))
                                },
                                DataTypes.ARRAY(
                                                DataTypes.MAP(
                                                                DataTypes.CHAR(1).notNull(),
                                                                DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull()));
    }
}
