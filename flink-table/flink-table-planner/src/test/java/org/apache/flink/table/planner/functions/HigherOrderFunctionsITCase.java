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
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.ifThenElse;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.row;

/**
 * Tests for the built-in array higher-order functions ({@code ARRAY_TRANSFORM}, {@code
 * ARRAY_FILTER}, {@code ARRAY_REDUCE} and {@code ARRAY_ZIP_WITH}), which take a lambda argument.
 * Both the SQL and the Table API (host-language lambda) surfaces are covered. These run in
 * streaming mode; {@link HigherOrderFunctionsBatchITCase} covers the same functions in batch mode.
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
                        .testResult(
                                array(array(1, 2), array(3))
                                        .arrayTransform(a -> a.arrayTransform(x -> x.plus(1))),
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
                        // a lambda parameter shadows an outer column of the same name (SQL only:
                        // the typed Table API methods generate fresh parameter names; the Table
                        // API spelling with explicit names is pinned by
                        // HigherOrderFunctionsBatchITCase#testLambdaParameterShadowingOnTableApi)
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
                        .testResult(
                                array(array(1, 2), array(3))
                                        .arrayTransform(
                                                a ->
                                                        a.arrayTransform(
                                                                x -> x.plus(a.cardinality()))),
                                "ARRAY_TRANSFORM(ARRAY[ARRAY[1, 2], ARRAY[3]], "
                                        + "a -> ARRAY_TRANSFORM(a, x -> x + CARDINALITY(a)))",
                                new Integer[][] {new Integer[] {3, 4}, new Integer[] {4}},
                                DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.INT().notNull())
                                                        .notNull())
                                        .notNull())
                        // a two-level capture threaded through an intermediate lambda
                        .testResult(
                                array(array(array(1, 2)))
                                        .arrayTransform(
                                                a ->
                                                        a.arrayTransform(
                                                                b ->
                                                                        b.arrayTransform(
                                                                                x ->
                                                                                        x.plus(
                                                                                                a
                                                                                                        .cardinality())))),
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
                        // only: the typed Table API methods generate fresh parameter names; the
                        // Table API spelling with explicit names is pinned by
                        // HigherOrderFunctionsBatchITCase#testLambdaParameterShadowingOnTableApi)
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
                        .testResult(
                                $("f2").arrayReduce(
                                                lit("").cast(DataTypes.STRING()),
                                                (acc, x) -> acc.concat(x)),
                                "ARRAY_REDUCE(f2, CAST('' AS STRING), (acc, x) -> acc || x)",
                                "abc",
                                DataTypes.STRING()),
                // A reducer body whose type is assignable to, but not identical with, the
                // accumulator type is accepted and converted to the accumulator type: the
                // accumulator (and therefore the result) type stays the one given by the initial
                // value. Types are compared with Flink's own type merging.
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
                                DataTypes.BIGINT().notNull())
                        // the accumulator is the first parameter whatever its name, also for
                        // names whose hash order differs from their declaration order (SQL only:
                        // the typed Table API methods generate the parameter names)
                        .testSqlResult(
                                "ARRAY_REDUCE(f0, CAST(0 AS BIGINT), (x, acc) -> x + acc)",
                                6L,
                                DataTypes.BIGINT().notNull()),
                // A reducer body that can produce NULL makes the result nullable even over a NOT
                // NULL array and NOT NULL initial accumulator, and the NULL is surfaced at runtime
                // rather than a primitive default. The array element nullability alone would not.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REDUCE)
                        .onFieldsWithData((Object) new Integer[] {1, 2, 3})
                        .andDataTypes(DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        .testResult(
                                $("f0").arrayReduce(lit(0), (acc, x) -> nullOf(DataTypes.INT())),
                                "ARRAY_REDUCE(f0, 0, (acc, x) -> CAST(NULL AS INT))",
                                null,
                                DataTypes.INT().nullable())
                        // an initial accumulator wider than the elements keeps its (wider) type;
                        // the reducer body is implicitly widened to the accumulator type
                        .testResult(
                                $("f0").arrayReduce(lit(0L), (acc, x) -> acc.plus(x)),
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
                // accumulator's DECIMAL(10, 2) so it stays assignable and the accumulator is not
                // widened; the widening direction is covered by the "accumulator widening" cases.
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
                                DataTypes.DECIMAL(10, 2).notNull()),
                // A reducer body that is not assignable to the initial value's type widens the
                // accumulator once, without loss and within the type family, to a type the fold is
                // stable in: STRING for character strings, DECIMAL(38, s) for decimals, the common
                // type otherwise, and field by field for constructed types. The initial value is
                // cast to it and it becomes the result type.
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.ARRAY_REDUCE, "accumulator widening")
                        .onFieldsWithData(
                                new String[] {"a", "b", "c"},
                                new BigDecimal[] {new BigDecimal("1.25"), new BigDecimal("2.50")},
                                new Long[] {1L, 2L, 3L},
                                new Integer[] {1, 2, 3},
                                new Integer[] {1, null},
                                new Double[] {0.5, 1.5})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.CHAR(1).notNull()).notNull(),
                                DataTypes.ARRAY(DataTypes.DECIMAL(3, 2).notNull()).notNull(),
                                DataTypes.ARRAY(DataTypes.BIGINT().notNull()).notNull(),
                                DataTypes.ARRAY(DataTypes.INT().notNull()).notNull(),
                                DataTypes.ARRAY(DataTypes.INT()).notNull(),
                                DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull())
                        // '' is a CHAR(0) that every concatenation outgrows
                        .testResult(
                                $("f0").arrayReduce(lit(""), (acc, x) -> acc.concat(x)),
                                "ARRAY_REDUCE(f0, '', (acc, x) -> acc || x)",
                                "abc",
                                DataTypes.STRING().notNull())
                        .testResult(
                                $("f0").arrayReduce(
                                                lit("").cast(DataTypes.VARCHAR(1)),
                                                (acc, x) -> acc.concat(x)),
                                "ARRAY_REDUCE(f0, CAST('' AS VARCHAR(1)), (acc, x) -> acc || x)",
                                "abc",
                                DataTypes.STRING().notNull())
                        // a reducer that keeps the accumulator keeps its type
                        .testResult(
                                $("f0").arrayReduce(lit("zz"), (acc, x) -> acc),
                                "ARRAY_REDUCE(f0, 'zz', (acc, x) -> acc)",
                                "zz",
                                DataTypes.CHAR(2).notNull())
                        // a decimal sum grows by one digit per addition and settles at the maximum
                        // precision, keeping the scale
                        .testResult(
                                $("f1").arrayReduce(
                                                lit(new BigDecimal("0.0")),
                                                (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f1, 0.0, (acc, x) -> acc + x)",
                                new BigDecimal("3.75"),
                                DataTypes.DECIMAL(38, 2).notNull())
                        // an integer accumulator becomes a decimal, which stays exact
                        .testResult(
                                $("f1").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f1, 0, (acc, x) -> acc + x)",
                                new BigDecimal("3.75"),
                                DataTypes.DECIMAL(38, 2).notNull())
                        .testResult(
                                $("f3").arrayReduce(
                                                lit(0).cast(DataTypes.DECIMAL(5, 0)),
                                                (acc, x) ->
                                                        acc.plus(x).cast(DataTypes.DECIMAL(10, 2))),
                                "ARRAY_REDUCE(f3, CAST(0 AS DECIMAL(5, 0)), "
                                        + "(acc, x) -> CAST(acc + x AS DECIMAL(10, 2)))",
                                new BigDecimal("6.00"),
                                DataTypes.DECIMAL(38, 2).notNull())
                        .testResult(
                                $("f2").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f2, 0, (acc, x) -> acc + x)",
                                6L,
                                DataTypes.BIGINT().notNull())
                        .testResult(
                                $("f3").arrayReduce(
                                                lit(0).cast(DataTypes.TINYINT()),
                                                (acc, x) -> acc.plus(x)),
                                "ARRAY_REDUCE(f3, CAST(0 AS TINYINT), (acc, x) -> acc + x)",
                                6,
                                DataTypes.INT().notNull())
                        .testResult(
                                $("f5").arrayReduce(
                                                lit(0).cast(DataTypes.FLOAT()),
                                                (acc, x) -> acc.cast(DataTypes.DOUBLE()).plus(x)),
                                "ARRAY_REDUCE(f5, CAST(0 AS FLOAT), "
                                        + "(acc, x) -> CAST(acc AS DOUBLE) + x)",
                                2.0d,
                                DataTypes.DOUBLE().notNull())
                        // ARRAY[0] has NOT NULL elements, the reducer's array does not
                        .testResult(
                                $("f4").arrayReduce(array(lit(0)), (acc, x) -> array(x)),
                                "ARRAY_REDUCE(f4, ARRAY[0], (acc, x) -> ARRAY[x])",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // a row is widened field by field and keeps the field names of the
                        // initial value
                        .testResult(
                                $("f0").arrayReduce(
                                                row(lit(""), lit(0))
                                                        .cast(
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "s",
                                                                                DataTypes.VARCHAR(
                                                                                        1)),
                                                                        DataTypes.FIELD(
                                                                                "n",
                                                                                DataTypes.INT()))),
                                                (acc, x) ->
                                                        row(
                                                                acc.get("s").concat(x),
                                                                acc.get("n").plus(1))),
                                "ARRAY_REDUCE(f0, CAST(ROW('', 0) AS ROW<s VARCHAR(1), n INT>), "
                                        + "(acc, x) -> ROW(acc.s || x, acc.n + 1))",
                                Row.of("abc", 3),
                                DataTypes.ROW(
                                                DataTypes.FIELD("s", DataTypes.STRING()),
                                                DataTypes.FIELD("n", DataTypes.INT()))
                                        .notNull()));
    }

    private Stream<TestSetSpec> arrayZipWithTestCases() {
        return Stream.of(
                // The lambda-object contract allows NULL only for a nullable parameter. Padding
                // passes NULL even when both arrays have NOT NULL elements, so ARRAY_ZIP_WITH must
                // declare both lambda parameters nullable.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_ZIP_WITH)
                        .onFieldsWithData(new Integer[] {1, 2, 3}, new Integer[] {10, 20})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT().notNull()).notNull(),
                                DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        .testResult(
                                $("f0").arrayZipWith($("f1"), (x, y) -> x.plus(y)),
                                "ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + y)",
                                new Integer[] {11, 22, null},
                                DataTypes.ARRAY(DataTypes.INT()).notNull()),
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
                                        .notNull()));
    }
}
