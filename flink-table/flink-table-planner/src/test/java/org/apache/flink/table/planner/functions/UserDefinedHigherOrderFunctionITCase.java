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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.utils.TestLambdaStrategies;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.TriFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for a user-defined higher-order function that declares a lambda argument via a
 * {@link org.apache.flink.table.types.inference.LambdaInputTypeStrategy} and receives it at runtime
 * as a first-class {@link java.util.function.Function} / {@link java.util.function.BiFunction}
 * object.
 */
class UserDefinedHigherOrderFunctionITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @Test
    void testUserDefinedArrayTransform() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        // a UDF lambda: x -> x + 10 (the parameter reference by name matches the lambda parameter)
        final Expression lambda =
                new UnresolvedLambdaExpression(Collections.singletonList("x"), $("x").plus(10));

        final Table result =
                input.select(call(ArrayTransformScalarFunction.class, $("f0"), lambda));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {11, 12, 13});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayReduce() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        // a 2-parameter (accumulator) UDF lambda: (acc, x) -> acc + x
        final Expression lambda =
                new UnresolvedLambdaExpression(
                        java.util.Arrays.asList("acc", "x"), $("acc").plus($("x")));

        final Table result =
                input.select(call(ArrayReduceScalarFunction.class, $("f0"), lit(0), lambda));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(6);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedZip3() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("b", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("c", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(
                                new Integer[] {1, 2},
                                new Integer[] {10, 20},
                                new Integer[] {100, 200}));

        // a 3-parameter UDF lambda (received as a TriFunction): (x, y, z) -> x + y + z
        final Expression lambda =
                new UnresolvedLambdaExpression(
                        java.util.Arrays.asList("x", "y", "z"), $("x").plus($("y")).plus($("z")));

        final Table result =
                input.select(call(Zip3ScalarFunction.class, $("a"), $("b"), $("c"), lambda));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {111, 222});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithTwoLambdas() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_two_lambdas", TwoLambdaScalarFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1));
        tEnv.createTemporaryView("t", input);

        // A UDF may declare more than one lambda argument; each lambda is applied independently and
        // the results are combined non-commutatively, so the two lambdas are told apart:
        // 100 * (1 + 1) + (1 + 2) = 203
        final Table result =
                tEnv.sqlQuery("SELECT my_two_lambdas(f0, x -> x + 1, x -> x + 2) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(203);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithTwoLambdasCapturingDistinctColumns() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_two_lambdas", TwoLambdaScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.INT()),
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.INT())),
                        Row.of(1, 10, 100));
        tEnv.createTemporaryView("t", input);

        // Each lambda closes over a distinct outer column. Capture lifting appends each lambda's
        // captures as trailing operands in left-to-right order; code generation must partition them
        // back to the owning lambda: 100 * (1 + 10) + (1 + 100) = 1201. Swapping the two captures
        // would yield 100 * (1 + 100) + (1 + 10) = 10111 instead.
        final Table result =
                tEnv.sqlQuery("SELECT my_two_lambdas(f0, x -> x + a, x -> x + b) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(1201);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedZip3Sql() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_zip3", Zip3ScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("b", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("c", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(
                                new Integer[] {1, 2},
                                new Integer[] {10, 20},
                                new Integer[] {100, 200}));
        tEnv.createTemporaryView("t", input);

        final Table result =
                tEnv.sqlQuery("SELECT my_zip3(a, b, c, (x, y, z) -> x + y + z) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {111, 222});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformSql() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        final Table result = tEnv.sqlQuery("SELECT my_array_transform(f0, x -> x + 10) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {11, 12, 13});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformWithTypeChangingLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform_to_string", ArrayTransformToStringScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // the lambda changes the element logical type (INT -> STRING), so the UDF result type,
        // derived from the lambda return data type, must be ARRAY<STRING>
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_array_transform_to_string(f0, x -> CAST(x + 10 AS STRING)) FROM t");

        assertThat(result.getResolvedSchema().getColumnDataTypes())
                .containsExactly(DataTypes.ARRAY(DataTypes.STRING()));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new String[] {"11", "12", "13"});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformClosureSql() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // the UDF lambda closes over the outer column `base`
        final Table result = tEnv.sqlQuery("SELECT my_array_transform(f0, x -> x + base) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformClosure() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));

        // the UDF lambda closes over the outer column `base`
        final Expression lambda =
                new UnresolvedLambdaExpression(
                        Collections.singletonList("x"), $("x").plus($("base")));
        final Table result =
                input.select(call(ArrayTransformScalarFunction.class, $("f0"), lambda));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformNestedCaptureSql() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a2d", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                        Row.of((Object) new Integer[][] {{1, 2}, {3, 4}}));
        tEnv.createTemporaryView("t", input);

        // the inner user-defined ARRAY_TRANSFORM lambda `x -> x + a[1]` closes over `a`, the
        // parameter of the enclosing built-in ARRAY_TRANSFORM lambda
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(a2d, a -> my_array_transform(a, x -> x + a[1])) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            // a=[1,2] -> a[1]=1 -> [2,3]; a=[3,4] -> a[1]=3 -> [6,7]
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[][] {{2, 3}, {6, 7}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformNestedCapture() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a2d", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                        Row.of((Object) new Integer[][] {{1, 2}, {3, 4}}));

        // the inner user-defined ARRAY_TRANSFORM lambda closes over `a`, the parameter of the
        // enclosing built-in ARRAY_TRANSFORM lambda
        final Table result =
                input.select(
                        $("a2d").arrayTransform(
                                        a ->
                                                call(
                                                        ArrayTransformScalarFunction.class,
                                                        a,
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x").plus(a.at(1))))));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[][] {{2, 3}, {6, 7}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testDeeplyNestedAlternatingHigherOrderFunctionsSql() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_nested_array_transform", NestedArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a3d",
                                        DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[][][] {{{1, 2}, {3, 4}}, {{5}}}, 100));
        tEnv.createTemporaryView("t", input);

        // Four nesting levels alternating user-defined and built-in higher-order functions, each
        // level's lambda capturing the parameters of *all* enclosing lambdas plus an outer column:
        //   L1 UDF        my_nested_array_transform(a3d, a -> ...)      a : ARRAY<ARRAY<INT>>
        //   L2 built-in   ARRAY_TRANSFORM(a, b -> ...)                  b : ARRAY<INT>
        //   L3 UDF        my_array_transform(b, x -> ...)               x : INT
        //   L4 built-in   ARRAY_REDUCE(b, x, (acc, y) -> ...)           captures b, x, a, base
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_nested_array_transform(a3d, a -> "
                                + "ARRAY_TRANSFORM(a, b -> "
                                + "my_array_transform(b, x -> "
                                + "ARRAY_REDUCE(b, x, (acc, y) -> acc + y + base + a[1][1])))) "
                                + "FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            // per element x of b the fold yields x + sum(b) + cardinality(b) * (base + a[1][1]):
            //   a=[[1,2],[3,4]], a[1][1]=1 -> b=[1,2]: x + 3 + 2*101 -> [206, 207]
            //                                 b=[3,4]: x + 7 + 2*101 -> [212, 213]
            //   a=[[5]],         a[1][1]=5 -> b=[5]:   x + 5 + 1*105 -> [115]
            assertThat(iterator.next().getField(0))
                    .isEqualTo(new Integer[][][] {{{206, 207}, {212, 213}}, {{115}}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testDeeplyNestedAlternatingHigherOrderFunctions() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a3d",
                                        DataTypes.ARRAY(
                                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[][][] {{{1, 2}, {3, 4}}, {{5}}}, 100));

        // the Table API equivalent of testDeeplyNestedAlternatingHigherOrderFunctionsSql(): a
        // user-defined higher-order call, a built-in one, a user-defined one and a built-in one,
        // with the innermost lambda capturing every enclosing lambda parameter and an outer column
        final Table result =
                input.select(
                        call(
                                NestedArrayTransformScalarFunction.class,
                                $("a3d"),
                                new UnresolvedLambdaExpression(
                                        Collections.singletonList("a"),
                                        $("a").arrayTransform(
                                                        UserDefinedHigherOrderFunctionITCase
                                                                ::innerUserDefinedTransform))));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0))
                    .isEqualTo(new Integer[][][] {{{206, 207}, {212, 213}}, {{115}}});
            assertThat(iterator).isExhausted();
        }
    }

    /**
     * The two innermost levels of {@link #testDeeplyNestedAlternatingHigherOrderFunctions()}:
     * {@code my_array_transform(b, x -> ARRAY_REDUCE(b, x, (acc, y) -> acc + y + base + a[1][1]))},
     * where {@code b} is the parameter of the enclosing built-in lambda, {@code a} the parameter of
     * the lambda enclosing that one, and {@code base} an outer column.
     */
    private static ApiExpression innerUserDefinedTransform(ApiExpression b) {
        return call(
                ArrayTransformScalarFunction.class,
                b,
                new UnresolvedLambdaExpression(
                        Collections.singletonList("x"),
                        b.arrayReduce(
                                $("x"),
                                (acc, y) -> acc.plus(y).plus($("base")).plus($("a").at(1).at(1)))));
    }

    @Test
    void testUserDefinedArrayReduceSql() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        final Table result =
                tEnv.sqlQuery("SELECT my_array_reduce(f0, 0, (acc, x) -> acc + x) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(6);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformStreaming() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        final Expression lambda =
                new UnresolvedLambdaExpression(Collections.singletonList("x"), $("x").plus(10));

        final Table result =
                input.select(call(ArrayTransformScalarFunction.class, $("f0"), lambda));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {11, 12, 13});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayReduceStreaming() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        final Expression lambda =
                new UnresolvedLambdaExpression(
                        java.util.Arrays.asList("acc", "x"), $("acc").plus($("x")));

        final Table result =
                input.select(call(ArrayReduceScalarFunction.class, $("f0"), lit(0), lambda));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(6);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformNullArray() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1));
        tEnv.createTemporaryView("t", input);

        // a NULL array reaches the function, which returns NULL without ever calling the lambda
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_array_transform(CAST(NULL AS ARRAY<INT>), x -> x + 1) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isNull();
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformNullElement() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, null, 3}));
        tEnv.createTemporaryView("t", input);

        // a NULL element is passed to the lambda and propagates through its body
        final Table result = tEnv.sqlQuery("SELECT my_array_transform(f0, x -> x + 10) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {11, null, 13});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedLambdaFollowedByRegularArgument() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_transform_offset", OffsetScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // the lambda is not the last argument: capture lifting appends the lifted captures behind
        // *all* declared arguments, so the trailing regular argument must keep its position
        final Table result =
                tEnv.sqlQuery("SELECT my_array_transform_offset(f0, x -> x + base, 1000) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {1101, 1102, 1103});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedSiblingCallsWithSameLambdaParameterName() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // two sibling calls in one projection whose lambdas declare the same parameter name, only
        // one of which captures an outer column
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_array_transform(f0, x -> x + 1), "
                                + "my_array_transform(f0, x -> x + base) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithTwoLambdasNestedInBuiltIn() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_two_lambdas", TwoLambdaScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // a two-lambda call nested in a built-in lambda: one lambda captures the enclosing lambda
        // parameter, the other an outer column, so per-lambda capture partitioning has to keep the
        // two capture kinds apart: 100 * (e + e) + (e + 100) = 201e + 100. Swapping the two
        // captures would yield 100 * (e + 100) + 2e = 102e + 10000 instead.
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, e -> my_two_lambdas(e, x -> x + e, x -> x + base)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {301, 502, 703});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedHigherOrderFunctionInsideUserDefinedLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // two *directly* nested user-defined higher-order calls (the deeply nested test alternates
        // user-defined and built-in ones): the inner lambda captures the outer lambda's parameter
        // as its initial accumulator, so each element is folded to x + sum(f0) = x + 6
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_array_transform(f0, x -> my_array_reduce(f0, x, (acc, y) -> acc + y)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {7, 8, 9});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedHigherOrderFunctionCapturingEnclosingBuiltInLambdaParameterSql()
            throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a user-defined higher-order call inside a *built-in* lambda whose own lambda captures the
        // enclosing parameter: the capture is only reachable through the inner lambda's body, so
        // the deferral of the enclosing binding pass has to look inside lambda operands as well
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, e -> my_array_transform(f0, x -> x + e)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0))
                    .isEqualTo(new Integer[][] {{2, 3, 4}, {3, 4, 5}, {4, 5, 6}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedHigherOrderFunctionCapturingEnclosingBuiltInLambdaParameter()
            throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        // the Table API equivalent of the query above
        final Table result =
                input.select(
                        $("f0").arrayTransform(
                                        e ->
                                                call(
                                                        ArrayTransformScalarFunction.class,
                                                        $("f0"),
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x").plus(e)))));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0))
                    .isEqualTo(new Integer[][] {{2, 3, 4}, {3, 4, 5}, {4, 5, 6}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedHigherOrderFunctionCapturingEnclosingLambdaParameterAndColumn()
            throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // the inner lambda mixes both capture kinds: an enclosing lambda parameter (resolved only
        // after the built-in call binds it) and an outer column (lifted by capture lifting)
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, e -> my_array_transform(f0, x -> x + e + base)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0))
                    .isEqualTo(new Integer[][] {{102, 103, 104}, {103, 104, 105}, {104, 105, 106}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedHigherOrderFunctionCapturingEnclosingLambdaParameterInUdfArgument()
            throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // the enclosing lambda parameter is not referenced directly but through an ordinary
        // user-defined function, whose own type inference must be deferred in the same way
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, e -> my_array_transform(f0, x -> my_add(x, e))) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0))
                    .isEqualTo(new Integer[][] {{2, 3, 4}, {3, 4, 5}, {4, 5, 6}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testRegularUserDefinedFunctionInsideLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // an ordinary (non-higher-order) user-defined function called from a lambda body, in a
        // built-in and in a user-defined higher-order function, with and without an outer capture
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, x -> my_plus_one(x)), "
                                + "my_array_transform(f0, x -> my_plus_one(x)), "
                                + "ARRAY_TRANSFORM(f0, x -> my_add(x, base)), "
                                + "my_array_transform(f0, x -> my_add(x, base)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(2)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(row.getField(3)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testRegularUserDefinedFunctionWithOpenInsideLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_opened", OpenedScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a function called from a lambda body keeps its regular lifecycle: it is a member of the
        // generated class, so open() runs once even though the call happens inside the element loop
        // of a built-in function, or inside the compiled evaluator of a user-defined one
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, x -> my_opened(x)), "
                                + "my_array_transform(f0, x -> my_opened(x)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {1001, 1002, 1003});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {1001, 1002, 1003});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testHigherOrderFunctionAsArgumentOfRegularUserDefinedFunction() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // the opposite direction: a lambda-carrying call as the argument of an ordinary function,
        // once with a built-in and once with a user-defined higher-order function whose lambda
        // body itself calls ordinary functions ((((0+2)+3)+4) + 1 = 10)
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_plus_one(ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x)), "
                                + "my_plus_one(my_array_reduce(f0, 0, "
                                + "(acc, x) -> my_add(acc, my_plus_one(x)))) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(7);
            assertThat(row.getField(1)).isEqualTo(10);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testRegularUserDefinedFunctionInDeeplyNestedLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // ordinary functions at the innermost level of an alternating nesting, in both orders:
        //   built-in -> user-defined: per e the fold yields e + sum(x + 1) = e + 9
        //   user-defined -> built-in: per e the fold yields e + sum(x + base) = e + 306
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, e -> "
                                + "my_array_reduce(f0, e, (acc, x) -> my_add(acc, my_plus_one(x)))), "
                                + "my_array_transform(f0, e -> "
                                + "ARRAY_REDUCE(f0, e, (acc, x) -> my_add(acc, my_add(x, base)))) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {10, 11, 12});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {307, 308, 309});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedLambdaInsideMapHigherOrderFunction() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD(
                                        "f1", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))),
                        Row.of(new Integer[] {1, 2, 3}, Map.of("key", 5)));
        tEnv.createTemporaryView("t", input);

        // a user-defined higher-order call inside the lambda of a built-in *map* higher-order
        // function, capturing both the key and the value parameter of the enclosing lambda:
        // 5 + (1 + 3) + (2 + 3) + (3 + 3) = 20
        final Table result =
                tEnv.sqlQuery(
                        "SELECT MAP_TRANSFORM_VALUES(f1, (k, v) -> "
                                + "my_array_reduce(f0, v, (acc, y) -> acc + y + CHAR_LENGTH(k))) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(Map.of("key", 20));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedMapTransformValues() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_map_transform_values", MapValuesScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "f0", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(Map.of("key", 5), 100));
        tEnv.createTemporaryView("t", input);

        // a user-defined higher-order function over a MAP with a two-parameter (key, value) lambda
        final Table result =
                tEnv.sqlQuery("SELECT my_map_transform_values(f0, (k, v) -> v + base) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(Map.of("key", 105));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayTransformClosureStreaming() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // capture lifting in streaming mode
        final Table result = tEnv.sqlQuery("SELECT my_array_transform(f0, x -> x + base) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedArrayReduceInFilterAndGroupBy() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // a higher-order call outside a projection: in a filter condition and as a grouping key
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_array_reduce(f0, 0, (acc, x) -> acc + x) FROM t "
                                + "WHERE my_array_reduce(f0, 0, (acc, x) -> acc + x) > 5 "
                                + "GROUP BY my_array_reduce(f0, 0, (acc, x) -> acc + x)");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(6);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdaWithWrongParameterCountIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // SQL: the declared lambda argument has one parameter, the call passes two
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_array_transform(f0, (x, y) -> x + y) FROM t"))
                .hasMessageContaining(
                        "The lambda expression at position 1 expects 1 parameter(s) but 2 were provided.");

        // Table API
        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                ArrayTransformScalarFunction.class,
                                                $("f0"),
                                                new UnresolvedLambdaExpression(
                                                        java.util.Arrays.asList("x", "y"),
                                                        $("x").plus($("y"))))))
                .hasMessageContaining(
                        "The lambda expression expects 1 parameter(s) but 2 were provided.");
    }

    @Test
    void testLambdaAtNonLambdaArgumentPositionIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // SQL: argument 0 is declared as an array, not as a lambda
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_array_transform(x -> x, x -> x) FROM t"))
                .hasMessageContaining(
                        "Cannot apply 'my_array_transform' to arguments of type "
                                + "'my_array_transform(<FUNCTION(ANY) -> ANY>, "
                                + "<FUNCTION(ANY) -> ANY>)'")
                .hasMessageContaining("Supported form(s): my_array_transform(ARRAY, FUNCTION)");

        // Table API
        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                ArrayTransformScalarFunction.class,
                                                new UnresolvedLambdaExpression(
                                                        Collections.singletonList("x"), $("x")),
                                                new UnresolvedLambdaExpression(
                                                        Collections.singletonList("x"), $("x")))))
                .hasMessageContaining("does not accept a lambda expression at position 0.");
    }

    @Test
    void testNonLambdaAtLambdaArgumentPositionIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 10));
        tEnv.createTemporaryView("t", input);

        // SQL: argument 1 is declared as a lambda, but a regular expression is passed
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_array_transform(f0, base) FROM t"))
                .hasStackTraceContaining("A lambda expression was expected for argument 1.");

        // Table API
        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                ArrayTransformScalarFunction.class,
                                                $("f0"),
                                                $("base"))))
                .hasStackTraceContaining("A lambda expression was expected for argument 1.");
    }

    @Test
    void testLambdaWithUnsupportedParameterCountIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_four_parameter_lambda", FourParameterLambdaScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a lambda is passed to a function as a Function, BiFunction, or TriFunction, so a
        // four-parameter lambda argument has no runtime representation
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_four_parameter_lambda(f0, (a, b, c, d) -> a) FROM t"))
                .hasStackTraceContaining(
                        "A lambda argument must have one, two, or three parameters, "
                                + "but 4 parameter types were derived.");
    }

    @Test
    void testLambdaWithUnderivableParameterTypesIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1));
        tEnv.createTemporaryView("t", input);

        // the sibling argument the lambda parameter type is derived from is not an array, so
        // elementOf(0) cannot bind the parameter. The lambda is not at fault, so the call is
        // reported with its argument types rather than the lambda being blamed.
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_array_transform(f0, x -> x + 1) FROM t"))
                .hasMessageContaining(
                        "Cannot apply 'my_array_transform' to arguments of type "
                                + "'my_array_transform(<INTEGER>, <FUNCTION(ANY) -> ANY>)'")
                .hasMessageContaining("Supported form(s): my_array_transform(ARRAY, FUNCTION)");
    }

    @Test
    void testInvalidArgumentCountIsReportedBeforeLambdaBinding() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // the initial-accumulator argument is missing: the argument count is reported rather than
        // the (secondary) failure to bind the lambda parameters
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_array_reduce(f0, (acc, x) -> acc + x) FROM t"))
                .hasMessageContaining("No match found for function signature");
    }

    @Test
    void testRegularUserDefinedFunctionInLambdaInEveryClause() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // a lambda calling a user-defined function is not restricted to the projection: the same
        // call works in WHERE, GROUP BY, HAVING and ORDER BY, and as an aggregate's argument
        final String reduce = "my_array_reduce(f0, 0, (acc, x) -> my_add(acc, x))";
        assertThat(collectFirst(tEnv, "SELECT f0 FROM t WHERE " + reduce + " > 5"))
                .isEqualTo(new Integer[] {1, 2, 3});
        assertThat(collectFirst(tEnv, "SELECT " + reduce + " FROM t GROUP BY " + reduce))
                .isEqualTo(6);
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT MAX(base) FROM t GROUP BY f0 HAVING " + reduce + " > 5"))
                .isEqualTo(100);
        assertThat(collectFirst(tEnv, "SELECT f0 FROM t ORDER BY " + reduce))
                .isEqualTo(new Integer[] {1, 2, 3});
        assertThat(collectFirst(tEnv, "SELECT SUM(" + reduce + ") FROM t")).isEqualTo(6);
    }

    @Test
    void testRegularUserDefinedFunctionInNestedLambdaCaptureCombinations() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // the inner lambda uses the enclosing parameter only inside a user-defined call and never
        // its own parameter: the capture must still reach the lambda
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(f0, e -> my_array_transform(f0, x -> my_plus_one(e))) FROM t"))
                .isEqualTo(new Integer[][] {{2, 2, 2}, {3, 3, 3}, {4, 4, 4}});

        // the same capture used twice, next to an outer column capture
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(f0, e -> my_array_transform(f0, x -> "
                                        + "my_add(my_add(x, e), my_add(e, base)))) FROM t"))
                .isEqualTo(new Integer[][] {{103, 104, 105}, {105, 106, 107}, {107, 108, 109}});

        // an inner lambda parameter shadowing the enclosing one, with a user-defined call between
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(f0, x -> my_array_transform(f0, x -> my_plus_one(x))) FROM t"))
                .isEqualTo(new Integer[][] {{2, 3, 4}, {2, 3, 4}, {2, 3, 4}});

        // three user-defined levels with user-defined calls at the innermost level
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT my_array_transform(f0, a -> my_array_reduce(f0, a, (acc, x) -> "
                                        + "my_array_reduce(f0, my_add(acc, x), (acc2, y) -> my_add(acc2, y)))) FROM t"))
                .isEqualTo(new Integer[] {25, 26, 27});

        // NULL propagates through a user-defined call in the lambda body
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(f0, x -> my_add(x, CAST(NULL AS INT))) FROM t"))
                .isEqualTo(new Integer[] {null, null, null});
    }

    @Test
    void testRegularUserDefinedFunctionInMapLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_array_reduce", ArrayReduceScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a user-defined call in the lambda of a map higher-order function, capturing the key and
        // the value parameter: 5 + (1+1) + (2+1) + (3+1) = 14
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT MAP_TRANSFORM_VALUES(MAP['k', 5], (k, v) -> "
                                        + "my_array_reduce(f0, v, (acc, y) -> my_add(acc, y + CHAR_LENGTH(k)))) FROM t"))
                .isEqualTo(Map.of("k", 14));

        // a user-defined higher-order call nested in a map lambda, capturing the value parameter
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT MAP_TRANSFORM_VALUES(MAP['k', 5], (k, v) -> "
                                        + "my_array_transform(f0, x -> my_add(x, v))[1]) FROM t"))
                .isEqualTo(Map.of("k", 6));
    }

    @Test
    void testAggregateInUserDefinedLambdaBodyIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_agg", SumAggregateFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // the lambda of a user-defined higher-order function is compiled into a per-element
        // expression as well, so it rejects aggregates over its parameter the same way, built-in
        // or user-defined
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_array_transform(f0, x -> SUM(x)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Aggregate functions over a lambda parameter are not supported in the body "
                                + "of a lambda expression. 'x' is a lambda parameter");
        assertThatThrownBy(
                        () -> tEnv.sqlQuery("SELECT my_array_transform(f0, x -> my_agg(x)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Aggregate functions over a lambda parameter are not supported");

        // the Table API reports the same, without naming the generated parameter
        assertThatThrownBy(
                        () ->
                                input.select(
                                                call(
                                                        ArrayTransformScalarFunction.class,
                                                        $("f0"),
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x").sum())))
                                        .execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Aggregate functions over a lambda parameter are not supported in the body "
                                + "of a lambda expression");
    }

    @Test
    void testAggregateOverOuterColumnInUserDefinedLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_agg", SumAggregateFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 10),
                        Row.of(new Integer[] {1, 2, 3}, 20)));

        // an aggregate over a column of the enclosing query works in a user-defined
        // higher-order function's lambda just as in a built-in one, and with a user-defined
        // aggregate too
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT my_array_transform(f0, x -> x + SUM(base)) FROM t"
                                        + " GROUP BY f0"))
                .isEqualTo(new Integer[] {31, 32, 33});
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT my_array_transform(f0, x -> x + my_agg(base)) FROM t"
                                        + " GROUP BY f0"))
                .isEqualTo(new Integer[] {31, 32, 33});
        // and inside a lambda nested in a user-defined higher-order function's lambda
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT my_array_transform(f0, e -> ARRAY_REDUCE(f0, 0,"
                                        + " (acc, x) -> acc + x + e + SUM(base))) FROM t"
                                        + " GROUP BY f0"))
                .isEqualTo(new Integer[] {99, 102, 105});
    }

    @Test
    void testOverWindowCapturedInNestedUserDefinedLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a2d", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
                                DataTypes.FIELD("g", DataTypes.STRING()),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[][] {{1, 2}, {3}}, "x", 10),
                        Row.of(new Integer[][] {{1, 2}, {3}}, "y", 20)));

        // The most composed case: the OVER window is lifted out of the innermost lambda into a
        // projection below the two nested calls, while the capture of the enclosing lambda's
        // parameter ('a[1]') stays inside -- across a user-defined higher-order call.
        // Both rows see the same unbounded window (10 + 20), so both produce the same result:
        // {1, 2} -> {1 + 1 + 30, 2 + 1 + 30}, {3} -> {3 + 3 + 30}.
        final Integer[][] unbounded = {{32, 33}, {36}};
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a2d, a -> my_array_transform(a,"
                                        + " x -> x + a[1] + SUM(base) OVER ())) FROM t"))
                .containsExactlyInAnyOrder(unbounded, unbounded);

        // the same composition with a partitioned window, so that the lifted value differs per row
        // rather than being a constant shared by both
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a2d, a -> my_array_transform(a,"
                                        + " x -> x + a[1] + SUM(base) OVER (PARTITION BY g)))"
                                        + " FROM t"))
                .containsExactlyInAnyOrder(
                        new Integer[][] {{12, 13}, {16}}, new Integer[][] {{22, 23}, {26}});
    }

    @Test
    void testTableFunctionInLambdaBodyIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_table_func", SplitTableFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("s", DataTypes.STRING())),
                        Row.of(new Integer[] {1, 2, 3}, "a b"));
        tEnv.createTemporaryView("t", input);

        // a table function is rejected in a lambda body just as in any other scalar position;
        // without the check it would reach code generation and fail with an unset collector
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(f0, x -> my_table_func(s)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Table functions are not supported in the body of a lambda expression");
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_array_transform(f0, x -> my_table_func(s)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Table functions are not supported in the body of a lambda expression");
    }

    @Test
    void testAsynchronousScalarFunctionInLambdaBodyIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_async_plus_one", AsyncPlusOneScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // an asynchronous function completes its future after the per-element expression that
        // would consume the result has been evaluated; without the check the call reaches code
        // generation, where the rule that extracts asynchronous calls out of a projection does not
        // descend into lambda bodies, and fails to compile
        final String expectedMessage =
                "Asynchronous scalar functions are not supported in the body of a lambda "
                        + "expression";
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(f0, x -> my_async_plus_one(x)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        // also when the body does not reference the lambda parameter at all
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_array_transform(f0, x -> my_async_plus_one(1)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        // the Table API validates through the input type strategy instead of the operand type
        // checker, so it needs its own check
        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                call(
                                                        ArrayTransformScalarFunction.class,
                                                        $("f0"),
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                call(
                                                                        AsyncPlusOneScalarFunction
                                                                                .class,
                                                                        $("x"))))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void testLambdaPassedToFunctionWithoutLambdaArgumentIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100));
        tEnv.createTemporaryView("t", input);

        // the function declares no lambda argument at all, so the call cannot be described by its
        // argument types (the lambda's parameters stay unbound); the cause is reported instead
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_add(f0, x -> x + 1) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid lambda expression at position 1. Function 'my_add' does not "
                                + "accept a lambda expression at any position.");

        assertThatThrownBy(
                        () ->
                                input.select(
                                                call(
                                                        AddScalarFunction.class,
                                                        $("base"),
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x").plus(1))))
                                        .execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Function 'AddScalarFunction' does not accept a lambda expression at "
                                + "position 1.");
    }

    @Test
    void testFunctionReferenceAsLambdaIsNotSupported() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // SQL has no function-valued expressions, so a bare function name at a lambda position is
        // an ordinary identifier and is reported as such. The supported spelling is an explicit
        // lambda, 'x -> my_plus_one(x)'.
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT ARRAY_TRANSFORM(f0, my_plus_one) FROM t"))
                .hasMessageContaining("Column 'my_plus_one' not found in any table");
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_array_transform(f0, my_plus_one) FROM t"))
                .hasMessageContaining("Column 'my_plus_one' not found in any table");
    }

    @Test
    void testUserDefinedHigherOrderTableFunction() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_explode_transform", ExplodeTransformTableFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a table function hosts a lambda argument exactly like a scalar function: its call is
        // compiled into a scalar expression that can host the lambda body
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT v FROM t, LATERAL TABLE(my_explode_transform(f0, x -> x * 10))"))
                .containsExactly(10, 20, 30);
    }

    @Test
    void testLambdaArgumentIsRejectedForUnsupportedFunctionKinds() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_lambda_agg", LambdaAggregateFunction.class);
        tEnv.createTemporarySystemFunction("my_lambda_ptf", LambdaProcessTableFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a lambda body is compiled into a per-element scalar expression over the row the call is
        // evaluated in; an aggregate folds rows and a PTF consumes a table, so neither can host one
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_lambda_agg(f0, x -> x * 2) FROM t"))
                .hasMessageContaining(
                        "Lambda arguments are not supported for functions of kind 'AGGREGATE'. "
                                + "Only scalar and table functions can declare a lambda argument.");

        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT * FROM my_lambda_ptf(t => TABLE t, f => x -> x * 2)"))
                .hasMessageContaining(
                        "Lambda arguments are not supported for functions of kind 'PROCESS_TABLE'. "
                                + "Only scalar and table functions can declare a lambda argument.");
    }

    @Test
    void testLambdaArgumentIsRejectedForAsynchronousFunctionKinds() throws Exception {
        // asynchronous calls are planned into a dedicated operator that only streaming supports
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_lambda_async", LambdaAsyncScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_lambda_async_table", LambdaAsyncTableFunction.class);
        tEnv.createTemporarySystemFunction("my_async_plus_one", AsyncPlusOneScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));
        tEnv.createTemporaryView("t", input);

        // a lambda object is bound to the call that created it and to the thread evaluating that
        // call, so it must not outlive eval; an asynchronous function completes a future on another
        // thread after eval returned and therefore cannot host one
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_lambda_async(f0, x -> x * 2) FROM t"))
                .hasMessageContaining(
                        "Lambda arguments are not supported for functions of kind 'ASYNC_SCALAR'. "
                                + "Only scalar and table functions can declare a lambda argument.");

        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT v FROM t, LATERAL TABLE(my_lambda_async_table(f0, x -> x * 2))"))
                .hasMessageContaining(
                        "Lambda arguments are not supported for functions of kind 'ASYNC_TABLE'. "
                                + "Only scalar and table functions can declare a lambda argument.");

        // only the lambda *argument* is out of scope: a higher-order call is an ordinary scalar
        // expression and remains usable as an argument of an asynchronous function
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT my_async_plus_one(ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x)) FROM t"))
                .containsExactly(7);
    }

    @Test
    void testLambdaBodyIsOnlyEvaluatedWhenTheFunctionAppliesIt() throws Exception {
        // the framework never evaluates a lambda body on its own: a function that ignores its
        // lambda argument never runs the body, even one that would fail
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_ignore_lambda", IgnoringLambdaScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_throw", ThrowingScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        assertThat(collectColumn(tEnv, "SELECT my_ignore_lambda(f0, x -> my_throw(x)) FROM t"))
                .containsExactly(3);
    }

    @Test
    void testLambdaObjectCanBeAppliedRepeatedly() throws Exception {
        // the function owns the loop: it may apply the same lambda object any number of times, in
        // any order, and every application sees the same captured values
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_apply_twice", ApplyTwiceScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 10)));

        // the array is visited back to front, and each element is passed to the lambda twice
        assertThat(collectFirst(tEnv, "SELECT my_apply_twice(f0, x -> x + base) FROM t"))
                .isEqualTo(new Integer[] {13, 13, 12, 12, 11, 11});
    }

    @Test
    void testLambdaObjectIsFreshPerEvaluation() throws Exception {
        // the object is constructed at the call site, so a fresh one exists per evaluation of the
        // enclosing call: per row for a top-level call, per element when the call sits in a body
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_lambda_identity", LambdaIdentityScalarFunction.class);
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1}),
                        Row.of((Object) new Integer[] {2}),
                        Row.of((Object) new Integer[] {3})));

        LambdaIdentityScalarFunction.SEEN.clear();
        assertThat(collectColumn(tEnv, "SELECT my_lambda_identity(f0, x -> x + 1) FROM t"))
                .containsExactlyInAnyOrder(2, 3, 4);
        assertThat(distinctByIdentity(LambdaIdentityScalarFunction.SEEN)).hasSize(3);

        // the inner call is evaluated once per element of the outer lambda's array, so one row
        // yields as many distinct objects as the array has elements
        tEnv.createTemporaryView(
                "t_single",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        LambdaIdentityScalarFunction.SEEN.clear();
        assertThat(
                        collectFirst(
                                tEnv,
                                "SELECT my_array_transform(f0, "
                                        + "y -> my_lambda_identity(ARRAY[y], x -> x + y)) "
                                        + "FROM t_single"))
                .isEqualTo(new Integer[] {2, 4, 6});
        assertThat(distinctByIdentity(LambdaIdentityScalarFunction.SEEN)).hasSize(3);
    }

    @Test
    void testLambdaBodyErrorPropagatesUnchanged() throws Exception {
        // an unchecked exception thrown while the body is evaluated leaves apply() unchanged, so a
        // function sees the same error the equivalent built-in higher-order function raises
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_catch_lambda", CatchingLambdaScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_throw", ThrowingScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1})));

        assertThat(collectFirst(tEnv, "SELECT my_catch_lambda(f0, x -> my_throw(x)) FROM t"))
                .isEqualTo("java.lang.IllegalStateException: boom for 1");

        // a function that does not catch it fails the job with that error
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        assertThatThrownBy(
                        () ->
                                collectFirst(
                                        tEnv,
                                        "SELECT my_array_transform(f0, x -> my_throw(x)) FROM t"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("boom for 1");

        // a built-in, which evaluates its body the same way, fails identically
        assertThatThrownBy(
                        () ->
                                collectFirst(
                                        tEnv,
                                        "SELECT ARRAY_TRANSFORM(f0, x -> my_throw(x)) FROM t"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("boom for 1");
    }

    @Test
    void testFunctionCalledFromALambdaBodyIsOpenedAndClosed() throws Exception {
        // the framework owns the lifecycle of the compiled lambda body: a function called from it
        // is opened and closed with the enclosing operator, like one called anywhere else
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_array_transform", ArrayTransformScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_tracked", LifecycleTrackingScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        LifecycleTrackingScalarFunction.OPEN_COUNT.set(0);
        LifecycleTrackingScalarFunction.CLOSE_COUNT.set(0);

        final TableResult result =
                tEnv.sqlQuery("SELECT my_array_transform(f0, x -> my_tracked(x)) FROM t").execute();
        try (final CloseableIterator<Row> iterator = result.collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(iterator).isExhausted();
        }
        // a finished job has run close() on every task
        result.await();

        assertThat(LifecycleTrackingScalarFunction.OPEN_COUNT).hasValue(1);
        assertThat(LifecycleTrackingScalarFunction.CLOSE_COUNT).hasValue(1);
    }

    @Test
    void testLambdaArgumentIsRejectedInAStaticSignature() {
        // a static signature (the shape a process table function must declare) has no channel for
        // deriving a lambda's parameter types from the sibling arguments
        assertThatThrownBy(() -> StaticArgument.scalar("f", DataTypes.FUNCTION(1), false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for argument 'f'. Lambda arguments are "
                                + "not supported in a static signature. Declare them with a "
                                + "LambdaInputTypeStrategy instead.");
    }

    @Test
    void testFunctionTypeIsRejectedAsReturnType() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("returns_function", FunctionReturningFunction.class);
        tEnv.createTemporarySystemFunction(
                "returns_nested_function", FunctionReturningNestedFunction.class);

        // A lambda is a compile-time construct, so a function cannot hand one back as its result --
        // neither directly nor nested in a constructed type.
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT returns_function()"))
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining(
                        "Invalid output data type 'FUNCTION(1)'. The FUNCTION data type is a "
                                + "helper type for lambda arguments of higher-order functions. It "
                                + "cannot be materialized and is not supported as a table column, "
                                + "persisted return type, or state type.");

        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT returns_nested_function()"))
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("Invalid output data type 'ARRAY<FUNCTION(1)>'.");
    }

    @Test
    void testFunctionTypeIsRejectedAsStateType() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "function_acc_agg", FunctionAccumulatorAggregateFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1));

        // A lambda object is valid only for the eval invocation it is passed to and has no
        // serialized form, so it can never be kept between invocations -- the type is rejected
        // during validation rather than when the state serializer is built.
        assertThatThrownBy(() -> input.select(call("function_acc_agg", $("f0"))))
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining(
                        "Invalid accumulator data type 'FUNCTION(1)'. The FUNCTION data type is a "
                                + "helper type for lambda arguments of higher-order functions. It "
                                + "cannot be materialized and is not supported as a table column, "
                                + "persisted return type, or state type.");
    }

    @Test
    void testFunctionTypeIsRejectedAsNamedStateEntry() {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction(
                "function_state_ptf", FunctionStateProcessTableFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1)));

        // Reflection-based extraction already rejects a non-composite state type, so this pins the
        // guard for a function that declares its state types programmatically.
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery("SELECT * FROM function_state_ptf(t => TABLE t)")
                                        .execute()
                                        .await())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for state entry 'lambda'. The FUNCTION "
                                + "data type is a helper type for lambda arguments of higher-order "
                                + "functions. It cannot be materialized and is not supported as a "
                                + "table column, persisted return type, or state type.");
    }

    @Test
    void testHigherOrderCallsComposeWithProcessTableFunctions() throws Exception {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_ptf", ThresholdProcessTableFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("arr", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of("Bob", new Integer[] {1, 2, 3})));

        // a higher-order call is an ordinary scalar expression, so it composes with a PTF in every
        // direction: as a scalar argument, inside the query behind a table argument, and over the
        // PTF's result. Only a lambda *argument* of the PTF itself is out of scope (see the FLIP's
        // "Function kinds, and process table functions").
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT * FROM my_ptf(t => TABLE t, threshold => "
                                        + "ARRAY_REDUCE(ARRAY[1, 2, 3], 0, (acc, x) -> acc + x))"))
                .containsExactly("Bob:6");

        tEnv.executeSql(
                "CREATE TEMPORARY VIEW v AS "
                        + "SELECT name, ARRAY_REDUCE(arr, 0, (acc, x) -> acc + x) AS s FROM t");
        assertThat(collectColumn(tEnv, "SELECT * FROM my_ptf(t => TABLE v, threshold => 1)"))
                .containsExactly("Bob:1");

        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(ARRAY[1, 2], x -> x + 1) "
                                        + "FROM my_ptf(t => TABLE t, threshold => 1)"))
                .containsExactly((Object) new Integer[] {2, 3});
    }

    @Test
    void testViewOverUserDefinedHigherOrderFunctions() throws Exception {
        // A view persists its query expanded. Tables and catalog functions are rewritten to fully
        // qualified identifiers -- the latter through the resolved BridgingSqlFunction -- while
        // built-ins and temporary system functions stay quoted-only. A lambda sits among the
        // operands of exactly such a call, so it has to survive the unparse/re-parse round-trip --
        // its parameters unqualified, its captures qualified.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                "CREATE FUNCTION my_array_transform AS '"
                        + ArrayTransformScalarFunction.class.getName()
                        + "'");
        tEnv.createTemporarySystemFunction("my_two_lambdas", TwoLambdaScalarFunction.class);

        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("s", DataTypes.INT()),
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 1, 10, 100)));

        tEnv.executeSql(
                "CREATE VIEW v AS SELECT"
                        + " my_array_transform(f0, x -> x + a) AS c0,"
                        + " ARRAY_TRANSFORM(f0, e -> my_array_transform(f0, x -> x + e + a)) AS c1,"
                        + " my_two_lambdas(s, x -> x + a, x -> x + b) AS c2"
                        + " FROM t");

        assertThat(expandedQueryOf(tEnv, "v"))
                .isEqualTo(
                        "SELECT `default_catalog`.`default_database`.`my_array_transform`"
                                + "(`t`.`f0`, `x` -> `x` + `t`.`a`) AS `c0`,"
                                + " `ARRAY_TRANSFORM`(`t`.`f0`, `e` -> "
                                + "`default_catalog`.`default_database`.`my_array_transform`"
                                + "(`t`.`f0`, `x` -> `x` + `e` + `t`.`a`)) AS `c1`,"
                                + " `my_two_lambdas`(`t`.`s`, `x` -> `x` + `t`.`a`,"
                                + " `x` -> `x` + `t`.`b`) AS `c2`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery("SELECT * FROM v").execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {11, 12, 13});
            // the inner lambda captures both the enclosing lambda parameter and an outer column
            assertThat(row.getField(1))
                    .isEqualTo(new Integer[][] {{12, 13, 14}, {13, 14, 15}, {14, 15, 16}});
            // 100 * (1 + 10) + (1 + 100), so the per-lambda capture partitioning survives the
            // round-trip
            assertThat(row.getField(2)).isEqualTo(1201);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserSuppliedLambdaParameterDerivation() throws Exception {
        // A lambda parameter type does not have to come from the structure of a sibling argument.
        // WidenToBigIntOfArray derives BIGINT from an ARRAY<INT> argument, which none of the
        // structural derivations can produce (element/argument/key/value would yield INT,
        // ARRAY<INT>, and nothing respectively). The lambda body is therefore compiled with a
        // BIGINT parameter, so both the result *type* and the result *value* differ from what a
        // structural derivation would give -- which is what makes it observable that the function's
        // own derivation is really consulted.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        // x -> x * 3000000000: the literal does not fit in an INT, so this is only well-typed
        // because the custom strategy widened the parameter to BIGINT.
        final Expression lambda =
                new UnresolvedLambdaExpression(
                        Collections.singletonList("x"), $("x").times(lit(3000000000L)));

        final Table result =
                input.select(call(CustomStrategyArrayTransformFunction.class, $("f0"), lambda));

        // the element type is derived from the lambda's return type, which is BIGINT only if the
        // custom strategy was honoured
        assertThat(result.getResolvedSchema().getColumnDataTypes().get(0))
                .isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT()));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0))
                    .isEqualTo(new Long[] {3000000000L, 6000000000L, 9000000000L});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserSuppliedLambdaParameterDerivationReturningNoType() {
        // Failure path of the same surface: a strategy that cannot derive a type returns an empty
        // optional. The lambda parameter then has nothing to bind to, and the call must be rejected
        // with a validation error rather than resolving to an untyped lambda.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        final Expression lambda =
                new UnresolvedLambdaExpression(Collections.singletonList("x"), $("x").plus(1));

        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                UnderivableStrategyArrayTransformFunction.class,
                                                $("f0"),
                                                lambda)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Function 'UnderivableStrategyArrayTransformFunction' does not accept a "
                                + "lambda expression at position 1.");
    }

    /**
     * The query a view was persisted with, i.e. the original statement with all identifiers
     * expanded, as a single line.
     */
    private static String expandedQueryOf(TableEnvironment tEnv, String viewName) throws Exception {
        final CatalogView view =
                (CatalogView)
                        tEnv.getCatalog(tEnv.getCurrentCatalog())
                                .orElseThrow(IllegalStateException::new)
                                .getTable(new ObjectPath(tEnv.getCurrentDatabase(), viewName));
        return view.getExpandedQuery().replace('\n', ' ');
    }

    private static Object collectFirst(TableEnvironment tEnv, String sql) throws Exception {
        try (final CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            assertThat(iterator).hasNext();
            return iterator.next().getField(0);
        }
    }

    private static List<Object> collectColumn(TableEnvironment tEnv, String sql) throws Exception {
        try (final CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            final List<Object> values = new ArrayList<>();
            iterator.forEachRemaining(row -> values.add(row.getField(0)));
            return values;
        }
    }

    private static Set<Object> distinctByIdentity(List<Object> objects) {
        final Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        distinct.addAll(objects);
        return distinct;
    }

    /** A user-defined aggregate function, for the tests on aggregates in lambda bodies. */
    public static class SumAggregateFunction
            extends AggregateFunction<Integer, SumAggregateFunction.Accumulator> {

        /** Accumulator of {@link SumAggregateFunction}. */
        public static class Accumulator {
            public int sum;
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        public void accumulate(Accumulator acc, @Nullable Integer value) {
            if (value != null) {
                acc.sum += value;
            }
        }

        @Override
        public Integer getValue(Accumulator acc) {
            return acc.sum;
        }
    }

    /**
     * A {@link TestLambdaStrategies.ParameterDerivation} that is not one of the structural
     * derivations. It binds the lambda parameter to {@code BIGINT} whenever the argument at {@code
     * arrayArgumentPos} is an array of an exact numeric type, widening the element type instead of
     * passing it through. No structural derivation can produce this type from an {@code ARRAY<INT>}
     * argument, which is what makes it usable as proof that the function's own derivation is really
     * consulted.
     */
    public static class WidenToBigIntOfArray implements TestLambdaStrategies.ParameterDerivation {

        private final int arrayArgumentPos;

        public WidenToBigIntOfArray(int arrayArgumentPos) {
            this.arrayArgumentPos = arrayArgumentPos;
        }

        @Override
        public Optional<DataType> derive(CallContext callContext) {
            final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
            if (arrayArgumentPos >= argumentDataTypes.size()) {
                return Optional.empty();
            }
            final DataType arrayType = argumentDataTypes.get(arrayArgumentPos);
            if (!arrayType.getLogicalType().is(LogicalTypeRoot.ARRAY)) {
                return Optional.empty();
            }
            return Optional.of(DataTypes.BIGINT());
        }
    }

    /**
     * A {@link TestLambdaStrategies.ParameterDerivation} that never derives a type, for the failure
     * path: the lambda parameter cannot be bound, so the call must be rejected instead of resolving
     * to an untyped lambda.
     */
    public static class UnderivableLambdaParameterDerivation
            implements TestLambdaStrategies.ParameterDerivation {

        @Override
        public Optional<DataType> derive(CallContext callContext) {
            return Optional.empty();
        }
    }

    /**
     * A user-defined {@code ARRAY_TRANSFORM} whose lambda parameter type comes from the
     * user-supplied {@link WidenToBigIntOfArray} rather than from {@code elementOf(0)}. It receives
     * an {@code ARRAY<INT>} but applies the lambda to {@code Long} values.
     */
    public static class CustomStrategyArrayTransformFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(new WidenToBigIntOfArray(0))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            if (array == null) {
                return null;
            }
            final Long[] result = new Long[array.length];
            for (int i = 0; i < array.length; i++) {
                result[i] = array[i] == null ? null : lambda.apply(array[i].longValue());
            }
            return result;
        }
    }

    /**
     * A user-defined higher-order function whose lambda parameter strategy never derives a type,
     * for the failure path of the user-supplied-strategy surface.
     */
    public static class UnderivableStrategyArrayTransformFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(
                                            new UnderivableLambdaParameterDerivation())))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.INT())))
                    .build();
        }

        public @Nullable Integer[] eval(
                @Nullable Integer[] array, Function<Integer, Integer> lambda) {
            return array;
        }
    }

    /** A table function, for the negative tests on lambda bodies. */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class SplitTableFunction extends TableFunction<Row> {

        public void eval(String s) {
            for (String part : s.split(" ")) {
                collect(Row.of(part));
            }
        }
    }

    /** An ordinary (non-higher-order) function, used inside lambda bodies. */
    public static class PlusOneScalarFunction extends ScalarFunction {

        public @Nullable Integer eval(@Nullable Integer x) {
            return x == null ? null : x + 1;
        }
    }

    /** An ordinary (non-higher-order) function of two arguments, used inside lambda bodies. */
    public static class AddScalarFunction extends ScalarFunction {

        public @Nullable Integer eval(@Nullable Integer x, @Nullable Integer y) {
            if (x == null || y == null) {
                return null;
            }
            return x + y;
        }
    }

    /**
     * An ordinary function that adds an offset established in {@link #open(FunctionContext)}. A
     * result of {@code x} instead of {@code x + 1000} would mean that the function was called
     * without having been opened.
     */
    public static class OpenedScalarFunction extends ScalarFunction {

        private transient int offset;

        @Override
        public void open(FunctionContext context) {
            offset = 1000;
        }

        public @Nullable Integer eval(@Nullable Integer x) {
            return x == null ? null : x + offset;
        }
    }

    /**
     * A user-defined {@code ARRAY_TRANSFORM} taking {@code (ARRAY<E>, E -> R)} and returning {@code
     * ARRAY<R>}. The lambda is received as a first-class {@link Function} object ("Option A"): the
     * framework compiles the lambda body and binds any captured columns behind the object, so the
     * function simply calls {@code lambda.apply(element)} and owns its loop.
     */
    public static class ArrayTransformScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Integer[] eval(
                @Nullable Integer[] array, Function<Integer, Integer> lambda) {
            if (array == null) {
                return null;
            }
            final Integer[] result = new Integer[array.length];
            for (int i = 0; i < array.length; i++) {
                result[i] = lambda.apply(array[i]);
            }
            return result;
        }
    }

    /**
     * A user-defined {@code ARRAY_TRANSFORM} whose lambda maps each element to a different logical
     * type ({@code Integer -> String}). The output type strategy derives the array element type
     * from the lambda argument's return data type, so the resolved result type must be {@code
     * ARRAY<STRING>} rather than the input's {@code ARRAY<INT>}. This exercises polymorphic
     * (type-changing) UDF lambda results.
     */
    public static class ArrayTransformToStringScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable String[] eval(
                @Nullable Integer[] array, Function<Integer, String> lambda) {
            if (array == null) {
                return null;
            }
            final String[] result = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                result[i] = lambda.apply(array[i]);
            }
            return result;
        }
    }

    /**
     * A user-defined {@code ARRAY_TRANSFORM} over an array of arrays: it takes {@code
     * (ARRAY<ARRAY<ARRAY<E>>>, ARRAY<ARRAY<E>> -> ARRAY<ARRAY<R>>)} and returns {@code
     * ARRAY<ARRAY<ARRAY<R>>>}. The lambda parameter is itself a nested array, so the {@link
     * Function} object handed to {@code eval} maps {@code Integer[][]} to {@code Integer[][]}. Used
     * to alternate user-defined and built-in higher-order calls across more than two nesting
     * levels.
     */
    public static class NestedArrayTransformScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Integer[][][] eval(
                @Nullable Integer[][][] array, Function<Integer[][], Integer[][]> lambda) {
            if (array == null) {
                return null;
            }
            final Integer[][][] result = new Integer[array.length][][];
            for (int i = 0; i < array.length; i++) {
                result[i] = lambda.apply(array[i]);
            }
            return result;
        }
    }

    /**
     * A user-defined {@code ARRAY_REDUCE} taking {@code (ARRAY<E>, A, (A, E) -> A)} and returning
     * the accumulator type {@code A}. Exercises a two-parameter lambda (received as a {@link
     * BiFunction}) whose first parameter is bound to the initial-accumulator argument via {@link
     * TestLambdaStrategies#argumentOf(int)}.
     */
    public static class ArrayReduceScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.ANY,
                                    TestLambdaStrategies.lambda(
                                            TestLambdaStrategies.argumentOf(1),
                                            TestLambdaStrategies.elementOf(0))))
                    // result type is the accumulator (initial-value) type
                    .outputTypeStrategy(call -> Optional.of(call.getArgumentDataTypes().get(1)))
                    .build();
        }

        public @Nullable Integer eval(
                @Nullable Integer[] array,
                Integer initial,
                BiFunction<Integer, Integer, Integer> lambda) {
            if (array == null) {
                return null;
            }
            Integer acc = initial;
            for (final Integer element : array) {
                acc = lambda.apply(acc, element);
            }
            return acc;
        }
    }

    /**
     * A user-defined higher-order function taking three arrays and a three-parameter lambda {@code
     * (E1, E2, E3) -> R}, returning {@code ARRAY<R>} of the lambda applied element-wise. Exercises
     * a three-parameter lambda, received as a first-class {@link TriFunction} object (the arity-3
     * conversion class of the {@code FUNCTION} type).
     */
    public static class Zip3ScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(
                                            TestLambdaStrategies.elementOf(0),
                                            TestLambdaStrategies.elementOf(1),
                                            TestLambdaStrategies.elementOf(2))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(3)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Integer[] eval(
                @Nullable Integer[] a,
                @Nullable Integer[] b,
                @Nullable Integer[] c,
                TriFunction<Integer, Integer, Integer, Integer> lambda) {
            if (a == null || b == null || c == null) {
                return null;
            }
            final Integer[] result = new Integer[a.length];
            for (int i = 0; i < a.length; i++) {
                result[i] = lambda.apply(a[i], b[i], c[i]);
            }
            return result;
        }
    }

    /**
     * A user-defined {@code ARRAY_TRANSFORM} whose lambda argument is <em>followed</em> by a
     * regular argument: {@code (ARRAY<INT>, INT -> INT, INT)}. Used by {@link
     * #testUserDefinedLambdaFollowedByRegularArgument()} to verify that lifted captures, which are
     * appended behind all declared arguments, do not shift the trailing regular argument.
     */
    public static class OffsetScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0)),
                                    TestLambdaStrategies.logical(LogicalTypeRoot.INTEGER)))
                    .outputTypeStrategy(call -> Optional.of(DataTypes.ARRAY(DataTypes.INT())))
                    .build();
        }

        public @Nullable Integer[] eval(
                @Nullable Integer[] array, Function<Integer, Integer> lambda, Integer offset) {
            if (array == null) {
                return null;
            }
            final Integer[] result = new Integer[array.length];
            for (int i = 0; i < array.length; i++) {
                result[i] = lambda.apply(array[i]) + offset;
            }
            return result;
        }
    }

    /**
     * A user-defined {@code MAP_TRANSFORM_VALUES} taking {@code (MAP<K, V>, (K, V) -> V)}.
     * Exercises a lambda over a map argument, whose two parameters are bound to the key and value
     * type of that argument via {@link TestLambdaStrategies#keyOf(int)} and {@link
     * TestLambdaStrategies#valueOf(int)}.
     */
    public static class MapValuesScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.MAP),
                                    TestLambdaStrategies.lambda(
                                            TestLambdaStrategies.keyOf(0),
                                            TestLambdaStrategies.valueOf(0))))
                    .outputTypeStrategy(call -> Optional.of(call.getArgumentDataTypes().get(0)))
                    .build();
        }

        public @Nullable Map<String, Integer> eval(
                @Nullable Map<String, Integer> map, BiFunction<String, Integer, Integer> lambda) {
            if (map == null) {
                return null;
            }
            final Map<String, Integer> result = new HashMap<>();
            map.forEach((key, value) -> result.put(key, lambda.apply(key, value)));
            return result;
        }
    }

    /**
     * A user-defined function that declares <em>two</em> lambda arguments and applies each to the
     * integer argument, combining the two results as {@code 100 * first + second}. Exercises
     * multiple lambda operands per call: capture lifting lifts each lambda's captures independently
     * and code generation partitions the trailing capture operands back to the owning lambda.
     *
     * <p>The combination is deliberately <em>not</em> commutative. Adding the two results would
     * make the tests blind to the very thing they pin: swapping the two lambdas, or mis-assigning a
     * lifted capture operand to the wrong lambda, would leave the sum unchanged.
     *
     * <p>Used by {@link #testUserDefinedFunctionWithTwoLambdas()}, {@link
     * #testUserDefinedFunctionWithTwoLambdasCapturingDistinctColumns()}, {@link
     * #testUserDefinedFunctionWithTwoLambdasNestedInBuiltIn()} and {@link
     * #testViewOverUserDefinedHigherOrderFunctions()}.
     */
    public static class TwoLambdaScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.INTEGER),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.argumentOf(0)),
                                    TestLambdaStrategies.lambda(
                                            TestLambdaStrategies.argumentOf(0))))
                    .outputTypeStrategy(call -> Optional.of(DataTypes.INT()))
                    .build();
        }

        public @Nullable Integer eval(
                @Nullable Integer value,
                Function<Integer, Integer> first,
                Function<Integer, Integer> second) {
            if (value == null) {
                return null;
            }
            return 100 * first.apply(value) + second.apply(value);
        }
    }

    /**
     * A user-defined function that declares a lambda argument with four parameters, which has no
     * functional interface to be passed as. Used by {@link
     * #testLambdaWithUnsupportedParameterCountIsRejected()}; building its type inference already
     * fails.
     */
    public static class FourParameterLambdaScalarFunction extends ScalarFunction {
        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(
                                            TestLambdaStrategies.elementOf(0),
                                            TestLambdaStrategies.elementOf(0),
                                            TestLambdaStrategies.elementOf(0),
                                            TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(call -> Optional.of(DataTypes.INT()))
                    .build();
        }

        public @Nullable Integer eval(@Nullable Integer[] array, Object lambda) {
            return null;
        }
    }

    /**
     * A user-defined higher-order <i>table</i> function taking {@code (ARRAY<E>, E -> R)} and
     * emitting one row per transformed element. A table function's call is compiled into a scalar
     * expression like a scalar function's, so it hosts a lambda argument the same way.
     */
    @FunctionHint(output = @DataTypeHint("ROW<v INT>"))
    public static class ExplodeTransformTableFunction extends TableFunction<Row> {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT()))))
                    .build();
        }

        public void eval(@Nullable Integer[] array, Function<Integer, Integer> lambda) {
            if (array == null) {
                return;
            }
            for (Integer element : array) {
                collect(Row.of(lambda.apply(element)));
            }
        }
    }

    /**
     * A user-defined aggregate function that declares a lambda argument. An aggregate folds rows,
     * so it cannot host a lambda body; declaring one is rejected. See {@link
     * #testLambdaArgumentIsRejectedForUnsupportedFunctionKinds()}.
     */
    public static class LambdaAggregateFunction extends AggregateFunction<Integer, Integer[]> {

        @Override
        public Integer[] createAccumulator() {
            return new Integer[] {0};
        }

        public void accumulate(Integer[] acc, Integer[] array, Function<Integer, Integer> lambda) {
            for (Integer element : array) {
                acc[0] += lambda.apply(element);
            }
        }

        @Override
        public Integer getValue(Integer[] accumulator) {
            return accumulator[0];
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .accumulatorTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.INT())))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * An ordinary process table function taking a table and a scalar threshold, used to pin that a
     * higher-order <i>call</i> composes with a PTF even though a lambda <i>argument</i> of a PTF is
     * out of scope.
     */
    public static class ThresholdProcessTableFunction extends ProcessTableFunction<String> {

        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer threshold) {
            collect(t.getField(0) + ":" + threshold);
        }
    }

    /**
     * A process table function that declares a lambda argument. A PTF consumes a table rather than
     * a row, so it cannot host a lambda body; declaring one is rejected. See {@link
     * #testLambdaArgumentIsRejectedForUnsupportedFunctionKinds()}.
     */
    public static class LambdaProcessTableFunction extends ProcessTableFunction<Integer> {

        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t,
                Function<Integer, Integer> lambda) {
            collect(lambda.apply(1));
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.explicit(
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(
                                                            "f0",
                                                            DataTypes.ARRAY(DataTypes.INT())))),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * An asynchronous scalar function that declares a lambda argument. A lambda object is bound to
     * the invocation of {@code eval} that received it, so it cannot be handed to the thread that
     * completes the future; declaring one is rejected. See {@link
     * #testLambdaArgumentIsRejectedForAsynchronousFunctionKinds()}.
     */
    public static class LambdaAsyncScalarFunction extends AsyncScalarFunction {

        public void eval(
                CompletableFuture<Integer> future,
                Integer[] array,
                Function<Integer, Integer> lambda) {
            future.complete(lambda.apply(array[0]));
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * An asynchronous table function that declares a lambda argument, rejected for the same reason
     * as {@link LambdaAsyncScalarFunction}.
     */
    public static class LambdaAsyncTableFunction extends AsyncTableFunction<Integer> {

        public void eval(
                CompletableFuture<Collection<Integer>> future,
                Integer[] array,
                Function<Integer, Integer> lambda) {
            future.complete(Collections.singletonList(lambda.apply(array[0])));
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /** An ordinary asynchronous scalar function, for the composition control. */
    public static class AsyncPlusOneScalarFunction extends AsyncScalarFunction {

        public void eval(CompletableFuture<Integer> future, Integer i) {
            future.complete(i + 1);
        }
    }

    /**
     * A higher-order function that never applies its lambda, to pin that the framework does not
     * evaluate a lambda body on its own.
     */
    public static class IgnoringLambdaScalarFunction extends ScalarFunction {

        public Integer eval(Integer[] array, Function<Integer, Integer> lambda) {
            return array.length;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * A higher-order function that applies its lambda twice per element and visits the array back
     * to front, to pin that the function owns the number and order of applications.
     */
    public static class ApplyTwiceScalarFunction extends ScalarFunction {

        public Integer[] eval(Integer[] array, Function<Integer, Integer> lambda) {
            final Integer[] result = new Integer[array.length * 2];
            for (int i = 0; i < array.length; i++) {
                final Integer element = array[array.length - 1 - i];
                result[2 * i] = lambda.apply(element);
                result[2 * i + 1] = lambda.apply(element);
            }
            return result;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }
    }

    /**
     * A higher-order function that records the lambda object it received, to pin that a fresh one
     * is created for every evaluation of the enclosing call. Retaining the bare reference without
     * ever applying it again is what makes the identity observation sound; a real function must not
     * retain a lambda object at all.
     */
    public static class LambdaIdentityScalarFunction extends ScalarFunction {

        static final List<Object> SEEN = Collections.synchronizedList(new ArrayList<>());

        public Integer eval(Integer[] array, Function<Integer, Integer> lambda) {
            SEEN.add(lambda);
            return lambda.apply(array[0]);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * A higher-order function that catches what {@code apply} throws and reports it, to pin the
     * exception contract of a lambda application.
     */
    public static class CatchingLambdaScalarFunction extends ScalarFunction {

        public String eval(Integer[] array, Function<Integer, Integer> lambda) {
            try {
                lambda.apply(array[0]);
                return "no error";
            } catch (Throwable t) {
                return t.getClass().getName() + ": " + t.getMessage();
            }
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING()))
                    .build();
        }
    }

    /** An ordinary scalar function that fails, for use in a lambda body. */
    public static class ThrowingScalarFunction extends ScalarFunction {

        public Integer eval(Integer i) {
            throw new IllegalStateException("boom for " + i);
        }
    }

    /**
     * An ordinary scalar function that counts its own lifecycle calls, for use in a lambda body.
     */
    public static class LifecycleTrackingScalarFunction extends ScalarFunction {

        static final AtomicInteger OPEN_COUNT = new AtomicInteger();
        static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        @Override
        public void open(FunctionContext context) {
            OPEN_COUNT.incrementAndGet();
        }

        @Override
        public void close() {
            CLOSE_COUNT.incrementAndGet();
        }

        public Integer eval(Integer i) {
            return i + 1;
        }
    }

    /** A function that declares a {@code FUNCTION} return type, which is never materializable. */
    public static class FunctionReturningFunction extends ScalarFunction {

        public Object eval() {
            return null;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(TestLambdaStrategies.sequence())
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.FUNCTION(1)))
                    .build();
        }
    }

    /** Like {@link FunctionReturningFunction}, but with the type nested in an array. */
    public static class FunctionReturningNestedFunction extends ScalarFunction {

        public Object eval() {
            return null;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(TestLambdaStrategies.sequence())
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.FUNCTION(1))))
                    .build();
        }
    }

    /**
     * An aggregate function that declares a {@code FUNCTION} accumulator. A lambda object is valid
     * only for the {@code eval} invocation it is passed to and has no serialized form, so it can
     * never be kept between invocations. See {@link #testFunctionTypeIsRejectedAsStateType()}.
     */
    public static class FunctionAccumulatorAggregateFunction
            extends AggregateFunction<Integer, Object> {

        @Override
        public Object createAccumulator() {
            return null;
        }

        public void accumulate(Object acc, Integer value) {}

        @Override
        public Integer getValue(Object accumulator) {
            return null;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.INTEGER)))
                    .accumulatorTypeStrategy(TypeStrategies.explicit(DataTypes.FUNCTION(1)))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * Like {@link FunctionAccumulatorAggregateFunction}, but for a named state entry of a process
     * table function. State types are declared programmatically here because reflective extraction
     * rejects a non-composite state type earlier. See {@link
     * #testFunctionTypeIsRejectedAsNamedStateEntry()}.
     */
    public static class FunctionStateProcessTableFunction extends ProcessTableFunction<Integer> {

        public void eval(Function<Integer, Integer> lambda, Row t) {
            collect(1);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            final LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies =
                    new LinkedHashMap<>();
            stateTypeStrategies.put(
                    "lambda", StateTypeStrategy.of(TypeStrategies.explicit(DataTypes.FUNCTION(1))));
            return TypeInference.newBuilder()
                    .staticArguments(
                            StaticArgument.table(
                                    "t",
                                    Row.class,
                                    false,
                                    EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)))
                    .stateTypeStrategies(stateTypeStrategies)
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }
}
