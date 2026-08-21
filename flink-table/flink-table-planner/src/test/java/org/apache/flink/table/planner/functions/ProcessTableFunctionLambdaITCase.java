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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.Function2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for lambdas in combination with {@link ProcessTableFunction}s: lambda arguments
 * of a PTF, declared in its static signature, and higher-order calls around a PTF call.
 */
class ProcessTableFunctionLambdaITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @Test
    void testProcessTableFunctionWithLambdaArgument() throws Exception {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_lambda_ptf", LambdaProcessTableFunction.class);
        tEnv.createTemporaryView("t", scoresTable(tEnv));

        // the parameter type is derived from the table argument's 'score' column
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT * FROM my_lambda_ptf(t => TABLE t, f => x -> x * 10)"))
                .containsExactly(10, 20, 30);

        // positional arguments, a nested higher-order call, and a lambda the planner reduces
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT * FROM my_lambda_ptf(TABLE t, "
                                        + "x -> ARRAY_REDUCE(ARRAY[x, x], 0, (acc, y) -> acc + y))"))
                .containsExactly(2, 4, 6);
        assertThat(collectColumn(tEnv, "SELECT * FROM my_lambda_ptf(TABLE t, x -> 1 + 1)"))
                .containsExactly(2, 2, 2);

        final Table result =
                tEnv.from("t")
                        .process(
                                "my_lambda_ptf",
                                new UnresolvedLambdaExpression(
                                        Collections.singletonList("x"), $("x").plus(100)));
        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            final List<Object> values = new ArrayList<>();
            iterator.forEachRemaining(row -> values.add(row.getField(0)));
            assertThat(values).containsExactly(101, 102, 103);
        }
    }

    @Test
    void testProcessTableFunctionWithLambdaArgumentAndState() throws Exception {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_fold_ptf", RunningFoldProcessTableFunction.class);
        tEnv.createTemporaryView("t", scoresTable(tEnv));

        // the lambda folds every row of a partition into the state; the object handed to eval()
        // never reaches the state itself, only the values it computes
        final List<Row> rows = new ArrayList<>();
        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery(
                                "SELECT * FROM my_fold_ptf("
                                        + "t => TABLE t PARTITION BY name, "
                                        + "f => (acc, x) -> acc * 10 + x)")
                        .execute()
                        .collect()) {
            iterator.forEachRemaining(rows::add);
        }
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of("Bob", 1), Row.of("Alice", 2), Row.of("Bob", 13));
    }

    @Test
    void testProcessTableFunctionWithLambdaArgumentInCompiledPlan() throws Exception {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_lambda_ptf", LambdaProcessTableFunction.class);
        tEnv.createTemporaryView("t", scoresTable(tEnv));
        tEnv.executeSql(
                "CREATE TABLE sink (v INT) WITH ('connector' = 'values', 'sink-insert-only' = 'true')");

        // the lambda is persisted with the PTF call and recompiled when the plan is restored
        final String json =
                tEnv.compilePlanSql(
                                "INSERT INTO sink SELECT * FROM my_lambda_ptf("
                                        + "t => TABLE t, f => x -> x * 10)")
                        .asJsonString();
        try {
            tEnv.loadPlan(PlanReference.fromJsonString(json)).execute().await();
            assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                    .containsExactlyInAnyOrder("+I[10]", "+I[20]", "+I[30]");
        } finally {
            TestValuesTableFactory.clearAllData();
        }
    }

    @Test
    void testInvalidProcessTableFunctionLambdaArgument() {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_lambda_ptf", LambdaProcessTableFunction.class);
        tEnv.createTemporaryView("t", scoresTable(tEnv));

        // a PTF locates its system arguments from the end of its operand list, which leaves no room
        // for the trailing operands a captured value is passed in. Columns are not in scope of a
        // PTF's scalar arguments anyway, so this only affects sub-queries.
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT * FROM my_lambda_ptf(t => TABLE t, "
                                                + "f => x -> x + (SELECT MAX(score) FROM t))"))
                .hasMessageContaining(
                        "A lambda argument of process table function 'my_lambda_ptf' may only "
                                + "reference its own parameters, literals, and the parameters of "
                                + "lambdas nested in its body.");

        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT * FROM my_lambda_ptf(t => TABLE t, "
                                                + "f => (x, y) -> x + y)"))
                .hasMessageContaining(
                        "The lambda expression at position 1 expects 1 parameter(s) but 2 were "
                                + "provided.");
    }

    @Test
    void testProcessTableFunctionWithTwoLambdaArguments() throws Exception {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction(
                "my_two_lambda_ptf", TwoLambdaProcessTableFunction.class);
        tEnv.createTemporaryView("t", scoresTable(tEnv));

        // named arguments bind each lambda to its declared position, whatever the order of the call
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT * FROM my_two_lambda_ptf("
                                        + "g => (n, s) -> n || ':' || CAST(s AS STRING), "
                                        + "t => TABLE t, "
                                        + "f => x -> x * 10)"))
                .containsExactly("Bob:10", "Alice:20", "Bob:30");
    }

    @Test
    void testProcessTableFunctionLambdaArgumentRequiresLambdaInputTypeStrategy() {
        // PTFs are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction(
                "my_annotated_ptf", AnnotatedLambdaProcessTableFunction.class);
        tEnv.createTemporaryView("t", scoresTable(tEnv));

        // the eval() method fixes the position and arity of the lambda, but its parameter types
        // depend on the call and can only be derived by a LambdaInputTypeStrategy
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT * FROM my_annotated_ptf("
                                                + "t => TABLE t, f => x -> x * 10)"))
                .hasMessageContaining(
                        "Lambda argument 'f' requires an input type strategy that implements "
                                + "LambdaInputTypeStrategy, which derives the types of the lambda "
                                + "parameters.")
                .hasMessageContaining("override getTypeInference()");
    }

    private static Table scoresTable(TableEnvironment tEnv) {
        return tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT())),
                Row.of("Bob", 1),
                Row.of("Alice", 2),
                Row.of("Bob", 3));
    }

    @Test
    void testLambdaArgumentInAStaticSignature() {
        // an omitted lambda would reach eval() as null, which no lambda object represents
        assertThatThrownBy(() -> StaticArgument.scalar("f", DataTypes.FUNCTION(1), true))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Lambda argument 'f' must not be optional.");

        // only a process table function declares its lambda arguments in the static signature
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_static_lambda", StaticLambdaArgumentScalarFunction.class);
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_static_lambda(x -> x * 2)"))
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for argument 'f'. Lambda arguments in a "
                                + "static signature are only supported for process table "
                                + "functions. Declare them with a LambdaInputTypeStrategy "
                                + "in getTypeInference() instead.");

        // nested in a constructed type it is not a lambda argument at all -- no call can supply a
        // lambda there -- so it is rejected as an unmaterializable type instead
        assertThatThrownBy(
                        () ->
                                StaticArgument.scalar(
                                        "f", DataTypes.ARRAY(DataTypes.FUNCTION(1)), false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'ARRAY<FUNCTION(1)>' for argument 'f'. The FUNCTION "
                                + "data type is a helper type for lambda arguments of higher-order "
                                + "functions. It cannot be materialized and must not be nested in "
                                + "another data type.");
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
        // PTF's result. A lambda *argument* of the PTF itself is covered by
        // testProcessTableFunctionWithLambdaArgument.
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

    /**
     * An ordinary process table function taking a table and a scalar threshold, used to pin that a
     * higher-order <i>call</i> composes with a PTF.
     */
    public static class ThresholdProcessTableFunction extends ProcessTableFunction<String> {

        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t,
                @ArgumentHint(ArgumentTrait.SCALAR) Integer threshold) {
            collect(t.getField(0) + ":" + threshold);
        }
    }

    /**
     * Declares the lambda argument of a process table function, deriving its parameter types from
     * columns of the table argument. The static signature fixes the lambda's position and arity, so
     * the strategy relies on the default methods of {@link LambdaInputTypeStrategy} for everything
     * else, which must accept the trailing system arguments as well.
     */
    private static LambdaInputTypeStrategy tableColumnLambdaStrategy(
            int lambdaPos, String... columns) {
        return (callContext, argumentPos) ->
                argumentPos == lambdaPos
                        ? Optional.of(tableColumnTypes(callContext, columns))
                        : Optional.empty();
    }

    private static List<DataType> tableColumnTypes(CallContext callContext, String... columns) {
        final DataType tableType = callContext.getArgumentDataTypes().get(0);
        final List<String> fieldNames = DataType.getFieldNames(tableType);
        final List<DataType> fieldTypes = DataType.getFieldDataTypes(tableType);
        final List<DataType> types = new ArrayList<>();
        for (String column : columns) {
            types.add(fieldTypes.get(fieldNames.indexOf(column)));
        }
        return types;
    }

    /**
     * A process table function with row semantics that applies its lambda argument to the 'score'
     * column of every row.
     */
    public static class LambdaProcessTableFunction extends ProcessTableFunction<Integer> {

        public void eval(Row t, Function<Integer, Integer> f) {
            collect(f.apply(t.getFieldAs("score")));
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .staticArguments(
                            StaticArgument.table(
                                    "t",
                                    Row.class,
                                    false,
                                    EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)),
                            StaticArgument.scalar("f", DataTypes.FUNCTION(1), false))
                    .inputTypeStrategy(tableColumnLambdaStrategy(1, "score"))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * A process table function with set semantics that folds the 'score' column of a partition with
     * its two-parameter lambda argument and keeps the running result in state.
     */
    public static class RunningFoldProcessTableFunction extends ProcessTableFunction<Integer> {

        public void eval(Row state, Row t, Function2<Integer, Integer, Integer> f) {
            final Integer acc = state.getFieldAs("acc");
            final Integer folded =
                    acc == null ? t.getFieldAs("score") : f.apply(acc, t.getFieldAs("score"));
            state.setField("acc", folded);
            collect(folded);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            final LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies =
                    new LinkedHashMap<>();
            stateTypeStrategies.put(
                    "state",
                    StateTypeStrategy.of(
                            TypeStrategies.explicit(
                                    DataTypes.ROW(DataTypes.FIELD("acc", DataTypes.INT())))));
            return TypeInference.newBuilder()
                    .staticArguments(
                            StaticArgument.table(
                                    "t",
                                    Row.class,
                                    false,
                                    EnumSet.of(StaticArgumentTrait.SET_SEMANTIC_TABLE)),
                            StaticArgument.scalar("f", DataTypes.FUNCTION(2), false))
                    .stateTypeStrategies(stateTypeStrategies)
                    .inputTypeStrategy(tableColumnLambdaStrategy(1, "score", "score"))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * A process table function with row semantics and two lambda arguments: 'f' maps the 'score'
     * column, and 'g' combines the 'name' column with the result of 'f'.
     */
    public static class TwoLambdaProcessTableFunction extends ProcessTableFunction<String> {

        public void eval(
                Row t, Function<Integer, Integer> f, Function2<String, Integer, String> g) {
            collect(g.apply(t.getFieldAs("name"), f.apply(t.getFieldAs("score"))));
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            final LambdaInputTypeStrategy strategy =
                    (callContext, argumentPos) -> {
                        switch (argumentPos) {
                            case 1:
                                return Optional.of(tableColumnTypes(callContext, "score"));
                            case 2:
                                return Optional.of(tableColumnTypes(callContext, "name", "score"));
                            default:
                                return Optional.empty();
                        }
                    };
            return TypeInference.newBuilder()
                    .staticArguments(
                            StaticArgument.table(
                                    "t",
                                    Row.class,
                                    false,
                                    EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)),
                            StaticArgument.scalar("f", DataTypes.FUNCTION(1), false),
                            StaticArgument.scalar("g", DataTypes.FUNCTION(2), false))
                    .inputTypeStrategy(strategy)
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING()))
                    .build();
        }
    }

    /**
     * A process table function whose lambda argument is extracted from its eval() method, which
     * yields the lambda's position and arity but no {@link LambdaInputTypeStrategy} that derives
     * the lambda's parameter types.
     */
    public static class AnnotatedLambdaProcessTableFunction extends ProcessTableFunction<Integer> {

        public void eval(
                @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row t,
                Function<Integer, Integer> f) {
            collect(f.apply(t.getFieldAs("score")));
        }
    }

    /**
     * A scalar function that declares a lambda argument in a static signature, which only a process
     * table function may do.
     */
    public static class StaticLambdaArgumentScalarFunction extends ScalarFunction {

        public Integer eval(Function<Integer, Integer> f) {
            return f.apply(1);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .staticArguments(StaticArgument.scalar("f", DataTypes.FUNCTION(1), false))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    private static List<Object> collectColumn(TableEnvironment tEnv, String sql) throws Exception {
        try (final CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            final List<Object> values = new ArrayList<>();
            iterator.forEachRemaining(row -> values.add(row.getField(0)));
            return values;
        }
    }
}
