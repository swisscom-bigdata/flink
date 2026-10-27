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

import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.clock.RelativeClock;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Batch execution coverage for the built-in array higher-order functions, plus the cross-cutting
 * SQL/Table API semantics that are asserted once rather than per runtime mode. The {@link
 * HigherOrderFunctionsITCase} suite runs the same built-ins in streaming mode; the duplication
 * across modes and across the SQL / Table API surfaces is deliberate, since batch/stream
 * equivalence and surface parity are requirements of FLINK-31207.
 *
 * <p>The class name states the runtime mode, but the file also hosts the areas below. They live
 * here because they share the fixtures at the bottom of the file ({@code higherOrderFunctionInput},
 * {@code groupedHigherOrderFunctionInput}, {@code collectSingle*}, {@code assertInvalidCall} and
 * friends). Grep for the method prefix to find an area:
 *
 * <ul>
 *   <li>{@code testArrayHigherOrderFunctions*}, {@code testHigherOrderFunctionsTableApi} — the
 *       built-ins end to end, in batch, on both surfaces.
 *   <li>{@code testInvalidArguments*}, {@code testFilterPredicateAndReducerContract*}, {@code
 *       testLambdaOutsideOfHigherOrderFunctionIsRejected}, {@code
 *       testDuplicateLambdaParameterNamesAreRejected}, {@code
 *       testLambdaParameterCountMismatchIsRejectedOnTableApi}, {@code
 *       testFunctionReferenceAsLambdaIsRejected} — validation and signature error messages.
 *   <li>{@code *InLambdaBodyIsRejected} — table, asynchronous and Python functions, which cannot be
 *       called from a lambda body.
 *   <li>{@code testArrayFilterOverEnclosingLambdaParameter}, {@code
 *       testLambdaParameterShadowingOnTableApi}, {@code testFieldAccessOnRowLambdaParameter},
 *       {@code testFieldAccessInNestedLambdaBody}, {@code
 *       testCollectionConstructorsOverLambdaParameter}, {@code
 *       testMutableLambdaResultsAreNotAliased} — lambda parameter scoping and the no-aliasing rule
 *       for mutable results.
 *   <li>{@code testFunctionTypeRejected*} — the {@code FUNCTION} type cannot surface as a table
 *       column, in a resolved schema, across a catalog round-trip, or as the return or state type
 *       of a function.
 *   <li>{@code testAggregate*}, {@code testCaptureOfGroupKeyInLambdaBody} — aggregates in and
 *       around lambda bodies, and capture of a group key.
 *   <li>{@code testOverWindowInLambdaBody*} — {@code OVER} windows in a lambda body.
 *   <li>{@code testSubQueryInLambdaBody}, {@code testOuterColumnCaptureFromLambdaInScalarSubquery}
 *       — sub-queries in a lambda body and capture across a sub-query boundary.
 *   <li>{@code testLambdaCaptureInJoinCondition} — capture of either join input in a join
 *       condition.
 *   <li>{@code testLambdaInStandaloneSqlExpression} — lambdas in a computed column and in {@code
 *       callSql}, which are validated and converted outside of a query.
 *   <li>{@code testLambdaInWatermarkExpression}, {@code testLambdaInPartitionPredicate} — lambdas
 *       in generated classes without a runtime context: a watermark generator, with and without
 *       push-down into the source, and the planner's partition pruning.
 *   <li>{@code testLambdaPredicateIsNotOfferedToSource} — a predicate with a lambda is never
 *       offered to a source's filter push-down.
 *   <li>{@code testViewOver*} — the view / SQL-expansion round-trip.
 *   <li>{@code testNonDeterministicLambdaBody*} — per-call re-evaluation of a non-deterministic
 *       body; see also {@code HigherOrderFunctionDeterminismTest} for the planner-level analysis.
 *   <li>{@code testUserDefinedFunctionInLambdaBody*}, {@code
 *       testHigherOrderFunctionAsArgumentOfUserDefinedFunction}, {@code
 *       testLambdaBodyErrorPropagatesUnchanged} — ordinary user-defined functions in and around a
 *       lambda body: captures, nesting, lifecycle and error propagation.
 *   <li>{@code testLargeLambdaBodyIsSplitIntoSeparateMethods} — generated-code splitting for a body
 *       that exceeds the method size limit.
 * </ul>
 *
 * <p>Despite the class name, four tests create a streaming environment because the feature under
 * test exists only there: {@code testFunctionTypeRejectedAsNamedStateEntry} (process table
 * functions), {@code testOverWindowInLambdaBodyTableApi} (an event-time {@code OVER} window), and
 * {@code testLambdaInWatermarkExpression} and {@code
 * testUserDefinedFunctionInPushedDownWatermarkLambdaUsesSourceMetricGroup} (watermarks).
 *
 * <p>Only the first bullet duplicates {@link HigherOrderFunctionsITCase}; every other area is
 * mode-independent and has no streaming counterpart. Should {@link BuiltInFunctionTestBase} ever be
 * parameterized over the runtime mode, those two methods are what it would absorb — the rest of
 * this file would stay as it is.
 */
class HigherOrderFunctionsBatchITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @Test
    void testArrayHigherOrderFunctionsBatch() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        // a connector rather than fromValues, which cannot express an empty array literal
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(new Integer[] {1, 2, 3}, new Integer[] {10, 20}),
                                Row.of(null, new Integer[] {1}),
                                Row.of(new Integer[] {1, null, 3}, new Integer[] {10, 20, 30}),
                                Row.of(new Integer[0], new Integer[0])));
        tEnv.executeSql(
                "CREATE TABLE t (f0 ARRAY<INT>, f1 ARRAY<INT>) WITH ("
                        + " 'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");

        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, x -> x + 1),"
                                + " ARRAY_FILTER(f0, x -> x > 1),"
                                + " ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x),"
                                + " ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + COALESCE(y, 0)) FROM t");

        // a NULL array gives NULL, a NULL element reaches the lambda (and a NULL predicate
        // drops it), an empty array gives an empty array or the initial accumulator
        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            final List<Row> rows = new ArrayList<>();
            iterator.forEachRemaining(rows::add);
            assertThat(rows)
                    .containsExactlyInAnyOrder(
                            Row.of(
                                    new Integer[] {2, 3, 4},
                                    new Integer[] {2, 3},
                                    6,
                                    new Integer[] {11, 22, 3}),
                            Row.of(null, null, null, null),
                            Row.of(
                                    new Integer[] {2, null, 4},
                                    new Integer[] {3},
                                    null,
                                    new Integer[] {11, null, 33}),
                            Row.of(new Integer[0], new Integer[0], 0, new Integer[0]));
        }
    }

    @Test
    void testHigherOrderFunctionsTableApi() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a1", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("a2", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(new Integer[] {1, 2, 3}, new Integer[] {10, 20}));

        final Table result =
                input.select(
                        org.apache.flink.table.api.Expressions.$("a1")
                                .arrayZipWith(
                                        org.apache.flink.table.api.Expressions.$("a2"),
                                        (x, y) -> x.plus(y.ifNull(0))));

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {11, 22, 3});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testInvalidArgumentsAreRejectedWithTheFunctionSignature() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // An argument of the wrong type, a lambda with the wrong number of parameters, or a regular
        // expression where a lambda is expected are all reported with the call's argument types and
        // the function's supported signature. Bridging a function does not change which of the two
        // reporting styles applies. When no lambda parameter type can be derived, the lambda is not
        // necessarily at fault -- here the first argument is not an ARRAY -- so the call is
        // reported
        // by Calcite with its signature, exactly as for a hand-written operator; only the supported
        // form is now generated from the type inference rather than hand-written. When the lambda
        // itself is rejected, the input type strategy reports it, which names the actual cause and
        // renders the same text on the SQL and the Table API surface (see
        // HigherOrderFunctionValidationParityTest).
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(base, x -> x + 1) FROM t",
                "Cannot apply 'ARRAY_TRANSFORM' to arguments of type "
                        + "'ARRAY_TRANSFORM(<INTEGER>, <FUNCTION(ANY) -> ANY>)'. "
                        + "Supported form(s): "
                        + "ARRAY_TRANSFORM(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->ANY)");
        // the Table API reports the same call with the signature as well, not as a lambda error
        assertThatThrownBy(() -> tEnv.from("t").select($("base").arrayTransform(x -> x.plus(1))))
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        e ->
                                assertThat(ExceptionUtils.stringifyException(e))
                                        .contains(
                                                "Invalid input arguments. Expected signatures are:\n"
                                                        + "ARRAY_TRANSFORM(array ARRAY, lambda "
                                                        + "FUNCTION(ARRAY_ELEMENT_TYPE)->ANY)",
                                                "Either the function does not declare a lambda "
                                                        + "argument at position 1, or the lambda "
                                                        + "parameter types cannot be derived from "
                                                        + "the preceding argument types [INT].")
                                        .doesNotContain("does not accept a lambda expression"));
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, base) FROM t",
                "Invalid input arguments. Expected signatures are:",
                "ARRAY_TRANSFORM(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->ANY)");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, (x, y) -> x) FROM t",
                "The lambda expression at position 1 expects 1 parameter(s) but 2 were provided.");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_FILTER(base, x -> x > 1) FROM t",
                "Cannot apply 'ARRAY_FILTER' to arguments of type "
                        + "'ARRAY_FILTER(<INTEGER>, <FUNCTION(ANY) -> BOOLEAN>)'. "
                        + "Supported form(s): "
                        + "ARRAY_FILTER(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->BOOLEAN)");
        // the predicate of ARRAY_FILTER must be BOOLEAN
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_FILTER(a, x -> x + 1) FROM t",
                "Invalid function call:\nARRAY_FILTER(ARRAY<INT>, FUNCTION(1))",
                "ARRAY_FILTER(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->BOOLEAN)",
                "The lambda expression at position 1 must return BOOLEAN, "
                        + "but its body returns INT.");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_REDUCE(a, 0, 5) FROM t",
                "Invalid function call:\nARRAY_REDUCE(ARRAY<INT>, INT NOT NULL, INT NOT NULL)",
                "ARRAY_REDUCE(array ARRAY, initial INIT, "
                        + "lambda FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE)");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_ZIP_WITH(a, base, (x, y) -> x) FROM t",
                "Cannot apply 'ARRAY_ZIP_WITH' to arguments of type "
                        + "'ARRAY_ZIP_WITH(<INTEGER ARRAY>, <INTEGER>, <FUNCTION(ANY, ANY) -> ANY>)'. "
                        + "Supported form(s): ARRAY_ZIP_WITH(array1 ARRAY, array2 ARRAY, "
                        + "lambda FUNCTION(ARRAY1_ELEMENT_TYPE, ARRAY2_ELEMENT_TYPE)->ANY)");
        // a reducer whose body is not assignable to the accumulator type, and that the
        // accumulator cannot be widened to without loss within its type family, is rejected during
        // validation (rather than failing later during code generation), with a hint to cast
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * CAST(0.5 AS DOUBLE)) FROM t",
                "Invalid function call:\nARRAY_REDUCE(ARRAY<INT>, INT NOT NULL, FUNCTION(2))",
                "ARRAY_REDUCE(array ARRAY, initial INIT, "
                        + "lambda FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE)",
                "The reducer of ARRAY_REDUCE must return a type assignable to the accumulator "
                        + "type INT NOT NULL, but its body returns DOUBLE.",
                "Cast the initial value to DOUBLE to accumulate in that type, or cast the "
                        + "reducer's result to INT.");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> CAST(acc AS STRING)) FROM t",
                "Invalid function call:\nARRAY_REDUCE(ARRAY<INT>, INT NOT NULL, FUNCTION(2))",
                "ARRAY_REDUCE(array ARRAY, initial INIT, "
                        + "lambda FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE)",
                "The reducer of ARRAY_REDUCE must return a type assignable to the accumulator "
                        + "type INT NOT NULL, but its body returns STRING NOT NULL.");
    }

    @Test
    void testFilterPredicateAndReducerContractOnTableApi() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = higherOrderFunctionInput(tEnv);

        // The Table API does not go through the planner's operand type checker, so the same rules
        // as SQL must be enforced by the input type strategies. Otherwise a non-boolean filter
        // predicate or an incompatible reducer body would only fail later during code generation.

        // ARRAY_FILTER predicate must be BOOLEAN
        assertThatThrownBy(() -> input.select($("a").arrayFilter(x -> x.plus(1))))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must return BOOLEAN");

        // ARRAY_REDUCE reducer body must be assignable to the accumulator (initial value) type
        assertThatThrownBy(
                        () ->
                                input.select(
                                        $("a").arrayReduce(
                                                        lit(0),
                                                        (acc, x) -> acc.plus(x).times(lit(0.5)))))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must return a type assignable to the accumulator type")
                .hasStackTraceContaining("Cast the initial value to DOUBLE");
    }

    @Test
    void testArrayFilterOverEnclosingLambdaParameter() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                        Row.of((Object) new Integer[][] {{1, 2, 3}, {4}}));
        tEnv.createTemporaryView("t", input);

        // The inner array is an unbound lambda parameter while ARRAY_FILTER is first checked, so
        // its operand check must be deferred to the pass in which the parameter is bound.
        final Table result =
                tEnv.sqlQuery("SELECT ARRAY_TRANSFORM(a, x -> ARRAY_FILTER(x, y -> y > 2)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[][] {{3}, {4}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdaParameterShadowingOnTableApi() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = higherOrderFunctionInput(tEnv);

        // The typed Table API methods generate fresh parameter names, so shadowing is reachable
        // only through explicit names. HigherOrderFunctionsITCase has the SQL counterparts.
        // A parameter shadows the column of the same name (base = 10).
        assertThat(
                        collectSingle(
                                input.select(
                                        call(
                                                "ARRAY_TRANSFORM",
                                                $("a"),
                                                new UnresolvedLambdaExpression(
                                                        Collections.singletonList("base"),
                                                        $("base").plus(1))))))
                .isEqualTo(new Integer[] {2, 3, 4});
        // An inner parameter shadows the enclosing parameter of the same name; resolving the
        // inner x to the outer one would yield {{10, 10, 10}, {20, 20, 20}, {30, 30, 30}}.
        assertThat(
                        collectSingle(
                                input.select(
                                        call(
                                                "ARRAY_TRANSFORM",
                                                $("a"),
                                                new UnresolvedLambdaExpression(
                                                        Collections.singletonList("x"),
                                                        call(
                                                                "ARRAY_TRANSFORM",
                                                                $("a"),
                                                                new UnresolvedLambdaExpression(
                                                                        Collections.singletonList(
                                                                                "x"),
                                                                        $("x").times(10))))))))
                .isEqualTo(new Integer[][] {{10, 20, 30}, {10, 20, 30}, {10, 20, 30}});
    }

    @Test
    void testFieldAccessOnRowLambdaParameter() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "rs",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("n", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "amount", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "nested",
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "v",
                                                                                DataTypes
                                                                                        .INT())))))),
                                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("amount", DataTypes.INT())),
                        Row.of(
                                new Row[] {Row.of(1, "a", Row.of(7)), Row.of(2, "b", Row.of(8))},
                                new Integer[] {10, 20},
                                100));
        tEnv.createTemporaryView("t", input);

        assertThat(collectSingleRow(tEnv, "SELECT ARRAY_TRANSFORM(rs, r -> r.n) FROM t"))
                .isEqualTo(new Integer[] {1, 2});
        assertThat(collectSingleRow(tEnv, "SELECT ARRAY_TRANSFORM(rs, r -> r.nested.v) FROM t"))
                .isEqualTo(new Integer[] {7, 8});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(rs, r -> ARRAY_TRANSFORM(a, x -> x + r.n))"
                                        + " FROM t"))
                .isEqualTo(new Integer[][] {{11, 21}, {12, 22}});
        assertThat(
                        collectSingleRow(
                                tEnv, "SELECT ARRAY_TRANSFORM(rs, r -> r.n) FROM t GROUP BY rs"))
                .isEqualTo(new Integer[] {1, 2});
        // The lambda parameter shadows the table alias of the same name: r.amount is the STRING
        // field of the element, not the INT column of the table. Validating it against the table
        // but evaluating it against the element failed at runtime with a ClassCastException.
        assertThat(
                        collectSingleRow(
                                tEnv, "SELECT ARRAY_TRANSFORM(r.rs, r -> r.amount) FROM t AS r"))
                .isEqualTo(new String[] {"a", "b"});

        assertInvalidCall(tEnv, "SELECT ARRAY_TRANSFORM(a, r -> r.n) FROM t", "Unknown field 'n'");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(rs, r -> SUM(r.n)) FROM t",
                "Aggregate functions over a lambda parameter are not supported",
                "'r' is a lambda parameter");
    }

    @Test
    void testFieldAccessInNestedLambdaBody() throws Exception {
        // A nested lambda, and any lambda of a stand-alone expression (callSql), has its body typed
        // once before the call owning it binds the parameters. A field of such a parameter, or of
        // a value derived from it, must be deferred to the pass after binding. It used to fail with
        // "This is a bug" (a field lookup on the unbound placeholder) or "Incompatible types".
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD(
                                        "orders",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("id", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "items",
                                                                DataTypes.ARRAY(
                                                                        DataTypes.ROW(
                                                                                DataTypes.FIELD(
                                                                                        "price",
                                                                                        DataTypes
                                                                                                .INT()),
                                                                                DataTypes.FIELD(
                                                                                        "qty",
                                                                                        DataTypes
                                                                                                .INT())))))))),
                        Row.of(
                                new Integer[] {1, 2},
                                new Row[] {
                                    Row.of(1, new Row[] {Row.of(10, 2), Row.of(20, 0)}),
                                    Row.of(2, new Row[] {Row.of(5, 1)})
                                }));
        tEnv.createTemporaryView("t", input);

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(orders, o -> ARRAY_TRANSFORM("
                                        + "ARRAY_FILTER(o.items, i -> i.qty > 0), i -> i.price))"
                                        + " FROM t"))
                .isEqualTo(new Integer[][] {{10}, {5}});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(orders, o -> ARRAY_REDUCE("
                                        + "o.items, 0, (acc, i) -> acc + i.price * i.qty + x))) FROM t"))
                .isEqualTo(new Integer[][] {{22, 6}, {24, 7}});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(orders,"
                                        + " o -> o.items[1].price + x)) FROM t"))
                .isEqualTo(new Integer[][] {{11, 6}, {12, 7}});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(a,"
                                        + " y -> IFNULL(ROW(y, x), ROW(0, 0)).EXPR$1)) FROM t"))
                .isEqualTo(new Integer[][] {{1, 1}, {2, 2}});
        assertThat(
                        collectSingle(
                                input.select(
                                        callSql(
                                                "ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(orders,"
                                                        + " o -> o.items[1].price + x))"))))
                .isEqualTo(new Integer[][] {{11, 6}, {12, 7}});
        assertThat(collectSingle(input.select(callSql("ARRAY_TRANSFORM(orders, o -> o.id)"))))
                .isEqualTo(new Integer[] {1, 2});
        assertThat(
                        collectSingle(
                                input.select(
                                        callSql(
                                                "ARRAY_TRANSFORM(a, x -> ARRAY[ROW(x, 1)][1].EXPR$0)"))))
                .isEqualTo(new Integer[] {1, 2});

        // An invalid field is reported once the parameter is bound, as at the top level.
        final String[][] invalidFields = {
            {"ARRAY_TRANSFORM(orders, o -> o.nope)", "Unknown field 'nope'"},
            {"ARRAY_TRANSFORM(orders, o -> o.items[1].nope)", "Unknown field 'nope'"},
            {"ARRAY_TRANSFORM(a, y -> y.f)", "Unknown field 'f'"}
        };
        for (String[] invalidField : invalidFields) {
            assertThatThrownBy(
                            () ->
                                    tEnv.sqlQuery(
                                            "SELECT ARRAY_TRANSFORM(a, x -> "
                                                    + invalidField[0]
                                                    + ") FROM t"))
                    .isInstanceOf(ValidationException.class)
                    .satisfies(
                            e ->
                                    assertThat(ExceptionUtils.stringifyException(e))
                                            .contains(invalidField[1])
                                            .doesNotContain("This is a bug"));
        }

        // Known limitation: a MULTISET constructor does not resolve lambda parameters.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> MULTISET[x]) FROM t",
                "Column 'x' not found in any table");
    }

    @Test
    void testCollectionConstructorsOverLambdaParameter() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda body is type-derived before the enclosing call binds the parameter, so an
        // ARRAY[...]/MAP[...] over the parameter cannot know its component type yet and must defer
        // instead of failing with "Type is not supported: ANY".
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(a, x -> ARRAY[x][1]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[x, base][2]),"
                                + " ARRAY_TRANSFORM(a, x -> CARDINALITY(ARRAY[x])),"
                                + " ARRAY_TRANSFORM(a, x -> MAP['k', x]['k']),"
                                + " ARRAY_TRANSFORM(a, x -> MAP[x, base][1]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[ARRAY[x]][1][1]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(ARRAY[x], y -> y + 1)[1]),"
                                + " ARRAY_REDUCE(a, 0, (acc, x) -> ARRAY[acc, x][2]),"
                                + " ARRAY_TRANSFORM(a, x -> CAST(ROW(x, base) AS ROW<c1 INT, c2 INT>).c1)"
                                + " FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {10, 10, 10});
            assertThat(row.getField(2)).isEqualTo(new Integer[] {1, 1, 1});
            assertThat(row.getField(3)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(row.getField(4)).isEqualTo(new Integer[] {10, null, null});
            assertThat(row.getField(5)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(row.getField(6)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(7)).isEqualTo(3);
            assertThat(row.getField(8)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testMutableLambdaResultsAreNotAliased() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A body result of a mutable type may be backed by an object that the generated code reuses
        // across iterations (e.g. the shared BinaryArrayData behind ARRAY[...]), so it must be
        // copied before it is buffered -- otherwise every entry ends up holding the last value.
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(a, x -> ARRAY[x, base]),"
                                + " ARRAY_TRANSFORM(a, x -> ROW(x, base)),"
                                + " ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y])"
                                + " FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 10}, {2, 10}, {3, 10}});
            assertThat(row.getField(1))
                    .isEqualTo(new Row[] {Row.of(1, 10), Row.of(2, 10), Row.of(3, 10)});
            assertThat(row.getField(2)).isEqualTo(new Integer[][] {{1, 1}, {2, 2}, {3, 3}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdasThatDifferOnlyInTheirCapturesAreNotMerged() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // Calcite compares two lambdas by their parameter indices and types only -- a parameter
        // name is not part of RexLambdaRef#equals -- so RexProgramBuilder merges lambdas that agree
        // on those. A lifted capture parameter is therefore numbered apart from a lambda's own
        // parameters: without that, the two-parameter lambda below and the one-parameter lambda
        // that captures `base` would both read (#0, #1) and be merged into one node, after which
        // the number of trailing capture operands no longer matches the call that carries it.
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[x, base]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[x, x]),"
                                + " ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y + base])"
                                + " FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 1}, {2, 2}, {3, 3}});
            assertThat(row.getField(1)).isEqualTo(new Integer[][] {{1, 10}, {2, 10}, {3, 10}});
            assertThat(row.getField(2)).isEqualTo(new Integer[][] {{1, 1}, {2, 2}, {3, 3}});
            assertThat(row.getField(3)).isEqualTo(new Integer[][] {{1, 11}, {2, 12}, {3, 13}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdaOutsideOfHigherOrderFunctionIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = higherOrderFunctionInput(tEnv);
        tEnv.createTemporaryView("t", input);

        // SQL rejects a lambda outside of a function call already at the parser
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT x -> x + 1 FROM t"))
                .hasMessageContaining("SQL parse failed. Encountered \"->\"");

        // a lambda passed to a function that does not declare a lambda argument parses, so the
        // validator rejects it
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT ARRAY_CONTAINS(a, x -> x) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Function 'ARRAY_CONTAINS' does not accept a lambda expression at any "
                                + "position.");

        // the Table API has no such syntax restriction, so the resolver rejects it
        assertThatThrownBy(
                        () ->
                                input.select(
                                        new UnresolvedLambdaExpression(
                                                Collections.singletonList("x"),
                                                $("x").plus(lit(1)))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unexpected lambda expression: x -> plus(x, 1). A lambda expression is "
                                + "only supported as an argument of a function that declares a "
                                + "lambda argument at this position.");

        // a lambda passed to a function that does not declare a lambda argument
        assertThatThrownBy(
                        () ->
                                input.select(
                                        $("base")
                                                .plus(
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x")))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Function 'plus' does not accept a lambda expression at position 1.");
    }

    @Test
    void testFunctionTypeRejectedAsColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        // A FUNCTION type is a lambda helper type and must not be usable as a materialized column.
        assertThatThrownBy(
                        () ->
                                tEnv.fromValues(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "f", DataTypes.FUNCTION(1))),
                                                Row.of((Object) null))
                                        .execute()
                                        .collect())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("FUNCTION data type is a helper type");
    }

    @Test
    void testFunctionTypeRejectedAsSchemaColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A schema is what a catalog persists, so the type has to be rejected while the schema is
        // resolved -- otherwise a FUNCTION column enters the catalog and only fails much later,
        // when the table is read or written.
        assertThatThrownBy(
                        () ->
                                tEnv.createTable(
                                        "t_function_column",
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column("f", DataTypes.FUNCTION(1))
                                                                .build())
                                                .build()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for column 'f'. The FUNCTION data type "
                                + "is a helper type for lambda arguments of higher-order "
                                + "functions.");

        // nested in a constructed type as well
        assertThatThrownBy(
                        () ->
                                tEnv.createTable(
                                        "t_nested_function_column",
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column(
                                                                        "f",
                                                                        DataTypes.ARRAY(
                                                                                DataTypes.ROW(
                                                                                        DataTypes
                                                                                                .FIELD(
                                                                                                        "g",
                                                                                                        DataTypes
                                                                                                                .FUNCTION(
                                                                                                                        2)))))
                                                                .build())
                                                .build()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'ARRAY<ROW<`g` FUNCTION(2)>>' for column 'f'.");

        // DDL has no syntax for the type at all
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE t_ddl (f FUNCTION(1))"
                                                + " WITH ('connector' = 'datagen')"))
                .hasMessageContaining("Incorrect syntax near the keyword 'FUNCTION'");
    }

    @Test
    void testFunctionTypeRejectedAsMetadataColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A metadata column is a separate column kind in the schema resolver, so the
        // physical-column
        // case above does not cover it. It reaches the same rejection, but only because the
        // resolver
        // checks every column kind; pin it so that a future resolver refactor cannot let a FUNCTION
        // metadata column through.
        assertThatThrownBy(
                        () ->
                                tEnv.createTable(
                                        "t_function_metadata_column",
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .columnByMetadata(
                                                                        "f", DataTypes.FUNCTION(1))
                                                                .build())
                                                .build()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for column 'f'. The FUNCTION data type "
                                + "is a helper type for lambda arguments of higher-order "
                                + "functions.");
    }

    @Test
    void testFunctionTypeRejectedAsComputedColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A computed column derives its type from an expression rather than from a declaration, so
        // it is the one column kind where a FUNCTION type could appear without anyone writing it
        // down. This pins that the computed-column path is not a back door: the grammar only
        // admits a lambda in the argument position of a call, so a bare lambda is rejected by the
        // parser long before it could become a column type.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE t_function_computed_column ("
                                                + " i INT,"
                                                + " f AS (x -> x + 1)"
                                                + ") WITH ('connector' = 'datagen')"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageContaining("Encountered \"->\"");
    }

    @Test
    void testFunctionTypeRejectedOnCatalogRoundTrip() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Catalog catalog =
                tEnv.getCatalog(tEnv.getCurrentCatalog()).orElseThrow(IllegalStateException::new);
        final ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "t_planted");

        // A catalog that already holds a FUNCTION column -- planted here past the catalog manager,
        // as an external catalog or an older Flink version could -- must not hand it out either:
        // the type is rejected again when the stored schema is resolved on read.
        catalog.createTable(
                path,
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("f", DataTypes.FUNCTION(1)).build())
                        .options(Collections.singletonMap("connector", "datagen"))
                        .build(),
                false);

        assertThatThrownBy(() -> tEnv.from("t_planted"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid data type 'FUNCTION(1)' for column 'f'.");

        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM t_planted"))
                .hasMessageContaining("Invalid data type 'FUNCTION(1)' for column 'f'.");

        // and neither can it be copied into a new table
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE t_ctas WITH ('connector' = 'blackhole')"
                                                + " AS SELECT * FROM t_planted"))
                .hasMessageContaining("Invalid data type 'FUNCTION(1)' for column 'f'.");
    }

    @Test
    void testFunctionTypeRejectedAsReturnType() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("returns_function", FunctionReturningFunction.class);
        tEnv.createTemporarySystemFunction(
                "returns_nested_function", FunctionReturningNestedFunction.class);

        // A lambda is a compile-time construct, so a function cannot hand one back as its result,
        // neither directly nor nested in a constructed type. Any function can declare such a type
        // through its type inference, whether or not it takes a lambda argument itself.
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
    void testFunctionTypeRejectedAsAccumulatorType() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "function_acc_agg", FunctionAccumulatorAggregateFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1));

        // A lambda object is valid only for the call it is passed to and has no serialized form,
        // so it can never be kept between invocations. The type is rejected during validation
        // rather than when the state serializer is built.
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
    void testFunctionTypeRejectedAsNamedStateEntry() {
        // process table functions are streaming-only
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction(
                "function_state_ptf", FunctionStateProcessTableFunction.class);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.INT())), Row.of(1)));

        // Reflective extraction already rejects a non-composite state type, so this pins the guard
        // for a function that declares its state types programmatically.
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
    void testDuplicateLambdaParameterNamesAreRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda binds its parameters by name, so two parameters of the same name would leave
        // the first one unreferenceable. Rejected on both surfaces rather than silently collapsed.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_ZIP_WITH(a, a, (x, x) -> x) FROM t",
                "Duplicate lambda parameter name 'x'. The parameters of a lambda expression "
                        + "must have unique names.");
        // a nested lambda may shadow an enclosing parameter, but not repeat its own
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_ZIP_WITH(a, a, (x, x) -> x)[1]) FROM t",
                "Duplicate lambda parameter name 'x'.");

        assertThatThrownBy(
                        () -> new UnresolvedLambdaExpression(Arrays.asList("x", "y", "x"), $("x")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The parameters of a lambda expression must have unique names. "
                                + "Found duplicates: [x]");
    }

    @Test
    void testLambdaParameterCountMismatchIsRejectedOnTableApi() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = higherOrderFunctionInput(tEnv);

        // The typed lambda methods of the Table API fix the number of parameters at compile time,
        // but a lambda built from UnresolvedLambdaExpression can declare any number of them. See
        // testInvalidArgumentsAreRejectedWithTheFunctionSignature for the SQL counterpart.
        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                "ARRAY_TRANSFORM",
                                                $("a"),
                                                new UnresolvedLambdaExpression(
                                                        Arrays.asList("x", "y"),
                                                        $("x").plus($("y"))))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The lambda expression expects 1 parameter(s) but 2 were provided.");
        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                "ARRAY_REDUCE",
                                                $("a"),
                                                lit(0),
                                                new UnresolvedLambdaExpression(
                                                        Collections.singletonList("acc"),
                                                        $("acc")))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The lambda expression expects 2 parameter(s) but 1 were provided.");
    }

    @Test
    void testFunctionReferenceAsLambdaIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // SQL has no function-valued expressions, so a bare function name at a lambda position is
        // an ordinary identifier and is reported as such. The supported spelling is an explicit
        // lambda, 'x -> my_plus_one(x)'.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, my_plus_one) FROM t",
                "Column 'my_plus_one' not found in any table");
    }

    @Test
    void testTableFunctionInLambdaBodyIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_table_func", SplitTableFunction.class);
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("s", DataTypes.STRING())),
                        Row.of(new Integer[] {1, 2, 3}, "a b"));
        tEnv.createTemporaryView("t", input);

        // A table function produces rows rather than a value, so it is rejected in a lambda body
        // just as in any other scalar position. Without the check it would reach code generation
        // and fail there with an unset collector.
        final String expectedMessage =
                "Table functions are not supported in the body of a lambda expression";
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(f0, x -> my_table_func(s)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        assertThatThrownBy(
                        () ->
                                input.select(
                                        $("f0").arrayTransform(x -> call("my_table_func", $("s")))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void testAsynchronousScalarFunctionInLambdaBodyIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_async_plus_one", AsyncPlusOneScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // An asynchronous function completes its future only after the per-element expression
        // that would consume the result has been evaluated. Without the check the call reaches
        // code generation, where the rule that extracts asynchronous calls out of a projection
        // does not descend into lambda bodies, and fails to compile.
        final String expectedMessage =
                "Asynchronous scalar functions are not supported in the body of a lambda "
                        + "expression";
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(a, x -> my_async_plus_one(x)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        // also when the body does not reference the lambda parameter at all
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(a, x -> my_async_plus_one(1)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        // the Table API validates the body during expression resolution, so it has its own check
        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                $("a").arrayTransform(
                                                                x ->
                                                                        call(
                                                                                AsyncPlusOneScalarFunction
                                                                                        .class,
                                                                                x))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void testPythonScalarFunctionInLambdaBodyIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_python_plus", new PythonScalarFunction("my_python_plus"));
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // Python calls are split out of a projection into a separate Python operator, which cannot
        // happen per element of a lambda body. Without the check the body is planned into a Python
        // calc that cannot evaluate the enclosing higher-order function.
        final String expectedMessage =
                "Python scalar functions are not supported in the body of a lambda expression";
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(a, x -> my_python_plus(x, 1)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(a, x -> my_python_plus(1, 1)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                $("a").arrayTransform(
                                                                x ->
                                                                        call(
                                                                                "my_python_plus",
                                                                                x,
                                                                                lit(1)))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void testAggregateOverLambdaParameterIsRejected() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda parameter exists per element, so there is no group to aggregate it over --
        // ARRAY_REDUCE is the fold for that. Rejected during validation, because deriving the type
        // of the aggregate over the not-yet-bound parameter would otherwise fail with an internal
        // error. An aggregate over a column of the enclosing query is a different matter and is
        // supported; see testAggregateOverOuterColumnInLambdaBody.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> SUM(x)) FROM t",
                "Aggregate functions over a lambda parameter are not supported in the body of a "
                        + "lambda expression. 'x' is a lambda parameter");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(x + base)) FROM t GROUP BY a",
                "Aggregate functions over a lambda parameter are not supported");
        // the parameter of an *enclosing* lambda counts as well
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, e -> ARRAY_REDUCE(a, 0, (acc, x) -> acc + SUM(e)))"
                        + " FROM t GROUP BY a",
                "Aggregate functions over a lambda parameter are not supported in the body of a "
                        + "lambda expression. 'e' is a lambda parameter");

        // but the parameter of a lambda nested *inside* the aggregate is bound within it, so this
        // aggregates over the enclosing query only and is accepted
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(ARRAY_REDUCE(a, 0,"
                                        + " (acc, y) -> acc + y))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {7, 8, 9});
    }

    @Test
    void testAggregateOverOuterColumnInLambdaBodyTableApi() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = groupedHigherOrderFunctionInput(tEnv);

        // The Table API reaches the same result by a different route: the aggregate is hoisted out
        // of the lambda body into the enclosing aggregation before resolution, so the body is left
        // capturing an ordinary column of the aggregation's output.
        assertThat(
                        collectSingle(
                                input.groupBy($("a"))
                                        .select(
                                                $("a").arrayTransform(
                                                                x -> x.plus($("base").sum())))))
                .isEqualTo(new Integer[] {31, 32, 33});
        // the group key order must not matter here either
        assertThat(
                        collectSingle(
                                input.groupBy($("g"), $("a"))
                                        .select(
                                                $("a").arrayTransform(
                                                                x -> x.plus($("base").sum())))))
                .isEqualTo(new Integer[] {31, 32, 33});
        // an aggregate and an ordinary column capture in the same body
        assertThat(
                        collectSingle(
                                input.groupBy($("a"), $("g"))
                                        .select(
                                                $("a").arrayTransform(
                                                                x ->
                                                                        x.plus($("base").sum())
                                                                                .plus(
                                                                                        $("g").charLength())))))
                .isEqualTo(new Integer[] {34, 35, 36});
        // the same aggregate used inside and outside the lambda is extracted once
        assertThat(
                        collectSingle(
                                input.groupBy($("a"))
                                        .select(
                                                $("a").arrayTransform(x -> x.plus($("base").sum())),
                                                $("base").sum())))
                .isEqualTo(new Integer[] {31, 32, 33});
        // inside a nested lambda, which also captures the enclosing lambda's parameter
        assertThat(
                        collectSingle(
                                input.groupBy($("a"))
                                        .select(
                                                $("a").arrayTransform(
                                                                e ->
                                                                        $("a").arrayReduce(
                                                                                        lit(0),
                                                                                        (acc, x) ->
                                                                                                acc.plus(
                                                                                                                x)
                                                                                                        .plus(
                                                                                                                e)
                                                                                                        .plus(
                                                                                                                $("base")
                                                                                                                        .sum()))))))
                .isEqualTo(new Integer[] {99, 102, 105});
    }

    @Test
    void testAggregateInLambdaBodyWithoutGroupingIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = groupedHigherOrderFunctionInput(tEnv);
        tEnv.createTemporaryView("t", input);

        // Hoisting the aggregate out of the lambda body does not exempt the column the body is
        // applied to from the grouping rules: without a GROUP BY the aggregate is global, so `a` is
        // not available to the projection. A lambda changes nothing here -- the same query without
        // one is rejected identically, on both surfaces.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t",
                "Expression 'a' is not being grouped");
        assertInvalidCall(
                tEnv, "SELECT a, SUM(base) FROM t", "Expression 'a' is not being grouped");

        // The Table API reports its usual error for a non-grouped column alongside a global
        // aggregate. The wording is not pinned -- only that the lambda form fails the same way as
        // the plain one, which is what this test is about.
        final String withLambda =
                messageOf(() -> input.select($("a").arrayTransform(x -> x.plus($("base").sum()))));
        final String withoutLambda = messageOf(() -> input.select($("a"), $("base").sum()));
        assertThat(withLambda).isEqualTo(withoutLambda);
    }

    @Test
    void testAggregateOverLambdaParameterIsRejectedInTableApi() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input = groupedHigherOrderFunctionInput(tEnv);

        // The message does not name the parameter: a Table API lambda parameter carries a generated
        // name that would mean nothing to the reader.
        assertThatThrownBy(() -> input.select($("a").arrayTransform(x -> x.sum())).execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Aggregate functions over a lambda parameter are not supported in the body "
                                + "of a lambda expression");
        assertThatThrownBy(
                        () ->
                                input.groupBy($("a"))
                                        .select(
                                                $("a").arrayTransform(
                                                                x ->
                                                                        x.plus(
                                                                                x.plus($("base"))
                                                                                        .sum())))
                                        .execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Aggregate functions over a lambda parameter are not supported");

        // but a parameter of a lambda nested inside the aggregate is bound within it
        assertThat(
                        collectSingle(
                                input.groupBy($("a"))
                                        .select(
                                                $("a").arrayTransform(
                                                                x ->
                                                                        x.plus(
                                                                                $("a").arrayReduce(
                                                                                                lit(
                                                                                                        0),
                                                                                                (acc,
                                                                                                        y) ->
                                                                                                        acc
                                                                                                                .plus(
                                                                                                                        y))
                                                                                        .sum())))))
                .isEqualTo(new Integer[] {13, 14, 15});
    }

    @Test
    void testAggregateOverOuterColumnInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // An aggregate over a column of the enclosing query is evaluated once per group; the body
        // sees its result, exactly as if it had been computed in a sub-query. The conversion turns
        // it into a reference into the Aggregate output, which capture lifting then hoists out of
        // the lambda (see SqlToRelConverter#convertLambda and HigherOrderFunctionUtil).
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {31, 32, 33});
        // grouping by another column too, in both orders -- the group key order must not matter
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t"
                                        + " GROUP BY a, g"))
                .isEqualTo(new Integer[] {31, 32, 33});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t"
                                        + " GROUP BY g, a"))
                .isEqualTo(new Integer[] {31, 32, 33});
        // COUNT(*), which has no argument to resolve at all
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + CAST(COUNT(*) AS INT)) FROM t"
                                        + " GROUP BY a"))
                .isEqualTo(new Integer[] {3, 4, 5});
        // an aggregate and an ordinary column capture in the same body
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) + CHAR_LENGTH(g))"
                                        + " FROM t GROUP BY a, g"))
                .isEqualTo(new Integer[] {34, 35, 36});
        // the other array higher-order functions host it just as well
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_FILTER(a, x -> x > SUM(base) - 30) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {1, 2, 3});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x + SUM(base)) FROM t"
                                        + " GROUP BY a"))
                .isEqualTo(96);
    }

    @Test
    void testAggregateOverOuterColumnInNestedLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[][] {{1, 2}, {3}}, 10)));

        // the aggregate is threaded out of the inner lambda and then out of the outer one
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, b -> ARRAY_TRANSFORM(b,"
                                        + " x -> x + SUM(base))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[][] {{11, 12}, {13}});
    }

    @Test
    void testColumnCaptureInAggregateArgument() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW w AS SELECT * FROM (VALUES ('abc', 2), ('abc', 3)) AS w(g, k)");
        final String weighted = "ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * base)";

        // An aggregate argument is converted against the row before aggregation, so a column the
        // lambda captures there is a plain input reference of that row, not a correlation (see
        // SqlToRelConverter#isSameQueryLambdaCapture). 6 * 10 + 6 * 20 = 180.
        assertThat(collectSingleRow(tEnv, "SELECT SUM(" + weighted + ") FROM t")).isEqualTo(180);
        assertThat(collectSingleRow(tEnv, "SELECT SUM(" + weighted + ") FROM t GROUP BY g"))
                .isEqualTo(180);
        assertThat(
                        collectSingleRow(
                                tEnv, "SELECT MAX(ARRAY_TRANSFORM(a, x -> x * base)[1]) FROM t"))
                .isEqualTo(20);
        assertThat(collectSingleRow(tEnv, "SELECT COUNT(DISTINCT " + weighted + ") FROM t"))
                .isEqualTo(2L);
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT SUM(base) FILTER (WHERE " + weighted + " > 100) FROM t"))
                .isEqualTo(20);
        // in HAVING and ORDER BY, whose aggregates are converted the same way
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT g FROM t GROUP BY g HAVING SUM(" + weighted + ") = 180"))
                .containsExactly("abc");
        assertThat(collectColumn(tEnv, "SELECT g FROM t GROUP BY g ORDER BY SUM(" + weighted + ")"))
                .containsExactly("abc");
        // columns of both sides of a join: (6 * k + 3 * 10) + (6 * k + 3 * 20) per k
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT SUM(ARRAY_REDUCE(t.a, 0, (acc, x) -> acc + x * w.k"
                                        + " + t.base)) FROM t JOIN w ON t.g = w.g GROUP BY w.k"))
                .containsExactlyInAnyOrder(114, 126);
        // inside the body of an enclosing lambda, which sees the aggregate's result
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(ARRAY_REDUCE(a, 0,"
                                        + " (acc, y) -> acc + y * base))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {181, 182, 183});
        // in an aggregating sub-query of a lambda body, capturing the sub-query's own column
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(ARRAY[1, 2], x -> x + (SELECT"
                                        + " MAX(ARRAY_TRANSFORM(ARRAY[1], y -> y + u.base)[1])"
                                        + " FROM t AS u))"))
                .isEqualTo(new Integer[] {22, 23});

        // A column of an *outer* query still crosses a query boundary and stays correlated, also
        // next to a capture of the sub-query's own column. Per outer row: 12 * t.base = 120 and
        // 240; (6 * 10 + 3 * t.base) + (6 * 20 + 3 * t.base) = 240 and 300.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT (SELECT SUM(ARRAY_REDUCE(u.a, 0, (acc, x) -> acc + x"
                                        + " * t.base)) FROM t AS u) FROM t"))
                .containsExactlyInAnyOrder(120, 240);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT (SELECT SUM(ARRAY_REDUCE(u.a, 0, (acc, x) -> acc + x"
                                        + " * u.base + t.base)) FROM t AS u) FROM t"))
                .containsExactlyInAnyOrder(240, 300);
    }

    @Test
    void testColumnCaptureInGroupBy() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));
        final String weighted = "ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * base)";

        // A lambda in GROUP BY sits under that clause's scope, but its captured column belongs to
        // the same query. The group keys are 60 and 120.
        assertThat(collectColumn(tEnv, "SELECT SUM(base) FROM t GROUP BY " + weighted))
                .containsExactlyInAnyOrder(10, 20);
        assertThat(collectColumn(tEnv, "SELECT " + weighted + " FROM t GROUP BY " + weighted))
                .containsExactlyInAnyOrder(60, 120);
    }

    @Test
    void testLambdaInOrderBy() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW q AS SELECT * FROM (VALUES (1, ARRAY[30], 'b'),"
                        + " (2, ARRAY[10], 'a'), (3, ARRAY[20], 'c')) AS q(v, arr, s)");
        final String byBase = "ARRAY_TRANSFORM(a, x -> x * base)[1]";

        // A body in ORDER BY resolves names as in any other clause: its parameters, then the
        // columns of the FROM clause.
        assertThat(collectColumn(tEnv, "SELECT base FROM t ORDER BY " + byBase + " DESC"))
                .containsExactly(20, 10);
        assertThat(collectColumn(tEnv, "SELECT base FROM t ORDER BY " + byBase + " DESC LIMIT 1"))
                .containsExactly(20);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT base, " + byBase + " FROM t ORDER BY " + byBase + " DESC"))
                .containsExactly(20, 10);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT DISTINCT base, "
                                        + byBase
                                        + " FROM t ORDER BY "
                                        + byBase
                                        + " DESC"))
                .containsExactly(20, 10);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT * FROM (SELECT base, a FROM t) ORDER BY "
                                        + byBase
                                        + " DESC"))
                .containsExactly(20, 10);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT base FROM t ORDER BY ARRAY_TRANSFORM(ARRAY[a],"
                                        + " e -> ARRAY_TRANSFORM(e, x -> x * base))[1][1] DESC"))
                .containsExactly(20, 10);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT s FROM q GROUP BY s"
                                        + " ORDER BY ARRAY_TRANSFORM(ARRAY['x'], y -> y || s)[1] DESC"))
                .containsExactly("c", "b", "a");
        // A parameter shadows a select-list alias of its name: the order follows the elements
        // (60, 20, 40), not v.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT v AS x FROM q ORDER BY ARRAY_TRANSFORM(arr, x -> x * 2)[1]"))
                .containsExactly(2, 3, 1);
        // An alias that stands for the column of its name reads the same either way, also when
        // repeated; outside a body, ORDER BY still sees the aliases.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT base, t.base AS base FROM t ORDER BY " + byBase + " DESC"))
                .containsExactly(20, 10);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT base AS y FROM t"
                                        + " ORDER BY ARRAY_REDUCE(a, y, (acc, x) -> acc + x) DESC"))
                .containsExactly(20, 10);

        // Any other alias of a body's name is rejected rather than read as either the alias or
        // the column: here the column base (10, 20) and the alias (20, 40) would sort alike, but
        // only the column is in scope.
        assertInvalidCall(
                tEnv,
                "SELECT base * 2 AS base FROM t ORDER BY " + byBase,
                "A lambda body in ORDER BY cannot use the select-list alias 'base'");
        assertInvalidCall(
                tEnv,
                "SELECT base * 2 AS base FROM t ORDER BY ARRAY_TRANSFORM(ARRAY[a],"
                        + " e -> ARRAY_TRANSFORM(e, x -> x * base))[1][1]",
                "A lambda body in ORDER BY cannot use the select-list alias 'base'");
        assertInvalidCall(
                tEnv,
                "SELECT base AS y FROM t ORDER BY ARRAY_TRANSFORM(a, x -> x * y)[1]",
                "A lambda body in ORDER BY cannot use the select-list alias 'y'");
        assertInvalidCall(
                tEnv,
                "SELECT g, SUM(base) AS sb FROM t GROUP BY g"
                        + " ORDER BY ARRAY_TRANSFORM(ARRAY[1], x -> x * sb)[1]",
                "A lambda body in ORDER BY cannot use the select-list alias 'sb'");
        // the column qualified with its table alias is what the message suggests
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT base * 2 AS base FROM t"
                                        + " ORDER BY ARRAY_TRANSFORM(a, x -> x * t.base)[1] DESC"))
                .containsExactly(40, 20);
    }

    @Test
    void testCommonJoinColumnInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW t1 AS SELECT * FROM (VALUES (ARRAY[1, 2], 10),"
                        + " (ARRAY[3], 30)) AS t1(a, base)");
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW t2 AS SELECT * FROM (VALUES (10, 'x'), (20, 'y'))"
                        + " AS t2(base, s)");

        // Known limitation: the select list accepts a column merged by USING or NATURAL
        // unqualified, but a lambda body does not, because the select-list expansion that turns
        // it into COALESCE(t1.base, t2.base) leaves a lambda alone.
        assertThat(collectColumn(tEnv, "SELECT base * 2 FROM t1 JOIN t2 USING (base)"))
                .containsExactly(20);
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x * base) FROM t1 JOIN t2 USING (base)",
                "Column 'base' is ambiguous");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x * base) FROM t1 NATURAL JOIN t2",
                "Column 'base' is ambiguous");

        // the workarounds that the documentation gives
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x * t1.base)[2]"
                                        + " FROM t1 JOIN t2 USING (base)"))
                .containsExactly(20);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x * COALESCE(t1.base, t2.base))[1]"
                                        + " FROM t1 FULL JOIN t2 USING (base)"))
                .containsExactlyInAnyOrder(10, 20, 30);
        // a parameter named like the merged column shadows it, as it shadows any column
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, base -> base * 2)[2]"
                                        + " FROM t1 JOIN t2 USING (base)"))
                .containsExactly(4);
    }

    @Test
    void testCaptureOfGroupKeyInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // A captured group key must be resolved against the Aggregate output, not against the
        // pre-aggregation input: the two agree only when the GROUP BY order happens to match the
        // input order, so both orders are pinned here.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + CHAR_LENGTH(g)) FROM t"
                                        + " GROUP BY a, g"))
                .isEqualTo(new Integer[] {4, 5, 6});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + CHAR_LENGTH(g)) FROM t"
                                        + " GROUP BY g, a"))
                .isEqualTo(new Integer[] {4, 5, 6});
        // a nested lambda that captures both a group key and the enclosing lambda's parameter
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, e -> ARRAY_REDUCE(a, 0,"
                                        + " (acc, x) -> acc + x + e + CHAR_LENGTH(g))) FROM t"
                                        + " GROUP BY g, a"))
                .isEqualTo(new Integer[] {18, 21, 24});
    }

    @Test
    void testOverWindowInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // An OVER window yields one value per row, so it is lifted out of the lambda and evaluated
        // in a projection below it -- the same plan as writing the window in a sub-query. Both
        // input rows see the same unbounded window, so both produce the same array.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) OVER ()) FROM t"))
                .containsExactly(new Integer[] {31, 32, 33}, new Integer[] {31, 32, 33});
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) OVER"
                                        + " (PARTITION BY g)) FROM t"))
                .containsExactly(new Integer[] {31, 32, 33}, new Integer[] {31, 32, 33});

        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) OVER (PARTITION BY x)) FROM t",
                "OVER windows over a lambda parameter are not supported in the body of a lambda "
                        + "expression. 'x' is a lambda parameter");
    }

    /**
     * The Table API counterpart of {@link #testOverWindowInLambdaBody()}. It runs in streaming mode
     * -- unlike every other test here -- because a Table API over window must be ordered by a time
     * attribute, which a batch table does not have: the batch {@code OverAggregateITCase} for the
     * Table API is {@code @Disabled} for exactly that reason ("OverWindow on Batch should support
     * to order by non-time attribute"). That restriction is pre-existing and unrelated to lambdas,
     * so the only way to cover an over window in a lambda body over this surface is a rowtime
     * table.
     */
    @Test
    void testOverWindowInLambdaBodyTableApi() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(
                                        new Integer[] {1, 2, 3},
                                        10,
                                        LocalDateTime.parse("2024-01-01T00:00:00")),
                                Row.of(
                                        new Integer[] {1, 2, 3},
                                        20,
                                        LocalDateTime.parse("2024-01-01T00:00:01"))));
        tEnv.executeSql(
                "CREATE TABLE t (\n"
                        + "  a ARRAY<INT>,\n"
                        + "  base INT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',\n"
                        + "  'data-id' = '"
                        + dataId
                        + "'\n"
                        + ")");

        // the window is lifted out of the lambda and evaluated below it, exactly as for the SQL
        // form. Ordering by the rowtime with the default unbounded-preceding range makes the lifted
        // value a running sum, so the two rows see different values (10, then 10 + 20) -- a
        // constant-folded or over-hoisted window could not produce both.
        final Expression windowedSum = $("base").sum().over($("w"));
        assertThat(
                        collectColumn(
                                tEnv.from("t")
                                        .window(Over.orderBy($("ts")).as("w"))
                                        .select($("a").arrayTransform(x -> x.plus(windowedSum)))))
                .containsExactlyInAnyOrder(new Integer[] {11, 12, 13}, new Integer[] {31, 32, 33});
    }

    @Test
    void testSubQueryInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // a scalar sub-query is a value, so the body may use it -- plain or correlated
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(base) FROM t))"
                                        + " FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(base) FROM t t2"
                                        + " WHERE t2.g = t1.g)) FROM t t1 GROUP BY a, g"))
                .isEqualTo(new Integer[] {21, 22, 23});

        // but not one that correlates on a lambda parameter
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(base) FROM t t2"
                        + " WHERE t2.base = x)) FROM t",
                "Subqueries over a lambda parameter are not supported in the body of a lambda "
                        + "expression. 'x' is a lambda parameter");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x IN (SELECT base FROM t)) FROM t",
                "Subqueries over a lambda parameter are not supported");

        // an unqualified name is not resolved against the sub-query, so the parameter's name wins
        // over a column of the sub-query; the message suggests qualifying the column or renaming
        // the parameter
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(x) FROM (VALUES (10), (20))"
                        + " AS q(x))) FROM t",
                "If the subquery is not meant to use the lambda parameter, qualify its own column"
                        + " with its table alias (for example 'q.x') or rename the lambda"
                        + " parameter.");
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, z -> z + (SELECT MAX(x) FROM"
                                        + " (VALUES (10), (20)) AS q(x))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(q.x) FROM"
                                        + " (VALUES (10), (20)) AS q(x))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});

        // a qualified name resolves against a table alias of the sub-query first, also when the
        // alias, a table or a WITH item has the parameter's name: the x.v outside the sub-query
        // is the field of the element (1), the x.v inside it the column of the alias (20)
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(CAST(ARRAY[ROW(1)] AS ARRAY<ROW<v INT>>),"
                                        + " x -> x.v + (SELECT MAX(x.v) FROM (VALUES (10), (20))"
                                        + " AS x(v)))"))
                .isEqualTo(new Integer[] {21});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, t -> t + (SELECT MAX(t.base) FROM t))"
                                        + " FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(x.base) FROM t AS x"
                                        + " JOIN t AS y ON x.base = y.base)) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (WITH x AS (SELECT base FROM t)"
                                        + " SELECT MAX(x.base) FROM x)) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});

        // but an alias declared in a derived table is not visible outside it, so there x.v is
        // the parameter's field
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(CAST(ARRAY[ROW(1)] AS ARRAY<ROW<v INT>>), x -> x.v +"
                        + " (SELECT MAX(q.v) + x.v FROM (SELECT v FROM (VALUES (10)) AS x(v))"
                        + " AS q))",
                "Subqueries over a lambda parameter are not supported in the body of a lambda "
                        + "expression. 'x' is a lambda parameter");

        // a lambda nested in such a sub-query sees the alias too, and as a column of the
        // sub-query it is subject to its GROUP BY
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT ARRAY_TRANSFORM("
                                        + "ARRAY[1], y -> y + x.base)[1] FROM t AS x WHERE"
                                        + " x.base = 20)) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {22, 23, 24});
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT ARRAY_TRANSFORM(ARRAY[1], y -> y +"
                        + " x.base)[1] FROM t AS x GROUP BY x.g)) FROM t GROUP BY a",
                "Expression 'x.base' is not being grouped");
    }

    @Test
    void testOuterColumnCaptureFromLambdaInScalarSubquery() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A lambda body inside a scalar sub-query captures a column of the *outer* query. This
        // crosses a query boundary, so the reference keeps Calcite's correlated-reference path (a
        // RexCorrelVariable field access placed *inside* the lambda) rather than being flattened
        // into a direct input reference of the sub-query's row (see
        // SqlToRelConverter#isSameQueryLambdaCapture). Decorrelation descends into the lambda body,
        // rewrites the correlated reference to an input reference of the decorrelated row, and
        // capture lifting hoists it out of the lambda, so the query executes. x = 1, t.a = 2 -> 3.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x + t.a)[1])"
                                        + " FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(3);

        // Nested lambda inside the correlated scalar sub-query: the outer correlation is captured
        // by the inner lambda body. x = 1, y = 1, t.a = 2 -> 1 + 1 + 2 = 4.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1],"
                                        + " x -> ARRAY_TRANSFORM(ARRAY[1], y -> y + x + t.a)[1])[1])"
                                        + " FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(4);

        // A lambda body that mixes a same-query capture (the sub-query's own column s.b, lifted as
        // an ordinary capture) with an outer correlated value (t.a, decorrelated). x = 1, s.b = 10,
        // t.a = 2 -> 1 + 10 + 2 = 13.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x + s.b + t.a)[1]"
                                        + " FROM (VALUES (10)) AS s(b))"
                                        + " FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(13);

        // Two distinct correlated outer fields, both discovered and rewritten inside the lambda.
        // x = 1, t.a = 2, t.b = 5 -> 1 + 2 + 5 = 8.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x + t.a + t.b)[1])"
                                        + " FROM (VALUES (2, 5)) AS t(a, b)"))
                .isEqualTo(8);

        // Control: a same-query outer-column capture (no query boundary crossed) is flattened to a
        // plain input reference, hoisted out of the lambda by capture lifting, and executes -- it
        // is
        // never turned into a correlation, so it does not exercise the decorrelation path above.
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));
        assertThat(collectSingleRow(tEnv, "SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM t"))
                .isEqualTo(new Integer[] {11, 12, 13});

        // Control: an equivalent correlated scalar sub-query *without* a lambda, guarding that the
        // decorrelator's behaviour is unchanged for the non-lambda case. t.a = 2 -> 3.
        assertThat(collectSingleRow(tEnv, "SELECT (SELECT t.a + 1) FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(3);
    }

    @Test
    void testLambdaCaptureInJoinCondition() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW l AS SELECT * FROM"
                        + " (VALUES (1, ARRAY[1, 2, 3]), (2, ARRAY[4, 5])) AS l(k, arr)");
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW r AS SELECT * FROM"
                        + " (VALUES (2, 10, ARRAY[2, 3]), (9, 20, ARRAY[8])) AS r(a, b, arr)");

        // Join conditions are pushed into the join inputs while the query is converted, which
        // classifies and re-indexes the column references of each predicate. Captures are
        // therefore lifted out of the lambda bodies before that, so that a captured column counts
        // as a reference of the predicate. Each result encodes a joined pair as l.k * 100 + r.b.

        // Captures of the predicate's own side: r.a and r.b are re-indexed into the right input.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + r.b FROM l JOIN r"
                                        + " ON CARDINALITY(ARRAY_FILTER(r.arr, x -> x = r.a)) > 0"))
                .containsExactlyInAnyOrder(110, 210);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + r.b FROM l JOIN r"
                                        + " ON CARDINALITY(ARRAY_FILTER(r.arr, x -> x + r.b < 15)) > 0"))
                .containsExactlyInAnyOrder(110, 210);

        // Captures of the other side: the predicate must stay on the join.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + r.b FROM l JOIN r"
                                        + " ON CARDINALITY(ARRAY_FILTER(l.arr, x -> x = r.a)) > 0"))
                .containsExactly(110);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + COALESCE(r.b, 0) FROM l LEFT JOIN r"
                                        + " ON CARDINALITY(ARRAY_FILTER(l.arr, x -> x = r.a)) > 0"))
                .containsExactlyInAnyOrder(110, 200);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + r.b FROM l JOIN r"
                                        + " ON CARDINALITY(ARRAY_FILTER(l.arr,"
                                        + " x -> CARDINALITY(ARRAY_FILTER(r.arr, y -> y = x)) > 0)) > 0"))
                .containsExactly(110);

        // An equality whose operands are pushed into opposite inputs, with a capture of the right
        // side in the left operand: l.arr = [1, 2, 3] and r.a = 2 give 12 = r.b + 2.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + r.b FROM l JOIN r"
                                        + " ON ARRAY_REDUCE(l.arr, 0, (acc, x) -> acc + x * r.a)"
                                        + " = r.b + 2"))
                .containsExactly(110);

        // A lateral join is converted to a correlate whose condition is pushed into the right
        // input, with references to the left input rewritten into correlated field accesses.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.k * 100 + t.b FROM l JOIN LATERAL"
                                        + " (SELECT r.b, r.arr FROM r WHERE r.a > l.k) AS t"
                                        + " ON CARDINALITY(ARRAY_FILTER(t.arr, x -> x > l.k)) > 0"))
                .containsExactlyInAnyOrder(110, 120, 220);
    }

    @Test
    void testLambdaInStandaloneSqlExpression() throws Exception {
        // A computed column and callSql are validated and converted as stand-alone expressions over
        // a parameter row rather than as part of a query, a path that registers no lambda scope
        // and resolves columns through a name-to-node map instead of blackboard inputs.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final String dataId =
                TestValuesTableFactory.registerData(
                        Collections.singletonList(Row.of(new Integer[] {1, 2, 3}, 10)));
        tEnv.executeSql(
                "CREATE TABLE t (\n"
                        + "  a ARRAY<INT>,\n"
                        + "  base INT,\n"
                        + "  c0 AS ARRAY_TRANSFORM(a, x -> x + 1),\n"
                        + "  c1 AS ARRAY_TRANSFORM(a, x -> x + base),\n"
                        + "  c2 AS ARRAY_TRANSFORM(a, x -> ARRAY_REDUCE(a, 0, (acc, y) -> acc + x * y + base)),\n"
                        + "  c3 AS ARRAY_TRANSFORM(a, base -> base * 100)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true',\n"
                        + "  'data-id' = '"
                        + dataId
                        + "'\n"
                        + ")");

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery("SELECT c0, c1, c2, c3 FROM t").execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {11, 12, 13});
            // sum(a) * x + 3 * base = 6 * x + 30
            assertThat(row.getField(2)).isEqualTo(new Integer[] {36, 42, 48});
            // the parameter shadows the column
            assertThat(row.getField(3)).isEqualTo(new Integer[] {100, 200, 300});
            assertThat(iterator).isExhausted();
        }

        final Table input = higherOrderFunctionInput(tEnv);
        assertThat(collectSingle(input.select(callSql("ARRAY_TRANSFORM(a, x -> x + 1)"))))
                .isEqualTo(new Integer[] {2, 3, 4});
        assertThat(collectSingle(input.select(callSql("ARRAY_TRANSFORM(a, x -> x + base)"))))
                .isEqualTo(new Integer[] {11, 12, 13});
        assertThat(
                        collectSingle(
                                input.select(
                                        callSql(
                                                "ARRAY_TRANSFORM(a, x -> ARRAY_REDUCE(a, 0,"
                                                        + " (acc, y) -> acc + x * y + base))"))))
                .isEqualTo(new Integer[] {36, 42, 48});
        assertThat(collectSingle(input.select(callSql("ARRAY_TRANSFORM(a, base -> base * 100)"))))
                .isEqualTo(new Integer[] {100, 200, 300});
    }

    @Test
    void testLambdaInWatermarkExpression() throws Exception {
        // A watermark expression is evaluated by a generated watermark generator: in a
        // WatermarkAssigner operator or, pushed into the source, instantiated by the source itself
        // without a runtime context. The watermark is ts - 1 s while an element of a exceeds
        // 12 - base and ts otherwise, and each row observes the watermark of the previous one.
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(
                                        new Integer[] {1, 2, 3},
                                        10,
                                        LocalDateTime.parse("2024-01-01T10:00:00")),
                                Row.of(
                                        new Integer[] {1},
                                        10,
                                        LocalDateTime.parse("2024-01-01T10:00:05")),
                                Row.of(
                                        new Integer[] {1},
                                        10,
                                        LocalDateTime.parse("2024-01-01T10:00:10"))));
        for (boolean pushDown : new boolean[] {false, true}) {
            final TableEnvironment tEnv =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            tEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);
            tEnv.executeSql(
                    "CREATE TABLE t (\n"
                            + "  a ARRAY<INT>,\n"
                            + "  base INT,\n"
                            + "  ts TIMESTAMP(3),\n"
                            + "  WATERMARK FOR ts AS CASE"
                            + " WHEN CARDINALITY(ARRAY_FILTER(a, x -> x + base > 12)) > 0"
                            + " THEN ts - INTERVAL '1' SECOND ELSE ts END\n"
                            + ") WITH (\n"
                            + "  'connector' = 'values',\n"
                            + "  'bounded' = 'true',\n"
                            + "  'disable-lookup' = 'true',\n"
                            + "  'enable-watermark-push-down' = '"
                            + pushDown
                            + "',\n"
                            + "  'data-id' = '"
                            + dataId
                            + "'\n"
                            + ")");

            final String query = "SELECT CURRENT_WATERMARK(ts) FROM t";
            if (pushDown) {
                assertThat(tEnv.explainSql(query))
                        .doesNotContain("+- WatermarkAssigner(")
                        .contains("watermark=[CASE(");
            } else {
                assertThat(tEnv.explainSql(query)).contains("+- WatermarkAssigner(");
            }
            assertThat(collectColumn(tEnv, query))
                    .containsExactly(
                            null,
                            LocalDateTime.parse("2024-01-01T09:59:59"),
                            LocalDateTime.parse("2024-01-01T10:00:05"));
        }
    }

    /**
     * A watermark generator pushed into a source has no runtime context, but the source hands it a
     * watermark context with the source's metric group. A function called from a lambda body in the
     * watermark expression reports to that group, like one called outside a body.
     */
    @Test
    void testUserDefinedFunctionInPushedDownWatermarkLambdaUsesSourceMetricGroup() {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.createTemporarySystemFunction("my_metrics", MetricGroupRecordingScalarFunction.class);
        final WatermarkCapturingSource source = new WatermarkCapturingSource();
        tEnv.createTable(
                "t",
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.ARRAY(DataTypes.INT()))
                                        .column("ts", DataTypes.TIMESTAMP(3))
                                        .watermark(
                                                "ts",
                                                "CASE WHEN CARDINALITY(ARRAY_FILTER(a,"
                                                        + " x -> my_metrics(x) > 0)) > 0"
                                                        + " THEN ts - INTERVAL '1' SECOND"
                                                        + " ELSE ts END")
                                        .build())
                        .source(source)
                        .build());
        tEnv.explainSql("SELECT * FROM t");
        assertThat(source.watermarkStrategy).isNotNull();

        MetricGroupRecordingScalarFunction.OPENED_WITH.clear();
        final MetricGroup sourceMetricGroup = new UnregisteredMetricsGroup();
        source.watermarkStrategy.createWatermarkGenerator(
                new WatermarkGeneratorSupplier.Context() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return sourceMetricGroup;
                    }

                    @Override
                    public RelativeClock getInputActivityClock() {
                        return SystemClock.getInstance();
                    }
                });
        assertThat(MetricGroupRecordingScalarFunction.OPENED_WITH)
                .singleElement()
                .isSameAs(sourceMetricGroup);
    }

    @Test
    void testLambdaInPartitionPredicate() throws Exception {
        // A predicate on partition columns only is evaluated in the planner against the listed
        // partitions, by a generated function that has no runtime context.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, "A"), Row.of(2, "B"), Row.of(3, "C")));
        tEnv.executeSql(
                "CREATE TABLE p (id INT, region STRING) PARTITIONED BY (region) WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true',\n"
                        + "  'partition-list' = 'region:A;region:B;region:C',\n"
                        + "  'data-id' = '"
                        + dataId
                        + "'\n"
                        + ")");

        final String query =
                "SELECT id FROM p"
                        + " WHERE CARDINALITY(ARRAY_FILTER(ARRAY['A', 'C'], x -> x = region)) > 0";
        assertThat(tEnv.explainSql(query)).contains("partitions=[{region=A}, {region=C}]");
        assertThat(collectColumn(tEnv, query)).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    void testLambdaPredicateIsNotOfferedToSource() {
        // A lambda cannot be expressed as an Expression for a connector, so a conjunct with a
        // higher-order call stays in the Calc above the scan. The other conjuncts are still pushed.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final FilterRecordingSource source = new FilterRecordingSource();
        tEnv.createTable(
                "t",
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("a", DataTypes.ARRAY(DataTypes.INT()))
                                        .build())
                        .source(source)
                        .build());

        final String plan =
                tEnv.explainSql(
                        "SELECT id FROM t"
                                + " WHERE id > 1 AND CARDINALITY(ARRAY_FILTER(a, x -> x > id)) > 0");

        assertThat(source.offeredFilters)
                .isNotEmpty()
                .allSatisfy(filter -> assertThat(filter).isEqualTo("greaterThan(id, 1)"));
        assertThat(plan).contains("ARRAY_FILTER");
    }

    @Test
    void testViewOverBuiltInHigherOrderFunctions() throws Exception {
        // A view does not persist its query as written: it persists the *expanded* query, i.e. the
        // validated statement unparsed back to SQL -- tables fully qualified, built-in function
        // names merely quoted -- and re-parses that string on every read. A lambda parameter is
        // bound by the enclosing higher-order call rather than by the query's row type, so the
        // expansion must leave it alone -- qualifying it would either fail to re-parse or silently
        // resolve to a column.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        tEnv.executeSql(
                "CREATE VIEW v AS SELECT"
                        + " ARRAY_TRANSFORM(a, x -> x + base) AS c0,"
                        + " ARRAY_FILTER(a, x -> x > 1) AS c1,"
                        + " ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) AS c2,"
                        + " ARRAY_ZIP_WITH(a, a, (x, y) -> x + y) AS c3"
                        + " FROM t");

        assertThat(expandedQueryOf(tEnv, "v"))
                .isEqualTo(
                        "SELECT `ARRAY_TRANSFORM`(`t`.`a`, `x` -> `x` + `t`.`base`) AS `c0`,"
                                + " `ARRAY_FILTER`(`t`.`a`, `x` -> `x` > 1) AS `c1`,"
                                + " `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`) AS `c2`,"
                                + " `ARRAY_ZIP_WITH`(`t`.`a`, `t`.`a`, (`x`, `y`) -> `x` + `y`) AS `c3`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery("SELECT * FROM v").execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {11, 12, 13});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {2, 3});
            assertThat(row.getField(2)).isEqualTo(6);
            assertThat(row.getField(3)).isEqualTo(new Integer[] {2, 4, 6});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testViewOverShadowingAndNestedLambdas() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda parameter named like a column of the source table. The expansion must not
        // qualify it, otherwise the re-parsed view would read the column instead of the parameter
        // and silently return {1000, 1000, 1000} (a = {1, 2, 3}, base = 10).
        tEnv.executeSql(
                "CREATE VIEW v_shadow AS SELECT base, ARRAY_TRANSFORM(a, base -> base * 100) AS c"
                        + " FROM t");
        assertThat(expandedQueryOf(tEnv, "v_shadow"))
                .isEqualTo(
                        "SELECT `t`.`base`, `ARRAY_TRANSFORM`(`t`.`a`, `base` -> `base` * 100) AS `c`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");
        assertThat(collectColumn(tEnv, "SELECT c FROM v_shadow"))
                .containsExactly((Object) new Integer[] {100, 200, 300});

        // Nested lambdas whose parameters shadow each other, mixed with a capture of an enclosing
        // parameter and of an outer column. Only the outer column may be qualified.
        tEnv.executeSql(
                "CREATE VIEW v_nested AS SELECT"
                        + " ARRAY_TRANSFORM(a, x -> ARRAY_REDUCE(a, x, (acc, x) -> acc + x)) AS c0,"
                        + " ARRAY_TRANSFORM(a, x -> ARRAY_REDUCE(a, 0, (acc, y) -> acc + y + x + base)) AS c1"
                        + " FROM t");
        assertThat(expandedQueryOf(tEnv, "v_nested"))
                .isEqualTo(
                        "SELECT `ARRAY_TRANSFORM`(`t`.`a`, `x` -> `ARRAY_REDUCE`(`t`.`a`, `x`,"
                                + " (`acc`, `x`) -> `acc` + `x`)) AS `c0`,"
                                + " `ARRAY_TRANSFORM`(`t`.`a`, `x` -> `ARRAY_REDUCE`(`t`.`a`, 0,"
                                + " (`acc`, `y`) -> `acc` + `y` + `x` + `t`.`base`)) AS `c1`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery("SELECT * FROM v_nested").execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            // the inner reduce starts at x and folds {1, 2, 3} over the shadowing parameter
            assertThat(row.getField(0)).isEqualTo(new Integer[] {7, 8, 9});
            // sum(a) + 3 * (x + base) = 6 + 3 * (x + 10)
            assertThat(row.getField(1)).isEqualTo(new Integer[] {39, 42, 45});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testViewOverHigherOrderFunctionsOutsideOfAProjection() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A higher-order call in a filter and as a grouping key, plus a star expansion next to one.
        tEnv.executeSql(
                "CREATE VIEW v AS SELECT *, ARRAY_FILTER(a, x -> x > base - 9) AS c FROM t"
                        + " WHERE ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) > 1");
        assertThat(expandedQueryOf(tEnv, "v"))
                .isEqualTo(
                        "SELECT `t`.`a`, `t`.`m`, `t`.`base`,"
                                + " `ARRAY_FILTER`(`t`.`a`, `x` -> `x` > `t`.`base` - 9) AS `c`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`"
                                + " WHERE `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`) > 1");
        assertThat(collectColumn(tEnv, "SELECT c FROM v"))
                .containsExactly((Object) new Integer[] {2, 3});

        // A view stacked on a view: the outer lambda works on a column that is itself the result of
        // a lambda, so both expansions have to hold.
        tEnv.executeSql("CREATE VIEW v2 AS SELECT ARRAY_TRANSFORM(c, x -> x * 10) AS c FROM v");
        assertThat(expandedQueryOf(tEnv, "v2"))
                .isEqualTo(
                        "SELECT `ARRAY_TRANSFORM`(`v`.`c`, `x` -> `x` * 10) AS `c`"
                                + " FROM `default_catalog`.`default_database`.`v` AS `v`");
        assertThat(collectColumn(tEnv, "SELECT c FROM v2"))
                .containsExactly((Object) new Integer[] {20, 30});

        tEnv.executeSql(
                "CREATE VIEW v_group AS SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) AS s,"
                        + " COUNT(*) AS n FROM t GROUP BY ARRAY_REDUCE(a, 0, (acc, x) -> acc + x)");
        assertThat(expandedQueryOf(tEnv, "v_group"))
                .isEqualTo(
                        "SELECT `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`) AS `s`,"
                                + " COUNT(*) AS `n`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`"
                                + " GROUP BY `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`)");
        assertThat(collectColumn(tEnv, "SELECT s FROM v_group")).containsExactly(6);
    }

    /**
     * Two textually identical higher-order calls whose lambda body is non-deterministic collapse
     * into a single {@code RexProgram} expression referenced twice, so code generation must not
     * reuse the generated value for the second reference. The counting function returns a fresh
     * value per invocation, hence six distinct values iff both calls are evaluated independently.
     */
    @Test
    void testNonDeterministicLambdaBodyIsEvaluatedPerCall() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        CountingScalarFunction.COUNTER.set(0);
        tEnv.createTemporarySystemFunction("cnt", CountingScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery(
                                "SELECT ARRAY_TRANSFORM(a, x -> cnt(x)),"
                                        + " ARRAY_TRANSFORM(a, x -> cnt(x)),"
                                        + " ARRAY_TRANSFORM(a, y -> cnt(y)) FROM t")
                        .execute()
                        .collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(iterator).isExhausted();
            assertThat(
                            Stream.of(row.getField(0), row.getField(1), row.getField(2))
                                    .flatMap(array -> Arrays.stream((Integer[]) array)))
                    .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9);
        }
    }

    /**
     * A non-deterministic call in a lambda body must not be pushed below the aggregation that a
     * filter on the higher-order call sits above, and must be re-evaluated for the projection: the
     * function is called once per element per surviving evaluation.
     */
    @Test
    void testNonDeterministicLambdaBodyInFilterAndProjection() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        CountingScalarFunction.COUNTER.set(0);
        tEnv.createTemporarySystemFunction("cnt", CountingScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        final List<Object> values =
                collectColumn(
                        tEnv,
                        "SELECT ARRAY_TRANSFORM(a, x -> cnt(x)) FROM t"
                                + " WHERE ARRAY_TRANSFORM(a, x -> cnt(x))[1] > 0");
        assertThat(values).hasSize(1);
        assertThat((Integer[]) values.get(0)).doesNotContain(1, 2, 3);
        assertThat(CountingScalarFunction.COUNTER).hasValue(6);
    }

    @Test
    void testUserDefinedFunctionInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2, 3}, 100)));

        // an ordinary user-defined function called from a lambda body, with and without an outer
        // capture, and at the innermost level of a nested lambda: per e, the fold yields
        // e + sum(x + base) = e + 306
        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery(
                                "SELECT ARRAY_TRANSFORM(f0, x -> my_plus_one(x)),"
                                        + " ARRAY_TRANSFORM(f0, x -> my_add(x, base)),"
                                        + " ARRAY_TRANSFORM(f0, e -> ARRAY_REDUCE(f0, e,"
                                        + " (acc, x) -> my_add(acc, my_add(x, base)))) FROM t")
                        .execute()
                        .collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(row.getField(2)).isEqualTo(new Integer[] {307, 308, 309});
            assertThat(iterator).isExhausted();
        }

        // the same through the Table API, calling the functions by name: the calls in a lambda
        // body, including a nested one, are looked up together with the rest of the projection
        try (final CloseableIterator<Row> iterator =
                tEnv.from("t")
                        .select(
                                $("f0").arrayTransform(x -> call("my_plus_one", x)),
                                $("f0").arrayTransform(x -> call("my_add", x, $("base"))),
                                $("f0").arrayTransform(
                                                x ->
                                                        $("f0").arrayFilter(
                                                                        y ->
                                                                                call("my_add", x, y)
                                                                                        .isGreater(
                                                                                                4))
                                                                .cardinality()))
                        .execute()
                        .collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {101, 102, 103});
            assertThat(row.getField(2)).isEqualTo(new Integer[] {0, 1, 2});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testHigherOrderFunctionAsArgumentOfUserDefinedFunction() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_plus_one", PlusOneScalarFunction.class);
        tEnv.createTemporarySystemFunction("my_add", AddScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // the opposite direction: a lambda-carrying call as the argument of an ordinary function,
        // once with a body that itself calls ordinary functions: ((0 + 2) + 3) + 4 + 1 = 10
        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery(
                                "SELECT my_plus_one(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x)),"
                                        + " my_plus_one(ARRAY_REDUCE(a, 0,"
                                        + " (acc, x) -> my_add(acc, my_plus_one(x)))) FROM t")
                        .execute()
                        .collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(7);
            assertThat(row.getField(1)).isEqualTo(10);
            assertThat(iterator).isExhausted();
        }
    }

    /**
     * A function called from a lambda body keeps its regular lifecycle, although the body is
     * compiled into a class of its own and evaluated once per element: {@code open()} runs once
     * before the first element, and {@code close()} once when the task finishes.
     */
    @Test
    void testUserDefinedFunctionInLambdaBodyIsOpenedAndClosed() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_tracked", LifecycleTrackingScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        LifecycleTrackingScalarFunction.OPEN_COUNT.set(0);
        LifecycleTrackingScalarFunction.CLOSE_COUNT.set(0);

        final TableResult result =
                tEnv.sqlQuery("SELECT ARRAY_TRANSFORM(a, x -> my_tracked(x)) FROM t").execute();
        try (final CloseableIterator<Row> iterator = result.collect()) {
            assertThat(iterator).hasNext();
            // the offset is established in open(), so x + 1000 shows it ran before the first call
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {1001, 1002, 1003});
            assertThat(iterator).isExhausted();
        }
        // a finished job has run close() on every task
        result.await();

        assertThat(LifecycleTrackingScalarFunction.OPEN_COUNT).hasValue(1);
        assertThat(LifecycleTrackingScalarFunction.CLOSE_COUNT).hasValue(1);
    }

    /**
     * A function called from a lambda body is opened with the operator's context, so it sees the
     * job parameters and the task info like a function called outside a body, also in a nested
     * body.
     */
    @Test
    void testUserDefinedFunctionInLambdaBodyIsOpenedWithOperatorContext() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig()
                .set(
                        PipelineOptions.GLOBAL_JOB_PARAMETERS,
                        Collections.singletonMap("lambda.offset", "500"));
        tEnv.createTemporarySystemFunction("my_context", JobParameterScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery(
                                "SELECT ARRAY_TRANSFORM(a, x -> my_context(x)), "
                                        + "ARRAY_TRANSFORM(a, x -> "
                                        + "ARRAY_TRANSFORM(ARRAY[x], y -> my_context(y))[1]) "
                                        + "FROM t")
                        .execute()
                        .collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {501, 502, 503});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {501, 502, 503});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdaBodyErrorPropagatesUnchanged() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_throw", ThrowingScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // an unchecked exception thrown while the body is evaluated fails the job with that
        // exception as the root cause, as it would outside of a lambda
        assertThatThrownBy(
                        () ->
                                collectSingleRow(
                                        tEnv, "SELECT ARRAY_TRANSFORM(a, x -> my_throw(x)) FROM t"))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("boom for 1");
    }

    /**
     * A lambda body is compiled into a generated class of its own, but that class is subject to the
     * JVM's 64 KB per-method limit like any other, and machine-generated SQL can drive a body past
     * it. Such a body must therefore be split into separate methods by {@code
     * flink-table-code-splitter} exactly like the same expression written outside a lambda,
     * otherwise it fails to compile with a misleading "this is a bug" error. The bodies below are
     * far above {@link TableConfigOptions#MAX_LENGTH_GENERATED_CODE}.
     */
    @Test
    void testLargeLambdaBodyIsSplitIntoSeparateMethods() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a1", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("a2", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(new Integer[] {1, 2}, new Integer[] {10, 20})));

        // The same expression outside of a lambda compiles, so the lambda must not regress.
        assertThat(collectSingleRow(tEnv, "SELECT " + longExpression("1") + " FROM t"))
                .isEqualTo(longExpressionResult(1));

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a1, x -> "
                                        + longExpression("x")
                                        + ") FROM t"))
                .isEqualTo(new Integer[] {longExpressionResult(1), longExpressionResult(2)});

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_FILTER(a1, x -> "
                                        + longExpression("x")
                                        + " > 0) FROM t"))
                .isEqualTo(new Integer[] {1, 2});

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_REDUCE(a1, 0, (acc, x) -> acc + "
                                        + longExpression("x")
                                        + ") FROM t"))
                .isEqualTo(longExpressionResult(1) + longExpressionResult(2));

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_ZIP_WITH(a1, a2, (x, y) -> y + "
                                        + longExpression("x")
                                        + ") FROM t"))
                .isEqualTo(
                        new Integer[] {10 + longExpressionResult(1), 20 + longExpressionResult(2)});
    }

    /** The number of terms in {@link #longExpression(String)}. */
    private static final int LONG_EXPRESSION_TERMS = 500;

    /**
     * An {@code INT} expression over {@code term} that generates well beyond 64 KB of Java code:
     * the casts are neither constant-folded nor shared, so every term contributes its own code.
     *
     * <p>The summands are combined into a balanced tree rather than the left-deep chain that {@code
     * a + b + c + ...} parses into, because code generation recurses once per nesting level of the
     * expression: a chain this long exhausts the JVM's default 1 MB thread stack before the
     * generated code is ever split. The nesting depth is not what this test is about, and testing
     * it would only pin down the recursion limit of the code generator.
     */
    private static String longExpression(String term) {
        List<String> summands = new ArrayList<>();
        summands.add(term);
        for (int i = 0; i < LONG_EXPRESSION_TERMS; i++) {
            summands.add("CAST(CAST(" + term + " + " + i + " AS VARCHAR) AS INT)");
        }
        while (summands.size() > 1) {
            final List<String> pairs = new ArrayList<>();
            for (int i = 0; i < summands.size(); i += 2) {
                if (i + 1 < summands.size()) {
                    pairs.add("(" + summands.get(i) + " + " + summands.get(i + 1) + ")");
                } else {
                    pairs.add(summands.get(i));
                }
            }
            summands = pairs;
        }
        return summands.get(0);
    }

    /** The value of {@link #longExpression(String)} for the given term value. */
    private static int longExpressionResult(int term) {
        int result = term;
        for (int i = 0; i < LONG_EXPRESSION_TERMS; i++) {
            result += term + i;
        }
        return result;
    }

    // --------------------------------------------------------------------------------------------

    /** A non-deterministic function returning a fresh value on every invocation. */
    public static class CountingScalarFunction extends ScalarFunction {

        static final AtomicInteger COUNTER = new AtomicInteger();

        public Integer eval(Integer i) {
            return COUNTER.incrementAndGet();
        }

        @Override
        public boolean isDeterministic() {
            return false;
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
     * An ordinary function that counts its lifecycle calls and adds an offset established in {@link
     * #open(FunctionContext)}, so that a call before {@code open()} would return {@code x} rather
     * than {@code x + 1000}.
     */
    public static class LifecycleTrackingScalarFunction extends ScalarFunction {

        static final AtomicInteger OPEN_COUNT = new AtomicInteger();
        static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        private transient int offset;

        @Override
        public void open(FunctionContext context) {
            OPEN_COUNT.incrementAndGet();
            offset = 1000;
        }

        @Override
        public void close() {
            CLOSE_COUNT.incrementAndGet();
        }

        public @Nullable Integer eval(@Nullable Integer x) {
            return x == null ? null : x + offset;
        }
    }

    /**
     * An ordinary function that adds an offset read from the job parameters in {@link
     * #open(FunctionContext)}, and requires the task info there, both of which only an operator's
     * runtime context provides.
     */
    public static class JobParameterScalarFunction extends ScalarFunction {

        private transient int offset;

        @Override
        public void open(FunctionContext context) {
            assertThat(context.getTaskInfo().getNumberOfParallelSubtasks()).isPositive();
            offset = Integer.parseInt(context.getJobParameter("lambda.offset", "0"));
        }

        public @Nullable Integer eval(@Nullable Integer x) {
            return x == null ? null : x + offset;
        }
    }

    /** An ordinary function that fails, for use in a lambda body. */
    public static class ThrowingScalarFunction extends ScalarFunction {

        public Integer eval(Integer i) {
            throw new IllegalStateException("boom for " + i);
        }
    }

    /** Records the metric group of the context it is opened with. */
    public static class MetricGroupRecordingScalarFunction extends ScalarFunction {

        static final List<MetricGroup> OPENED_WITH = new CopyOnWriteArrayList<>();

        @Override
        public void open(FunctionContext context) {
            OPENED_WITH.add(context.getMetricGroup());
        }

        public @Nullable Integer eval(@Nullable Integer x) {
            return x;
        }
    }

    /** A source that keeps the watermark strategy pushed into it. */
    private static class WatermarkCapturingSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsWatermarkPushDown {

        private @Nullable WatermarkStrategy<RowData> watermarkStrategy;

        @Override
        public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
            this.watermarkStrategy = watermarkStrategy;
        }
    }

    /** A source that accepts no filter and keeps a summary of every filter offered to it. */
    private static class FilterRecordingSource extends TableFactoryHarness.ScanSourceBase
            implements SupportsFilterPushDown {

        private final List<String> offeredFilters = new ArrayList<>();

        @Override
        public Result applyFilters(List<ResolvedExpression> filters) {
            filters.forEach(filter -> offeredFilters.add(filter.asSummaryString()));
            return Result.of(Collections.emptyList(), filters);
        }
    }

    /** A table function, which is not supported in a lambda body. */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class SplitTableFunction extends TableFunction<Row> {

        public void eval(String s) {
            for (String part : s.split(" ")) {
                collect(Row.of(part));
            }
        }
    }

    /** An asynchronous scalar function, which is not supported in a lambda body. */
    public static class AsyncPlusOneScalarFunction extends AsyncScalarFunction {

        public void eval(CompletableFuture<Integer> future, Integer i) {
            future.complete(i + 1);
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
                    .inputTypeStrategy(InputTypeStrategies.sequence())
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
                    .inputTypeStrategy(InputTypeStrategies.sequence())
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.FUNCTION(1))))
                    .build();
        }
    }

    /** An aggregate function that declares a {@code FUNCTION} accumulator. */
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
                            InputTypeStrategies.sequence(
                                    InputTypeStrategies.logical(LogicalTypeRoot.INTEGER)))
                    .accumulatorTypeStrategy(TypeStrategies.explicit(DataTypes.FUNCTION(1)))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * Like {@link FunctionAccumulatorAggregateFunction}, but for a named state entry of a process
     * table function. The state types are declared programmatically because reflective extraction
     * rejects a non-composite state type earlier.
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

    /** A single row with an array, a map, and a scalar column for negative tests. */
    private static Table higherOrderFunctionInput(TableEnvironment tEnv) {
        final java.util.Map<String, Integer> m = new java.util.LinkedHashMap<>();
        m.put("a", 1);
        return tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD("base", DataTypes.INT())),
                Row.of(new Integer[] {1, 2, 3}, m, 10));
    }

    /**
     * Two rows sharing an array and a group column, with a scalar column that sums to 30 -- so that
     * an aggregate over the enclosing query is distinguishable from the value of a single row.
     */
    private static Table groupedHigherOrderFunctionInput(TableEnvironment tEnv) {
        return tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("g", DataTypes.STRING()),
                        DataTypes.FIELD("base", DataTypes.INT())),
                Row.of(new Integer[] {1, 2, 3}, "abc", 10),
                Row.of(new Integer[] {1, 2, 3}, "abc", 20));
    }

    private static Object collectSingleRow(TableEnvironment tEnv, String sql) throws Exception {
        try (final CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            assertThat(iterator).hasNext();
            final Object value = iterator.next().getField(0);
            assertThat(iterator).isExhausted();
            return value;
        }
    }

    /** The message of the {@link ValidationException} the given query is expected to fail with. */
    private static String messageOf(Supplier<Table> query) {
        return assertThatThrownBy(() -> query.get().execute())
                .isInstanceOf(ValidationException.class)
                .actual()
                .getMessage();
    }

    private static Object collectSingle(Table table) throws Exception {
        try (final CloseableIterator<Row> iterator = table.execute().collect()) {
            assertThat(iterator).hasNext();
            final Object value = iterator.next().getField(0);
            assertThat(iterator).isExhausted();
            return value;
        }
    }

    private static List<Object> collectColumn(TableEnvironment tEnv, String sql) throws Exception {
        return collectColumn(tEnv.sqlQuery(sql));
    }

    private static List<Object> collectColumn(Table table) throws Exception {
        try (final CloseableIterator<Row> iterator = table.execute().collect()) {
            final List<Object> values = new ArrayList<>();
            iterator.forEachRemaining(row -> values.add(row.getField(0)));
            return values;
        }
    }

    private static void assertInvalidCall(
            TableEnvironment tEnv, String sql, String... expectedMessageParts) {
        assertThatThrownBy(() -> tEnv.sqlQuery(sql))
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        e -> {
                            final String message = ExceptionUtils.stringifyException(e);
                            for (String part : expectedMessageParts) {
                                assertThat(message).contains(part);
                            }
                        });
    }
}
