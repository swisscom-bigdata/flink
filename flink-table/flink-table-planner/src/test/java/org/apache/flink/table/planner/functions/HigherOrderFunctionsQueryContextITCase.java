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

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.RowSemanticTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.SetSemanticTableFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.apache.calcite.plan.RelOptUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.currentWatermark;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lambdas in the query constructs that the planner handles apart from a plain projection: joins
 * with their own operators (lookup, temporal, interval and multi-way joins), aggregations,
 * changelog input, asynchronous and Python functions next to a lambda, correlates ({@code UNNEST},
 * {@code LATERAL TABLE}), {@code MATCH_RECOGNIZE} and time attributes. Each construct generates or
 * runs the lambda through a different code path, so each is asserted on its computed result.
 *
 * <p>The table {@code t} holds {@code (1, [1, 2, 3], base 2)} and {@code (2, [5], base 1)}; the
 * lookup table {@code d} holds {@code (1, b 1, [7, 8])} and {@code (2, b 9, [1])}.
 */
class HigherOrderFunctionsQueryContextITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    private static final String MAIN_DATA =
            TestValuesTableFactory.registerData(
                    Arrays.asList(
                            Row.of(
                                    1,
                                    new Integer[] {1, 2, 3},
                                    2,
                                    LocalDateTime.parse("2024-01-01T10:00:01")),
                            Row.of(
                                    2,
                                    new Integer[] {5},
                                    1,
                                    LocalDateTime.parse("2024-01-01T10:00:02"))));

    private static final String LOOKUP_DATA =
            TestValuesTableFactory.registerData(
                    Arrays.asList(
                            Row.of(1, 1, new Integer[] {7, 8}), Row.of(2, 9, new Integer[] {1})));

    @Test
    void testLambdaInLookupJoin() throws Exception {
        // An asynchronous lookup join instantiates the generated join condition once per in-flight
        // request; every instance opens a lambda factory of its own.
        for (boolean async : new boolean[] {false, true}) {
            final TableEnvironment tEnv = streamingEnvironment();
            tEnv.executeSql(
                    "CREATE TABLE d (id INT, b INT, arr ARRAY<INT>) WITH ("
                            + " 'connector' = 'values', 'bounded' = 'true', 'async' = '"
                            + async
                            + "', 'data-id' = '"
                            + LOOKUP_DATA
                            + "')");
            // A lambda in the join condition, capturing both sides.
            assertThat(
                            collectColumn(
                                    tEnv,
                                    "SELECT t.id FROM t JOIN d FOR SYSTEM_TIME AS OF t.proc"
                                            + " ON t.id = d.id"
                                            + " AND CARDINALITY(ARRAY_FILTER(t.a, x -> x > d.b)) > 0"))
                    .as("async = %s", async)
                    .containsExactly(1);
            // A lambda over the lookup table only, evaluated on the looked-up rows.
            assertThat(
                            collectColumn(
                                    tEnv,
                                    "SELECT t.id FROM t JOIN d FOR SYSTEM_TIME AS OF t.proc"
                                            + " ON t.id = d.id"
                                            + " WHERE CARDINALITY(ARRAY_FILTER(d.arr, x -> x > d.b)) > 0"))
                    .as("async = %s", async)
                    .containsExactly(1);
        }
    }

    @Test
    void testLambdaInDeltaJoin() throws Exception {
        // A delta join keeps no join state: for a row of one side, it looks up the matching rows
        // of the other side in its source, through an index and asynchronously. The asynchronous
        // runner instantiates the join condition and the calcs on its inputs once per in-flight
        // request, and each instance opens a lambda factory of its own.
        final TableEnvironment tEnv = deltaJoinEnvironment(DeltaJoinStrategy.FORCE);

        // A lambda in the join condition that reads both sides.
        final String conditionQuery =
                "SELECT l.k, r.b FROM dl l JOIN dr r ON l.k = r.k"
                        + " AND CARDINALITY(ARRAY_FILTER(l.a, x -> x > r.b)) > 0";
        assertThat(insertIntoUpsertSink(tEnv, "delta_condition_sink", "b INT", conditionQuery))
                .containsExactly("+I[1, 1]");

        // Lambdas in a filter and in projections on both inputs. A delta join looks up each side
        // from the other, so these run on the stream side and on the looked-up rows.
        assertThat(
                        insertIntoUpsertSink(
                                tEnv,
                                "delta_calc_sink",
                                "t ARRAY<INT>, u ARRAY<INT>",
                                "SELECT l.k, l.t, r.u FROM (SELECT k,"
                                        + " ARRAY_TRANSFORM(a, x -> x + base) AS t FROM dl"
                                        + " WHERE CARDINALITY(ARRAY_FILTER(a, x -> x > base)) > 0) l"
                                        + " JOIN (SELECT k, ARRAY_TRANSFORM(arr, x -> x * b) AS u"
                                        + " FROM dr) r ON l.k = r.k"))
                .containsExactlyInAnyOrder("+I[1, [3, 4, 5], [7, 8]]", "+I[2, [6], [9]]");

        // A cascaded delta join, with a lambda in the condition of the second join.
        assertThat(
                        insertIntoUpsertSink(
                                tEnv,
                                "delta_cascaded_sink",
                                "c INT",
                                "SELECT l.k, m.c FROM dl l JOIN dr r ON l.k = r.k"
                                        + " JOIN dm m ON r.k = m.k"
                                        + " AND CARDINALITY(ARRAY_FILTER(l.a, x -> x > m.c)) > 0"))
                .containsExactlyInAnyOrder("+I[1, 2]", "+I[2, 0]");

        // The lambda survives a compiled plan.
        tEnv.executeSql(upsertSinkDdl("delta_plan_sink", "b INT"));
        final CompiledPlan plan =
                tEnv.compilePlanSql(
                        "INSERT INTO delta_plan_sink "
                                + conditionQuery
                                + " ON CONFLICT DO DEDUPLICATE");
        tEnv.loadPlan(PlanReference.fromJsonString(plan.asJsonString()))
                .execute()
                .await(60, TimeUnit.SECONDS);
        assertThat(TestValuesTableFactory.getResultsAsStrings("delta_plan_sink"))
                .containsExactly("+I[1, 1]");

        // A delta join requires deterministic inputs, and RAND() in a lambda body makes the input
        // non-deterministic: FORCE rejects the query, AUTO falls back to a regular join.
        tEnv.executeSql(upsertSinkDdl("delta_random_sink", "b INT"));
        final String randomQuery =
                "INSERT INTO delta_random_sink SELECT l.k, r.b FROM (SELECT * FROM dl"
                        + " WHERE CARDINALITY(ARRAY_FILTER(a, x -> x > RAND() * 10)) > 0) l"
                        + " JOIN dr r ON l.k = r.k ON CONFLICT DO DEDUPLICATE";
        assertThatThrownBy(() -> tEnv.explainSql(randomQuery))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("doesn't support to do delta join optimization");
        final TableEnvironment autoEnv = deltaJoinEnvironment(DeltaJoinStrategy.AUTO);
        autoEnv.executeSql(upsertSinkDdl("delta_random_sink", "b INT"));
        assertThat(autoEnv.explainSql(randomQuery)).doesNotContain("DeltaJoin");
    }

    @Test
    void testLambdaInTemporalIntervalAndMultiWayJoin() throws Exception {
        final TableEnvironment tEnv = streamingEnvironment();
        final String versions =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1, 1, LocalDateTime.parse("2024-01-01T09:00:00")),
                                Row.of(2, 9, LocalDateTime.parse("2024-01-01T09:00:00"))));
        tEnv.executeSql(
                "CREATE TABLE r (id INT, b INT, rts TIMESTAMP(3), WATERMARK FOR rts AS rts,"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'values',"
                        + " 'bounded' = 'true', 'changelog-mode' = 'I', 'data-id' = '"
                        + versions
                        + "')");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT t.id FROM t JOIN r FOR SYSTEM_TIME AS OF t.ts"
                                        + " ON t.id = r.id"
                                        + " AND CARDINALITY(ARRAY_FILTER(t.a, x -> x > r.b)) > 0"))
                .containsExactly(1);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT l.id FROM t l, t r WHERE l.id = r.id"
                                        + " AND l.ts BETWEEN r.ts - INTERVAL '1' MINUTE"
                                        + " AND r.ts + INTERVAL '1' MINUTE"
                                        + " AND CARDINALITY(ARRAY_FILTER(l.a, x -> x > r.base)) > 0"))
                .containsExactlyInAnyOrder(1, 2);

        final TableEnvironment multiJoinEnv = streamingEnvironment();
        multiJoinEnv.getConfig().set("table.optimizer.multi-join.enabled", "true");
        assertThat(
                        collectColumn(
                                multiJoinEnv,
                                "SELECT t1.id FROM t t1 JOIN t t2 ON t1.id = t2.id"
                                        + " JOIN t t3 ON t2.id = t3.id"
                                        + " AND CARDINALITY(ARRAY_FILTER(t1.a, x -> x > t3.base)) > 0"))
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void testLambdaInStreamingAggregations() throws Exception {
        final TableEnvironment tEnv = streamingEnvironment();
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT SUM(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * base))"
                                        + " FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' HOUR))"
                                        + " GROUP BY window_start, window_end"))
                .containsExactly(17);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT SUM(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x))"
                                        + " OVER (PARTITION BY id ORDER BY ts) FROM t"))
                .containsExactlyInAnyOrder(6, 5);
        assertThat(
                        materialize(
                                tEnv,
                                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY id"
                                        + " ORDER BY ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) DESC) AS rn"
                                        + " FROM t) WHERE rn = 1"))
                .containsExactlyInAnyOrder("+I[1]", "+I[2]");

        // Two-phase aggregation with mini-batches splits the aggregate into a local and a global
        // one; the lambda argument is computed below both.
        final TableEnvironment twoPhaseEnv = streamingEnvironment();
        twoPhaseEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        twoPhaseEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
        twoPhaseEnv.getConfig().set("table.exec.mini-batch.size", "10");
        twoPhaseEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        assertThat(
                        materialize(
                                twoPhaseEnv,
                                "SELECT id, SUM(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * base))"
                                        + " FROM t GROUP BY id"))
                .containsExactlyInAnyOrder("+I[1, 12]", "+I[2, 5]");
    }

    @Test
    void testLambdaInBatchAggregations() throws Exception {
        final TableEnvironment tEnv = batchEnvironment();
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT COUNT(DISTINCT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * base))"
                                        + " FROM t"))
                .containsExactly(2L);
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) AS s, COUNT(*) FROM t"
                                        + " GROUP BY GROUPING SETS"
                                        + " ((ARRAY_REDUCE(a, 0, (acc, x) -> acc + x)), ())"))
                .containsExactlyInAnyOrder("+I[6, 1]", "+I[5, 1]", "+I[null, 2]");
        // A lambda as a group key, and a lambda over aggregates in HAVING.
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x * 0), COUNT(*) FROM t"
                                        + " GROUP BY ARRAY_TRANSFORM(a, x -> x * 0)"))
                .containsExactlyInAnyOrder("+I[[0, 0, 0], 1]", "+I[[0], 1]");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT id FROM t GROUP BY id HAVING"
                                        + " CARDINALITY(ARRAY_FILTER(ARRAY[SUM(base), MAX(base)],"
                                        + " x -> x > 1)) > 1"))
                .containsExactly(1);
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT DISTINCT ARRAY_TRANSFORM(t.a, x -> x * 0) FROM t, t AS t2"))
                .containsExactlyInAnyOrder("+I[[0, 0, 0]]", "+I[[0]]");
    }

    @Test
    void testLambdaOnChangelogInput() throws Exception {
        final TableEnvironment tEnv = streamingEnvironment();
        final String changelog =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, 1, new Integer[] {1, 2}),
                                Row.ofKind(RowKind.UPDATE_AFTER, 1, new Integer[] {3}),
                                Row.ofKind(RowKind.INSERT, 2, new Integer[] {0})));
        tEnv.executeSql(
                "CREATE TABLE u (id INT, a ARRAY<INT>, PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + " 'connector' = 'values', 'bounded' = 'true',"
                        + " 'changelog-mode' = 'I,UA,D', 'data-id' = '"
                        + changelog
                        + "')");
        // The filter is pushed past the normalization of the upsert input.
        assertThat(
                        materialize(
                                tEnv,
                                "SELECT id, a FROM u WHERE CARDINALITY(ARRAY_FILTER(a, x -> x > 2)) > 0"))
                .containsExactly("+I[1, [3]]");
        assertThat(materialize(tEnv, "SELECT SUM(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x)) FROM u"))
                .containsExactly("+I[3]");
    }

    @Test
    void testLambdaNextToAsynchronousAndPythonFunctions() throws Exception {
        final TableEnvironment tEnv = streamingEnvironment();
        tEnv.createTemporarySystemFunction(
                "async_plus", HigherOrderFunctionsBatchITCase.AsyncPlusOneScalarFunction.class);
        // The asynchronous call is split into an operator of its own; the lambda stays in the Calc.
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT async_plus(base), ARRAY_TRANSFORM(a, x -> x + base) FROM t"))
                .containsExactlyInAnyOrder("+I[3, [3, 4, 5]]", "+I[2, [6]]");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + 1)"
                                        + " FROM (SELECT ARRAY[async_plus(base)] AS a FROM t)"))
                .containsExactlyInAnyOrder(new Integer[] {4}, new Integer[] {3});

        // A Python function is split into a Python operator; a lambda next to it or as its
        // argument stays in a Java Calc.
        tEnv.createTemporarySystemFunction("py_plus", new PythonScalarFunction("py_plus"));
        // The optimized plan is checked without translating it, which would need flink-python.
        final PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
        final Table query =
                tEnv.sqlQuery(
                        "SELECT py_plus(id, base), ARRAY_TRANSFORM(a, x -> x + base),"
                                + " py_plus(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x), id) FROM t");
        final String plan = RelOptUtil.toString(planner.optimize(TableTestUtil.toRelNode(query)));
        assertThat(plan)
                .contains("StreamPhysicalPythonCalc(")
                .contains("StreamPhysicalCalc(")
                .contains("ARRAY_TRANSFORM(")
                .contains("ARRAY_REDUCE(");
    }

    @Test
    void testLambdaWithProcessTableFunction() throws Exception {
        // A built-in lambda can be used with a process table function: in its input table, in a
        // scalar argument, which the generated function runner evaluates, and over its output. A
        // lambda as an argument of the process table function itself is not supported yet.
        final TableEnvironment tEnv = streamingEnvironment();
        tEnv.executeSql("CREATE TEMPORARY VIEW pt AS SELECT id, a, base FROM t");
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW pv AS"
                        + " SELECT id, ARRAY_TRANSFORM(a, x -> x + base) AS tr FROM t");
        tEnv.createTemporarySystemFunction("row_ptf", RowSemanticTableFunction.class);
        tEnv.createTemporarySystemFunction("set_ptf", SetSemanticTableFunction.class);
        final String reduce = "ARRAY_REDUCE(ARRAY[1, 2, 3], 0, (acc, x) -> acc + x)";

        assertThat(collectColumn(tEnv, "SELECT `out` FROM row_ptf(r => TABLE pv, i => 1)"))
                .containsExactlyInAnyOrder("{+I[1, [3, 4, 5]], 1}", "{+I[2, [6]], 1}");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT `out` FROM row_ptf(r => TABLE pt, i => " + reduce + ")"))
                .containsExactlyInAnyOrder("{+I[1, [1, 2, 3], 2], 6}", "{+I[2, [5], 1], 6}");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT `out` FROM row_ptf(r => TABLE pt,"
                                        + " i => CARDINALITY(ARRAY_FILTER(ARRAY[1, 2, 3], x -> x > 1)))"))
                .containsExactlyInAnyOrder("{+I[1, [1, 2, 3], 2], 2}", "{+I[2, [5], 1], 2}");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT `out` FROM set_ptf(r => TABLE pv PARTITION BY id, i => "
                                        + reduce
                                        + ")"))
                .containsExactlyInAnyOrder("{+I[1, [3, 4, 5]], 6}", "{+I[2, [6]], 6}");

        // Over the output: "{+I[1, [1, 2, 3], 2], 1}" has 24 characters, the other row 18.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(ARRAY[CHAR_LENGTH(`out`)], x -> x * 2)"
                                        + " FROM row_ptf(r => TABLE pt, i => 1)"))
                .containsExactlyInAnyOrder(new Integer[] {48}, new Integer[] {36});
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT `out` FROM row_ptf(r => TABLE pt, i => 1) WHERE"
                                        + " CARDINALITY(ARRAY_FILTER(ARRAY[CHAR_LENGTH(`out`)], x -> x > 20)) > 0"))
                .containsExactly("{+I[1, [1, 2, 3], 2], 1}");

        // The same in the Table API.
        assertThat(
                        collectColumn(
                                tEnv.from("t")
                                        .select(
                                                $("id"),
                                                $("a").arrayTransform(x -> x.plus($("base")))
                                                        .as("tr"))
                                        .process(
                                                RowSemanticTableFunction.class,
                                                lit(1).asArgument("i"))))
                .containsExactlyInAnyOrder("{+I[1, [3, 4, 5]], 1}", "{+I[2, [6]], 1}");
        assertThat(
                        collectColumn(
                                tEnv.from("pt")
                                        .process(
                                                RowSemanticTableFunction.class,
                                                array(lit(1), lit(2), lit(3))
                                                        .arrayReduce(
                                                                lit(0), (acc, x) -> acc.plus(x))
                                                        .asArgument("i"))))
                .containsExactlyInAnyOrder("{+I[1, [1, 2, 3], 2], 6}", "{+I[2, [5], 1], 6}");

        // The lambdas in the input table and in the scalar argument survive a compiled plan.
        tEnv.executeSql(
                "CREATE TABLE ptf_lambda_sink (v STRING) WITH ("
                        + " 'connector' = 'values', 'sink-insert-only' = 'true')");
        final CompiledPlan plan =
                tEnv.compilePlanSql(
                        "INSERT INTO ptf_lambda_sink SELECT `out`"
                                + " FROM row_ptf(r => TABLE pv, i => "
                                + reduce
                                + ")");
        tEnv.loadPlan(PlanReference.fromJsonString(plan.asJsonString())).execute().await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("ptf_lambda_sink"))
                .containsExactlyInAnyOrder("+I[{+I[1, [3, 4, 5]], 6}]", "+I[{+I[2, [6]], 6}]");
    }

    @Test
    void testLambdaInQueryStructures() throws Exception {
        final TableEnvironment tEnv = batchEnvironment();
        assertThat(collectColumn(tEnv, "SELECT ARRAY_TRANSFORM(ARRAY[1, 2], x -> x * 2)"))
                .containsExactly((Object) new Integer[] {2, 4});
        assertThat(
                        collectRows(
                                tEnv,
                                "WITH c AS (SELECT id, ARRAY_TRANSFORM(a, x -> x + base) AS v FROM t)"
                                        + " SELECT c1.v, c2.v FROM c c1 JOIN c c2 ON c1.id = c2.id"))
                .containsExactlyInAnyOrder("+I[[3, 4, 5], [3, 4, 5]]", "+I[[6], [6]]");
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM t UNION ALL"
                                        + " SELECT ARRAY_TRANSFORM(a, x -> x - base) FROM t"))
                .containsExactlyInAnyOrder("+I[[3, 4, 5]]", "+I[[6]]", "+I[[-1, 0, 1]]", "+I[[4]]");

        // Two sinks that share the source and the lambda projection, so the sub-plan is reused.
        tEnv.executeSql("CREATE TABLE lambda_sink_1 (v ARRAY<INT>) WITH ('connector' = 'values')");
        tEnv.executeSql("CREATE TABLE lambda_sink_2 (v ARRAY<INT>) WITH ('connector' = 'values')");
        final StatementSet statements = tEnv.createStatementSet();
        statements.addInsertSql(
                "INSERT INTO lambda_sink_1 SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM t WHERE id = 1");
        statements.addInsertSql(
                "INSERT INTO lambda_sink_2 SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM t WHERE id = 2");
        statements.execute().await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("lambda_sink_1"))
                .containsExactly("+I[[3, 4, 5]]");
        assertThat(TestValuesTableFactory.getResultsAsStrings("lambda_sink_2"))
                .containsExactly("+I[[6]]");

        // With object reuse, each element's result is still independent of the next one.
        final TableEnvironment reuseEnv = batchEnvironment();
        reuseEnv.getConfig().set(PipelineOptions.OBJECT_REUSE, true);
        assertThat(
                        collectColumn(
                                        reuseEnv,
                                        "SELECT ARRAY_TRANSFORM(a, x -> ROW(x, base)) FROM t")
                                .stream()
                                .map(rows -> Arrays.toString((Object[]) rows))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("[+I[1, 2], +I[2, 2], +I[3, 2]]", "[+I[5, 1]]");
    }

    @Test
    void testLambdaInCorrelatedSubqueryCondition() throws Exception {
        // A lambda in the condition of a sub-query whose body reads a column of the sub-query or
        // of the enclosing query. Sub-query removal and decorrelation re-index the columns of such
        // a condition, so the lambda is lifted at conversion: an outer column becomes an ordinary
        // correlated operand of the call. The outer table has no time attribute: a correlated
        // scalar or LATERAL sub-query over a rowtime table fails in streaming also without a
        // lambda.
        for (boolean streaming : new boolean[] {false, true}) {
            final TableEnvironment tEnv =
                    TableEnvironment.create(
                            streaming
                                    ? EnvironmentSettings.inStreamingMode()
                                    : EnvironmentSettings.inBatchMode());
            tEnv.executeSql(
                    "CREATE TEMPORARY VIEW t AS SELECT * FROM (VALUES (1, ARRAY[1, 2, 3], 2),"
                            + " (2, ARRAY[5], 1)) AS t(id, a, base)");
            tEnv.executeSql(
                    "CREATE TEMPORARY VIEW su AS SELECT * FROM (VALUES (1, 3), (2, 1), (1, 1))"
                            + " AS su(k, d)");
            // The body reads t.base, an outer column, in a scalar sub-query.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id, (SELECT COUNT(*) FROM su WHERE"
                                            + " CARDINALITY(ARRAY_FILTER(ARRAY[su.d], x -> x > t.base))"
                                            + " > 0) FROM t"))
                    .as("streaming = %s", streaming)
                    .containsExactlyInAnyOrder("+I[1, 1]", "+I[2, 1]");
            // The body reads su.d, a column of the sub-query, in EXISTS.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM su WHERE su.k = t.id"
                                            + " AND CARDINALITY(ARRAY_FILTER(t.a, x -> x = su.d)) > 0)"))
                    .as("streaming = %s", streaming)
                    .containsExactly("+I[1]");
            // The body reads t.base in IN.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id FROM t WHERE id IN (SELECT k FROM su WHERE"
                                            + " CARDINALITY(ARRAY_FILTER(ARRAY[d], x -> x > t.base)) > 0)"))
                    .as("streaming = %s", streaming)
                    .containsExactly("+I[1]");
            // The body reads t.base in a LATERAL sub-query.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id, v FROM t, LATERAL (SELECT d AS v FROM su WHERE"
                                            + " CARDINALITY(ARRAY_FILTER(ARRAY[su.d], x -> x > t.base))"
                                            + " > 0)"))
                    .as("streaming = %s", streaming)
                    .containsExactlyInAnyOrder("+I[1, 3]", "+I[2, 3]");
        }
    }

    @Test
    void testCorrelatedSubqueryReadsEnclosingRowInMoreThanOnePlace() throws Exception {
        // A sub-query that reads the enclosing row in more than one place gets one correlation
        // variable per reference, which the conversion merges into one. The merging must reach a
        // sub-query inside a lambda body and a lambda inside a nested sub-query.
        for (boolean streaming : new boolean[] {false, true}) {
            final TableEnvironment tEnv =
                    TableEnvironment.create(
                            streaming
                                    ? EnvironmentSettings.inStreamingMode()
                                    : EnvironmentSettings.inBatchMode());
            tEnv.executeSql(
                    "CREATE TEMPORARY VIEW t AS SELECT * FROM (VALUES (1, ARRAY[1, 2, 3], 2),"
                            + " (2, ARRAY[5], 1)) AS t(id, a, base)");
            tEnv.executeSql(
                    "CREATE TEMPORARY VIEW su AS SELECT * FROM (VALUES (1, 3), (2, 1), (1, 1))"
                            + " AS su(k, d)");
            // A sub-query in the body reads t.base and t.id: MAX(d * base) is 6 for id 1 and 1
            // for id 2.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id, ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(d * t.base)"
                                            + " FROM su WHERE su.k = t.id)) FROM t"))
                    .as("streaming = %s", streaming)
                    .containsExactlyInAnyOrder("+I[1, [7, 8, 9]]", "+I[2, [6]]");
            // The same, with t.base read by a lambda inside that sub-query.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id, ARRAY_TRANSFORM(a, x -> x + (SELECT"
                                            + " MAX(ARRAY_REDUCE(ARRAY[d], 0, (acc, y) -> acc + y"
                                            + " * t.base)) FROM su WHERE su.k = t.id)) FROM t"))
                    .as("streaming = %s", streaming)
                    .containsExactlyInAnyOrder("+I[1, [7, 8, 9]]", "+I[2, [6]]");
            // A lambda in a nested sub-query reads t.base, and the sub-query around it reads t.id.
            assertThat(
                            materialize(
                                    tEnv,
                                    "SELECT id, (SELECT COUNT(*) FROM su WHERE su.k = t.id AND"
                                            + " EXISTS (SELECT 1 FROM su AS s2 WHERE s2.k = su.k AND"
                                            + " CARDINALITY(ARRAY_FILTER(ARRAY[s2.d], x -> x >"
                                            + " t.base)) > 0)) FROM t"))
                    .as("streaming = %s", streaming)
                    .containsExactlyInAnyOrder("+I[1, 2]", "+I[2, 0]");
        }
    }

    @Test
    void testLambdaCaptureInCorrelatedTableFunction() throws Exception {
        // UNNEST and LATERAL TABLE keep their correlation through decorrelation, so a lambda in
        // their argument reads a column of the left input through a correlation variable
        // ($cor0.base). It is lifted into a trailing call operand at conversion, where the
        // correlate evaluates it like any other operand.
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(new Integer[] {1, 2}, 10), Row.of(new Integer[] {3}, 20)));
        final String sourceDdl =
                "CREATE TABLE l (arr ARRAY<INT>, base INT) WITH ("
                        + " 'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')";
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.executeSql(sourceDdl);
        tEnv.createTemporarySystemFunction("explode_ints", ExplodeIntsTableFunction.class);

        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT v FROM l"
                                        + " CROSS JOIN UNNEST(ARRAY_TRANSFORM(arr, x -> x + base)) AS u(v)"))
                .containsExactlyInAnyOrder(11, 12, 23);
        // The capture is threaded through a nested lambda.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT v FROM l CROSS JOIN UNNEST(ARRAY_TRANSFORM(arr,"
                                        + " x -> ARRAY_REDUCE(arr, x, (acc, y) -> acc + y * base)))"
                                        + " AS u(v)"))
                .containsExactlyInAnyOrder(31, 32, 63);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT v FROM l LEFT JOIN"
                                        + " UNNEST(ARRAY_FILTER(arr, x -> x > base - 9)) AS u(v) ON TRUE"))
                .containsExactlyInAnyOrder(2, null);
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT v FROM l, LATERAL TABLE("
                                        + "explode_ints(ARRAY_TRANSFORM(arr, x -> x * base))) AS u(v)"))
                .containsExactlyInAnyOrder(10, 20, 60);
        assertThat(
                        collectColumn(
                                tEnv.from("l")
                                        .joinLateral(
                                                call(
                                                                "explode_ints",
                                                                $("arr").arrayTransform(
                                                                                x ->
                                                                                        x.times(
                                                                                                $(
                                                                                                        "base"))))
                                                        .as("v"))
                                        .select($("v"))))
                .containsExactlyInAnyOrder(10, 20, 60);

        // The lifted capture survives a compiled plan in streaming mode.
        final TableEnvironment streamEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        streamEnv.executeSql(sourceDdl);
        streamEnv.executeSql(
                "CREATE TABLE unnest_capture_sink (v INT) WITH ("
                        + " 'connector' = 'values', 'sink-insert-only' = 'true')");
        final CompiledPlan plan =
                streamEnv.compilePlanSql(
                        "INSERT INTO unnest_capture_sink SELECT v FROM l"
                                + " CROSS JOIN UNNEST(ARRAY_TRANSFORM(arr, x -> x + base)) AS u(v)");
        streamEnv.loadPlan(PlanReference.fromJsonString(plan.asJsonString())).execute().await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("unnest_capture_sink"))
                .containsExactlyInAnyOrder("+I[11]", "+I[12]", "+I[23]");
    }

    @Test
    void testLambdaInMatchRecognize() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(
                                        1,
                                        new Integer[] {1, 2},
                                        LocalDateTime.parse("2024-01-01T10:00:00")),
                                Row.of(
                                        1,
                                        new Integer[] {5},
                                        LocalDateTime.parse("2024-01-01T10:00:01"))));
        tEnv.executeSql(
                "CREATE TABLE e (id INT, a ARRAY<INT>, ts TIMESTAMP(3), WATERMARK FOR ts AS ts)"
                        + " WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");

        // Calcite registers no lambda scope in the clauses of MATCH_RECOGNIZE, so a lambda there is
        // rejected instead of failing with an internal error.
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY id ORDER BY ts"
                                                + " MEASURES ARRAY_TRANSFORM(A.a, x -> x + 1) AS m"
                                                + " ONE ROW PER MATCH PATTERN (A) DEFINE A AS TRUE)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Lambda expressions are not supported in the MEASURES clause of"
                                + " MATCH_RECOGNIZE.");
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT * FROM e MATCH_RECOGNIZE (PARTITION BY id ORDER BY ts"
                                                + " MEASURES A.id AS m ONE ROW PER MATCH PATTERN (A)"
                                                + " DEFINE A AS CARDINALITY(ARRAY_FILTER(A.a, x -> x > 1))"
                                                + " > 0)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Lambda expressions are not supported in the DEFINE clause of"
                                + " MATCH_RECOGNIZE.");

        // A lambda in the input table of MATCH_RECOGNIZE works as anywhere else.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT m FROM (SELECT id, ts,"
                                        + " CARDINALITY(ARRAY_FILTER(a, x -> x > 1)) AS c FROM e)"
                                        + " MATCH_RECOGNIZE (PARTITION BY id ORDER BY ts"
                                        + " MEASURES B.c AS m ONE ROW PER MATCH"
                                        + " PATTERN (A B) DEFINE A AS A.c = 1, B AS B.c = 1)"))
                .containsExactly(1);
    }

    /**
     * Only the operator reads the watermark and materializes the processing time, which a compiled
     * lambda body cannot do. {@code CURRENT_WATERMARK} and {@code PROCTIME()} in a body are
     * therefore computed once per row, outside the body. A body that returns a time attribute
     * converts its result to a regular timestamp, so the lambda itself is not converted.
     */
    @Test
    void testWatermarkAndTimeAttributesInLambdaBody() throws Exception {
        final TableEnvironment tEnv = streamingEnvironment();
        tEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, 1);

        assertThat(collectColumn(tEnv, "SELECT CURRENT_WATERMARK(ts) FROM t"))
                .containsExactly(null, LocalDateTime.parse("2024-01-01T10:00:01"));
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> CURRENT_WATERMARK(ts)) FROM t"))
                .containsExactly("+I[[null, null, null]]", "+I[[2024-01-01T10:00:01]]");
        assertThat(
                        collectColumn(
                                tEnv.from("t")
                                        .select(
                                                $("a").arrayTransform(
                                                                x -> currentWatermark($("ts"))))))
                .extracting(value -> Arrays.deepToString((Object[]) value))
                .containsExactly("[null, null, null]", "[2024-01-01T10:00:01]");
        // In a nested body and inside a larger expression
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_FILTER(a,"
                                        + " y -> CURRENT_WATERMARK(ts) IS NULL OR y > x)) FROM t"))
                .containsExactly("+I[[[1, 2, 3], [1, 2, 3], [1, 2, 3]]]", "+I[[[]]]");
        // The watermark is read by the operator once per row, so it cannot be read for a lambda
        // parameter, also not for that of an enclosing lambda.
        final String watermarkOverParameter =
                "CURRENT_WATERMARK over a lambda parameter is not supported in the body of a"
                        + " lambda expression.";
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(ARRAY[ts],"
                                                + " x -> CURRENT_WATERMARK(x)) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(watermarkOverParameter)
                .hasMessageContaining("'x' is a lambda parameter");
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT ARRAY_TRANSFORM(ARRAY[ts], x -> ARRAY_TRANSFORM(a,"
                                                + " y -> CURRENT_WATERMARK(x))) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(watermarkOverParameter)
                .hasMessageContaining("'x' is a lambda parameter");
        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                array($("ts"))
                                                        .arrayTransform(x -> currentWatermark(x))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(watermarkOverParameter);

        // A body that returns a rowtime attribute, also as the accumulator of ARRAY_REDUCE
        assertThat(collectRows(tEnv, "SELECT ARRAY_TRANSFORM(a, x -> ts) FROM t"))
                .containsExactly(
                        "+I[[2024-01-01T10:00:01, 2024-01-01T10:00:01, 2024-01-01T10:00:01]]",
                        "+I[[2024-01-01T10:00:02]]");
        assertThat(
                        collectRows(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> ts + INTERVAL '1' SECOND) FROM t"))
                .containsExactly(
                        "+I[[2024-01-01T10:00:02, 2024-01-01T10:00:02, 2024-01-01T10:00:02]]",
                        "+I[[2024-01-01T10:00:03]]");
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_REDUCE(a, ts,"
                                        + " (acc, x) -> acc + INTERVAL '1' SECOND) FROM t"))
                .containsExactly(
                        LocalDateTime.parse("2024-01-01T10:00:04"),
                        LocalDateTime.parse("2024-01-01T10:00:03"));
        assertThat(collectColumn(tEnv.from("t").select($("a").arrayTransform(x -> $("ts")))))
                .extracting(value -> Arrays.deepToString((Object[]) value))
                .containsExactly(
                        "[2024-01-01T10:00:01, 2024-01-01T10:00:01, 2024-01-01T10:00:01]",
                        "[2024-01-01T10:00:02]");

        // PROCTIME() and a proctime column give every element of a row the same processing time.
        final Instant start = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final List<Object> proctimeCalls =
                collectColumn(tEnv, "SELECT ARRAY_TRANSFORM(a, x -> PROCTIME()) FROM t");
        final List<Object> proctimeColumns =
                collectColumn(tEnv, "SELECT ARRAY_TRANSFORM(a, x -> proc) FROM t");
        final Instant end = Instant.now();
        for (List<Object> arrays : Arrays.asList(proctimeCalls, proctimeColumns)) {
            assertThat(arrays)
                    .hasSize(2)
                    .allSatisfy(
                            value -> {
                                final Object[] elements = (Object[]) value;
                                assertThat(elements).doesNotContainNull();
                                assertThat(new HashSet<>(Arrays.asList(elements))).hasSize(1);
                                assertThat((Instant) elements[0]).isBetween(start, end);
                            });
        }

        // The lifted values survive a compiled plan.
        tEnv.executeSql(
                "CREATE TABLE time_sink (w ARRAY<TIMESTAMP(3)>, p ARRAY<BOOLEAN>) WITH ("
                        + " 'connector' = 'values', 'sink-insert-only' = 'true')");
        final CompiledPlan plan =
                tEnv.compilePlanSql(
                        "INSERT INTO time_sink SELECT ARRAY_TRANSFORM(a, x -> CURRENT_WATERMARK(ts)),"
                                + " ARRAY_TRANSFORM(a, x -> YEAR(CAST(PROCTIME() AS TIMESTAMP(3)))"
                                + " > 2000) FROM t");
        assertThat(plan.asJsonString()).contains("\"cap$wm$0\"", "\"cap$proctime$0\"");
        tEnv.loadPlan(PlanReference.fromJsonString(plan.asJsonString())).execute().await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("time_sink"))
                .containsExactly(
                        "+I[[null, null, null], [true, true, true]]",
                        "+I[[2024-01-01T10:00:01], [true]]");
    }

    /**
     * The current time functions take the time once per row in streaming mode and once per query in
     * batch mode, which the operator does. In a body, they are therefore computed once per row as
     * well, and each element of a row sees the value that the function has outside the body.
     */
    @Test
    void testCurrentTimeFunctionsInLambdaBody() throws Exception {
        for (TableEnvironment tEnv : Arrays.asList(batchEnvironment(), streamingEnvironment())) {
            for (String function :
                    Arrays.asList(
                            "CURRENT_TIMESTAMP",
                            "NOW()",
                            "LOCALTIMESTAMP",
                            "CURRENT_DATE",
                            "CURRENT_TIME",
                            "LOCALTIME",
                            "CURRENT_ROW_TIMESTAMP()")) {
                final String sql =
                        String.format(
                                "SELECT %s, ARRAY_TRANSFORM(a, x -> %s) FROM t",
                                function, function);
                final List<Row> rows = new ArrayList<>();
                try (CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
                    iterator.forEachRemaining(rows::add);
                }
                assertThat(rows)
                        .as(sql)
                        .hasSize(2)
                        .allSatisfy(
                                row ->
                                        assertThat((Object[]) row.getField(1))
                                                .as(sql)
                                                .isNotEmpty()
                                                .doesNotContainNull()
                                                .containsOnly(row.getField(0)));
            }
        }
    }

    private static TableEnvironment streamingEnvironment() {
        return withTable(TableEnvironment.create(EnvironmentSettings.inStreamingMode()));
    }

    private static TableEnvironment deltaJoinEnvironment(DeltaJoinStrategy strategy) {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY, strategy);
        createIndexedTable(
                tEnv,
                "dl",
                Schema.newBuilder()
                        .column("k", DataTypes.INT().notNull())
                        .column("a", DataTypes.ARRAY(DataTypes.INT()))
                        .column("base", DataTypes.INT())
                        .primaryKey("k")
                        .index("k")
                        .immutableColumns("a", "base")
                        .build(),
                Arrays.asList(
                        Row.of(1, new Integer[] {1, 2, 3}, 2),
                        Row.of(2, new Integer[] {5}, 1),
                        Row.of(3, new Integer[] {9}, 0)));
        createIndexedTable(
                tEnv,
                "dr",
                Schema.newBuilder()
                        .column("k", DataTypes.INT().notNull())
                        .column("b", DataTypes.INT())
                        .column("arr", DataTypes.ARRAY(DataTypes.INT()))
                        .primaryKey("k")
                        .index("k")
                        .immutableColumns("b", "arr")
                        .build(),
                Arrays.asList(Row.of(1, 1, new Integer[] {7, 8}), Row.of(2, 9, new Integer[] {1})));
        createIndexedTable(
                tEnv,
                "dm",
                Schema.newBuilder()
                        .column("k", DataTypes.INT().notNull())
                        .column("c", DataTypes.INT())
                        .primaryKey("k")
                        .index("k")
                        .immutableColumns("c")
                        .build(),
                Arrays.asList(Row.of(1, 2), Row.of(2, 0)));
        return tEnv;
    }

    /**
     * An unbounded upsert source whose lookups are asynchronous, as a delta join requires. The
     * primary key and the immutable columns let a cascaded delta join look up the output of the
     * join below it.
     */
    private static void createIndexedTable(
            TableEnvironment tEnv, String name, Schema schema, List<Row> rows) {
        tEnv.createTemporaryTable(
                name,
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .option("bounded", "false")
                        .option("changelog-mode", "I,UA,UB")
                        .option("async", "true")
                        .option("data-id", TestValuesTableFactory.registerData(rows))
                        .build());
    }

    private static String upsertSinkDdl(String name, String valueColumns) {
        return "CREATE TABLE "
                + name
                + " (k INT, "
                + valueColumns
                + ", PRIMARY KEY (k) NOT ENFORCED) WITH ("
                + " 'connector' = 'values', 'bounded' = 'false', 'sink-insert-only' = 'false')";
    }

    private static List<String> insertIntoUpsertSink(
            TableEnvironment tEnv, String sink, String valueColumns, String query)
            throws Exception {
        tEnv.executeSql(upsertSinkDdl(sink, valueColumns));
        tEnv.executeSql("INSERT INTO " + sink + " " + query + " ON CONFLICT DO DEDUPLICATE")
                .await(60, TimeUnit.SECONDS);
        return TestValuesTableFactory.getResultsAsStrings(sink);
    }

    private static TableEnvironment batchEnvironment() {
        return withTable(TableEnvironment.create(EnvironmentSettings.inBatchMode()));
    }

    private static TableEnvironment withTable(TableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE t (id INT, a ARRAY<INT>, base INT, ts TIMESTAMP(3),"
                        + " proc AS PROCTIME(), WATERMARK FOR ts AS ts) WITH ("
                        + " 'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + MAIN_DATA
                        + "')");
        return tEnv;
    }

    private static List<Object> collectColumn(TableEnvironment tEnv, String sql) throws Exception {
        return collectColumn(tEnv.sqlQuery(sql));
    }

    private static List<Object> collectColumn(Table table) throws Exception {
        try (CloseableIterator<Row> iterator = table.execute().collect()) {
            final List<Object> values = new ArrayList<>();
            iterator.forEachRemaining(row -> values.add(row.getField(0)));
            return values;
        }
    }

    private static List<String> collectRows(TableEnvironment tEnv, String sql) throws Exception {
        try (CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            final List<String> rows = new ArrayList<>();
            iterator.forEachRemaining(row -> rows.add(row.toString()));
            return rows;
        }
    }

    /** Applies a changelog to an initially empty table and returns the remaining rows. */
    private static List<String> materialize(TableEnvironment tEnv, String sql) throws Exception {
        final List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            iterator.forEachRemaining(
                    row -> {
                        final RowKind kind = row.getKind();
                        row.setKind(RowKind.INSERT);
                        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                            rows.add(row);
                        } else {
                            rows.remove(row);
                        }
                    });
        }
        return rows.stream().map(Row::toString).collect(Collectors.toList());
    }

    /** A table function that emits each element of an integer array. */
    public static class ExplodeIntsTableFunction extends TableFunction<Integer> {

        public void eval(@DataTypeHint("ARRAY<INT>") List<Integer> values) {
            if (values != null) {
                for (Integer value : values) {
                    collect(value);
                }
            }
        }
    }
}
