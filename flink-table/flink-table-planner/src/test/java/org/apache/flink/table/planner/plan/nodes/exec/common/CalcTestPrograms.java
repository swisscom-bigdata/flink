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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc1;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc2;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.UdfWithOpen;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/** {@link TableTestProgram}s for testing {@link StreamExecCalc} and {@link BatchExecCalc}. */
public class CalcTestPrograms {

    private static final VariantBuilder VARIANT_BUILDER = Variant.newBuilder();

    // --------------------------------------------------------------------------------------------
    // With restore data
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram SIMPLE_CALC =
            TableTestProgram.of("calc-simple", "validates basic calc node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .producedBeforeRestore(Row.of(420L, 42.0))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .consumedBeforeRestore(Row.of(421L, 42.0))
                                    .consumedAfterRestore(Row.of(422L, 42.1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a + 1, b FROM t")
                    .build();

    // The higher-order programs below pin one compatibility surface: the serialized lambda
    // representation ("kind": "LAMBDA" / "LAMBDA_REF" and the lifted capture indices), which is
    // written identically for every built-in that takes a lambda and for user-defined ones. They
    // are per function only because they were written one per function; the coverage they provide
    // is not. A new higher-order built-in therefore needs no program here -- it needs semantic and
    // validation cases, which HigherOrderFunctionCoverageTest requires. Add a program here only
    // when the persisted representation itself changes, for instance a new lambda shape that
    // serializes differently.
    public static final TableTestProgram CALC_ARRAY_TRANSFORM =
            TableTestProgram.of(
                            "calc-array-transform",
                            "validates calc node with the ARRAY_TRANSFORM higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a ARRAY<INT>")
                                    .producedBeforeRestore(Row.of((Object) new Integer[] {1, 2, 3}))
                                    .producedAfterRestore(Row.of((Object) new Integer[] {4, 5}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a ARRAY<INT>")
                                    .consumedBeforeRestore(
                                            Row.of((Object) new Integer[] {10, 20, 30}))
                                    .consumedAfterRestore(Row.of((Object) new Integer[] {40, 50}))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT ARRAY_TRANSFORM(a, x -> x * 10) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_ARRAY_FILTER =
            TableTestProgram.of(
                            "calc-array-filter",
                            "validates calc node with the ARRAY_FILTER higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a ARRAY<INT>")
                                    .producedBeforeRestore(
                                            Row.of((Object) new Integer[] {1, 2, 3, 4}))
                                    .producedAfterRestore(Row.of((Object) new Integer[] {5, 1}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a ARRAY<INT>")
                                    .consumedBeforeRestore(Row.of((Object) new Integer[] {3, 4}))
                                    .consumedAfterRestore(Row.of((Object) new Integer[] {5}))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT ARRAY_FILTER(a, x -> x > 2) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_ARRAY_REDUCE =
            TableTestProgram.of(
                            "calc-array-reduce",
                            "validates calc node with the ARRAY_REDUCE higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a ARRAY<INT>")
                                    .producedBeforeRestore(Row.of((Object) new Integer[] {1, 2, 3}))
                                    .producedAfterRestore(Row.of((Object) new Integer[] {4, 5}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(Row.of(6))
                                    .consumedAfterRestore(Row.of(9))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t "
                                    + "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_ARRAY_TRANSFORM_CAPTURE =
            TableTestProgram.of(
                            "calc-array-transform-capture",
                            "validates calc node with a higher-order function whose lambda captures an outer column")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a ARRAY<INT>", "base INT")
                                    .producedBeforeRestore(Row.of(new Integer[] {1, 2, 3}, 10))
                                    .producedAfterRestore(Row.of(new Integer[] {4, 5}, 100))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a ARRAY<INT>")
                                    .consumedBeforeRestore(
                                            Row.of((Object) new Integer[] {11, 12, 13}))
                                    .consumedAfterRestore(Row.of((Object) new Integer[] {104, 105}))
                                    .build())
                    // the capture is lifted into a trailing lambda parameter (cap$0) and an
                    // additional call operand, which must survive plan (de)serialization
                    .runSql(
                            "INSERT INTO sink_t SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_ARRAY_ZIP_WITH =
            TableTestProgram.of(
                            "calc-array-zip-with",
                            "validates calc node with the ARRAY_ZIP_WITH higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a ARRAY<INT>", "b ARRAY<INT>")
                                    .producedBeforeRestore(
                                            Row.of(new Integer[] {1, 2, 3}, new Integer[] {10, 20}))
                                    .producedAfterRestore(
                                            Row.of(
                                                    new Integer[] {4, 5},
                                                    new Integer[] {40, 50, 60}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a ARRAY<INT>")
                                    .consumedBeforeRestore(
                                            Row.of((Object) new Integer[] {11, 22, 3}))
                                    .consumedAfterRestore(
                                            Row.of((Object) new Integer[] {44, 55, 60}))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT ARRAY_ZIP_WITH(a, b, (x, y) -> "
                                    + "COALESCE(x, 0) + COALESCE(y, 0)) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_MAP_FILTER =
            TableTestProgram.of(
                            "calc-map-filter",
                            "validates calc node with the MAP_FILTER higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .producedBeforeRestore(Row.of(map("a", 1, "b", 2)))
                                    .producedAfterRestore(Row.of(map("c", 3, "d", 0)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .consumedBeforeRestore(Row.of(map("b", 2)))
                                    .consumedAfterRestore(Row.of(map("c", 3)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT MAP_FILTER(m, (k, v) -> v > 1) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_MAP_TRANSFORM_KEYS =
            TableTestProgram.of(
                            "calc-map-transform-keys",
                            "validates calc node with the MAP_TRANSFORM_KEYS higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .producedBeforeRestore(Row.of(map("a", 1, "b", 2)))
                                    .producedAfterRestore(Row.of(map("c", 3)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .consumedBeforeRestore(Row.of(map("a!", 1, "b!", 2)))
                                    .consumedAfterRestore(Row.of(map("c!", 3)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT MAP_TRANSFORM_KEYS(m, (k, v) -> k || '!') "
                                    + "FROM source_t")
                    .build();

    public static final TableTestProgram CALC_MAP_TRANSFORM_VALUES =
            TableTestProgram.of(
                            "calc-map-transform-values",
                            "validates calc node with the MAP_TRANSFORM_VALUES higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .producedBeforeRestore(Row.of(map("a", 1, "b", 2)))
                                    .producedAfterRestore(Row.of(map("c", 3)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .consumedBeforeRestore(Row.of(map("a", 10, "b", 20)))
                                    .consumedAfterRestore(Row.of(map("c", 30)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT MAP_TRANSFORM_VALUES(m, (k, v) -> v * 10) "
                                    + "FROM source_t")
                    .build();

    public static final TableTestProgram CALC_MAP_ZIP_WITH =
            TableTestProgram.of(
                            "calc-map-zip-with",
                            "validates calc node with the MAP_ZIP_WITH higher-order function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("m1 MAP<STRING, INT>", "m2 MAP<STRING, INT>")
                                    .producedBeforeRestore(
                                            Row.of(map("a", 1, "b", 2), map("a", 10, "c", 30)))
                                    .producedAfterRestore(Row.of(map("x", 5), map("x", 1, "y", 2)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("m MAP<STRING, INT>")
                                    .consumedBeforeRestore(Row.of(map("a", 11, "b", 2, "c", 30)))
                                    .consumedAfterRestore(Row.of(map("x", 6, "y", 2)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> "
                                    + "COALESCE(v1, 0) + COALESCE(v2, 0)) FROM source_t")
                    .build();

    private static Map<String, Integer> map(Object... keyValues) {
        final Map<String, Integer> result = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            result.put((String) keyValues[i], (Integer) keyValues[i + 1]);
        }
        return result;
    }

    public static final TableTestProgram CALC_PROJECT_PUSHDOWN =
            TableTestProgram.of(
                            "calc-project-pushdown", "validates calc node with project pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(421L, 42.1))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "a1 VARCHAR")
                                    .consumedBeforeRestore(Row.of(421L, "421"))
                                    .consumedAfterRestore(Row.of(421L, "421"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, CAST(a AS VARCHAR) FROM source_t WHERE a > CAST(1 AS BIGINT)")
                    .build();

    public static final TableTestProgram CALC_FILTER =
            TableTestProgram.of("calc-filter", "validates calc node with filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b INT", "c DOUBLE", "d VARCHAR")
                                    .producedBeforeRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .producedAfterRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b INT", "c DOUBLE", "d VARCHAR")
                                    .consumedBeforeRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .consumedAfterRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t WHERE b > 0")
                    .build();

    public static final TableTestProgram CALC_FILTER_PUSHDOWN =
            TableTestProgram.of("calc-filter-pushdown", "validates calc node with filter pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(421L, 42.1))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .consumedBeforeRestore(Row.of(421L, 42.1))
                                    .consumedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t WHERE a > CAST(420 AS BIGINT)")
                    .build();

    public static final TableTestProgram CALC_SARG =
            TableTestProgram.of("calc-sarg", "validates calc node with Sarg")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(1))
                                    .producedAfterRestore(Row.of(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(Row.of(1))
                                    .consumedAfterRestore(Row.of(1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a FROM source_t WHERE a = 1 or a = 2 or a is null")
                    .build();

    public static final TableTestProgram CALC_UDF_SIMPLE =
            TableTestProgram.of("calc-udf-simple", "validates calc node with simple UDF")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .producedBeforeRestore(Row.of(5))
                                    .producedAfterRestore(Row.of(5))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "a1 BIGINT")
                                    .consumedBeforeRestore(Row.of(5, 6L))
                                    .consumedAfterRestore(Row.of(5, 6L))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, udf1(a) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_UDF_COMPLEX =
            TableTestProgram.of("calc-udf-complex", "validates calc node with complex UDFs")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .setupTemporaryCatalogFunction("udf2", JavaFunc1.class)
                    .setupTemporarySystemFunction("udf3", JavaFunc2.class)
                    .setupTemporarySystemFunction("udf4", UdfWithOpen.class)
                    .setupCatalogFunction("udf5", JavaFunc5.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a BIGINT, b INT NOT NULL, c VARCHAR, d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    5L,
                                                    11,
                                                    "hello world",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 1, 123)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    5L,
                                                    11,
                                                    "hello world",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 1, 123)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a BIGINT",
                                            "a1 VARCHAR",
                                            "b INT NOT NULL",
                                            "b1 VARCHAR",
                                            "c1 VARCHAR",
                                            "c2 VARCHAR",
                                            "d1 TIMESTAMP(3)")
                                    .consumedBeforeRestore(
                                            Row.of(
                                                    5L,
                                                    "5",
                                                    11,
                                                    "11 and 11 and 1702688461000",
                                                    "hello world11",
                                                    "$hello",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 0, 0)))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    5L,
                                                    "5",
                                                    11,
                                                    "11 and 11 and 1702688461000",
                                                    "hello world11",
                                                    "$hello",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 0, 0)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "a, "
                                    + "cast(a as VARCHAR) as a1, "
                                    + "b, "
                                    + "udf2(b, b, d) as b1, "
                                    + "udf3(c, b) as c1, "
                                    + "udf4(substring(c, 1, 5)) as c2, "
                                    + "udf5(d, 1000) as d1 "
                                    + "from source_t where "
                                    + "(udf1(a) > 0 or (a * b) < 100) and b > 10")
                    .build();

    public static final TableTestProgram CALC_CURRENT_TIMESTAMP =
            TableTestProgram.of(
                            "calc-current-timestamp", "validates basic calc with current timestamp")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("a BIGINT")
                                    .producedBeforeRestore(Row.of(100L))
                                    .producedAfterRestore(Row.of(10000L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT")
                                    .consumedBeforeRestore(Row.of(20L))
                                    .consumedAfterRestore(Row.of(0L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT extract(year from current_timestamp) / a FROM t")
                    .build();

    public static final TableTestProgram CALC_VARIANT =
            TableTestProgram.of("calc-variant", "validates calc node with VARIANT type")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("s STRING", "v VARIANT")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "{\"a\":1}",
                                                    VARIANT_BUILDER
                                                            .object()
                                                            .add("k", VARIANT_BUILDER.of(1))
                                                            .build()))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "{\"a\":2}",
                                                    VARIANT_BUILDER
                                                            .object()
                                                            .add("k", VARIANT_BUILDER.of(2))
                                                            .build()))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "parsed VARIANT", "try_parsed VARIANT", "field VARIANT")
                                    .consumedBeforeRestore("+I[{\"a\":1}, {\"a\":1}, 1]")
                                    .consumedAfterRestore("+I[{\"a\":2}, {\"a\":2}, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT PARSE_JSON(s), TRY_PARSE_JSON(s), v['k'] FROM t")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Without restore data
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram CURRENT_WATERMARK =
            TableTestProgram.of(
                            "calc-current-watermark", "validates the CURRENT_WATERMARK function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "WATERMARK FOR ts AS ts")
                                    .producedValues(
                                            Row.of("Bob", Instant.ofEpochMilli(0)),
                                            Row.of("Bob", Instant.ofEpochMilli(1)),
                                            Row.of("Alice", Instant.ofEpochMilli(2)),
                                            Row.of("Bob", Instant.ofEpochMilli(3)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "name STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "w TIMESTAMP_LTZ(3)")
                                    .consumedValues(
                                            "+I[Bob, 1970-01-01T00:00:00Z, null]",
                                            "+I[Bob, 1970-01-01T00:00:00.001Z, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, 1970-01-01T00:00:00.002Z, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, 1970-01-01T00:00:00.003Z, 1970-01-01T00:00:00.002Z]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT name, ts, CURRENT_WATERMARK(ts) AS w FROM t")
                    .build();

    public static final TableTestProgram COALESCE_NESTED_ROW_LEFT_JOIN =
            TableTestProgram.of(
                            "calc-coalesce-nested-row-left-join",
                            "validates coalesce on nested ROW field from LEFT JOIN")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "`order_id` BIGINT NOT NULL",
                                            "`amount` DOUBLE",
                                            "PRIMARY KEY (`order_id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L, 10.0), Row.of(2L, 20.0))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("order_details_row")
                                    .addSchema(
                                            "`r` ROW<`order_id` BIGINT NOT NULL, `name` STRING NOT NULL> NOT NULL",
                                            "`detail` STRING",
                                            "PRIMARY KEY (`r`) NOT ENFORCED")
                                    .producedValues(Row.of(Row.of(1L, "first"), "d1"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("coalesce_sink")
                                    .addSchema("order_id_str STRING")
                                    .testMaterializedData()
                                    .consumedValues("+I[1]", "+I[2]")
                                    .build())
                    .runSql(
                            "INSERT INTO coalesce_sink "
                                    + "SELECT CAST(COALESCE(b.r.order_id, a.order_id) AS STRING) AS order_id_str "
                                    + "FROM orders a LEFT JOIN order_details_row b "
                                    + "ON a.order_id = b.r.order_id")
                    .build();

    public static final TableTestProgram COALESCE =
            TableTestProgram.of("calc-coalesce", "validates coalesce node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "a DECIMAL(2, 1)",
                                            "b DECIMAL(4, 2)",
                                            "c TIMESTAMP(0)",
                                            "d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    null,
                                                    new BigDecimal("11.22"),
                                                    null,
                                                    LocalDateTime.of(
                                                            1970, 1, 1, 0, 0, 0, 123_000_000)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    new BigDecimal("5.3"),
                                                    null,
                                                    LocalDateTime.of(2000, 2, 2, 2, 2, 2),
                                                    null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("x DECIMAL(4, 2)", "y TIMESTAMP(3)")
                                    .consumedBeforeRestore(
                                            Row.of(
                                                    new BigDecimal("11.22"),
                                                    LocalDateTime.of(
                                                            1970, 1, 1, 0, 0, 0, 123_000_000)))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    new BigDecimal("5.30"),
                                                    LocalDateTime.of(2000, 2, 2, 2, 2, 2)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT COALESCE(a, b) AS x, COALESCE(c, d) AS y FROM t")
                    .build();
}
