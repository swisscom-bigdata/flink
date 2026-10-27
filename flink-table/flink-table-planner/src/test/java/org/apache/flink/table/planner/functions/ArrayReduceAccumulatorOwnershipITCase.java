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
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the accumulator {@code ARRAY_REDUCE} carries between applications and returns is owned
 * by the function, not by the reducer body or the caller.
 *
 * <p>For an empty array the reducer is never applied, so {@code ARRAY_REDUCE} returns the initial
 * accumulator argument by reference rather than a copy. These tests pin that this reference remains
 * valid for the operators that consume it, in both batch mode (where object reuse is enabled) and
 * streaming mode.
 *
 * <p>For a non-empty array the reducer body may build its result in a buffer that the compiled body
 * reuses for every application, so {@code ARRAY_REDUCE} copies each result before passing it back
 * as the accumulator.
 */
class ArrayReduceAccumulatorOwnershipITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    /**
     * {@code ARRAY_FILTER(arr, x -> FALSE)} is the only way to obtain an empty array here, because
     * an empty array literal has no inferrable element type.
     */
    private static final String QUERY =
            "SELECT b.id, b.acc FROM ("
                    + "  SELECT id,"
                    + "         ARRAY_REDUCE("
                    + "             ARRAY_FILTER(arr, x -> FALSE),"
                    + "             acc,"
                    + "             (a, e) -> ARRAY_CONCAT(a, ARRAY[e])) AS acc"
                    + "  FROM buffered"
                    + ") b JOIN probe p ON b.id = p.id";

    private static TableEnvironment tableEnvironment(EnvironmentSettings settings) {
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        final Table buffered =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                DataTypes.FIELD("acc", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("arr", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(1, new Integer[] {10}, new Integer[] {1}),
                        Row.of(2, new Integer[] {20}, new Integer[] {2}),
                        Row.of(3, new Integer[] {30}, new Integer[] {3}));
        tEnv.createTemporaryView("buffered", buffered);
        final Table probe =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT().notNull())),
                        Row.of(1),
                        Row.of(2),
                        Row.of(3));
        tEnv.createTemporaryView("probe", probe);
        return tEnv;
    }

    private static void assertBufferedAccumulators(EnvironmentSettings settings) throws Exception {
        final TableEnvironment tEnv = tableEnvironment(settings);
        try (final CloseableIterator<Row> result = tEnv.sqlQuery(QUERY).execute().collect()) {
            final List<Row> rows = CollectionUtil.iteratorToList(result);
            assertThat(rows)
                    .containsExactlyInAnyOrder(
                            Row.of(1, new Integer[] {10}),
                            Row.of(2, new Integer[] {20}),
                            Row.of(3, new Integer[] {30}));
        }
    }

    @Test
    void testBatchJoinBuffersTheInitialAccumulator() throws Exception {
        assertBufferedAccumulators(EnvironmentSettings.inBatchMode());
    }

    @Test
    void testStreamingJoinBuffersTheInitialAccumulator() throws Exception {
        assertBufferedAccumulators(EnvironmentSettings.inStreamingMode());
    }

    @Test
    void testAccumulatorIsDetachedFromTheReducerBuffer() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("arr", DataTypes.ARRAY(DataTypes.INT().notNull()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        // Each body has exactly the accumulator type, so it is not wrapped in a CAST and its result
        // is the ARRAY or ROW that the compiled body updates in place, element by element. The
        // body reads the old accumulator in swapped order, so if the accumulator still were that
        // buffer, the second element would read the first one after it had been overwritten:
        // (0, 0) -> (0, 1) -> (1, 2) -> (2, 4) would become (0, 1) -> (1, 3) -> (3, 6).
        final String query =
                "SELECT"
                        + " ARRAY_REDUCE(arr, CAST(ARRAY[0, 0] AS ARRAY<INT>),"
                        + "     (acc, x) -> ARRAY[acc[2], acc[1] + x]),"
                        + " ARRAY_REDUCE(arr, ROW(0, 0),"
                        + "     (acc, x) -> ROW(acc.EXPR$1, acc.EXPR$0 + x))"
                        + " FROM t";
        try (final CloseableIterator<Row> result = tEnv.sqlQuery(query).execute().collect()) {
            assertThat(CollectionUtil.iteratorToList(result))
                    .containsExactly(Row.of(new Integer[] {2, 4}, Row.of(2, 4)));
        }
    }
}
