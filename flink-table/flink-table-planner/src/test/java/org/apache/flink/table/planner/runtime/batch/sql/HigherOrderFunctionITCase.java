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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case that executes SQL queries with a lambda (a value of the {@code FUNCTION} type) passed to
 * the higher-order functions {@code ARRAY_FILTER(array, element -> predicate)} and {@code
 * TRANSFORM(collection, lambda)}.
 *
 * <p>This also covers views defined on top of lambda SQL, which forces the query to go through SQL
 * expansion (re-unparsing of the original SQL) before it is planned again on {@code SELECT}. This
 * verifies that lambda expressions survive the expand/rewrite round-trip.
 */
class HigherOrderFunctionITCase {

    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    }

    @Test
    void testArrayFilterOverColumn() {
        final TableResult result =
                tEnv.executeSql(
                        "SELECT id, ARRAY_FILTER(vals, x -> x > 2) "
                                + "FROM (VALUES "
                                + "  (1, ARRAY[1, 2, 3, 4]), "
                                + "  (2, ARRAY[0, 1]), "
                                + "  (3, CAST(NULL AS ARRAY<INT>))"
                                + ") AS t(id, vals)");

        final List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, new Integer[] {3, 4}),
                        Row.of(2, new Integer[] {}),
                        Row.of(3, null));
    }

    @Test
    void testArrayTransformOverColumn() {
        final TableResult result =
                tEnv.executeSql(
                        "SELECT id, TRANSFORM(vals, x -> x * 10) "
                                + "FROM (VALUES "
                                + "  (1, ARRAY[1, 2, 3]), "
                                + "  (2, ARRAY[5])"
                                + ") AS t(id, vals)");

        final List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, new Integer[] {10, 20, 30}), Row.of(2, new Integer[] {50}));
    }

    @Test
    void testArrayTransformChangingElementType() {
        final TableResult result =
                tEnv.executeSql("SELECT TRANSFORM(ARRAY['a', 'bb', 'ccc'], s -> CHAR_LENGTH(s))");

        final List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).containsExactly(Row.of((Object) new Integer[] {1, 2, 3}));
    }

    @Test
    void testMapTransformValues() {
        final TableResult result =
                tEnv.executeSql("SELECT TRANSFORM(MAP['a', 1, 'b', 2], (k, v) -> v * 100)");

        final List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        @SuppressWarnings("unchecked")
        final Map<String, Integer> transformed = (Map<String, Integer>) rows.get(0).getField(0);
        assertThat(transformed).containsEntry("a", 100).containsEntry("b", 200);
    }

    @Test
    void testArrayFilterInViewIsExpandable() {
        tEnv.executeSql(
                "CREATE VIEW filtered_view AS "
                        + "SELECT id, ARRAY_FILTER(vals, x -> x > 2) AS filtered "
                        + "FROM (VALUES "
                        + "  (1, ARRAY[1, 2, 3, 4]), "
                        + "  (2, ARRAY[0, 1]), "
                        + "  (3, CAST(NULL AS ARRAY<INT>))"
                        + ") AS t(id, vals)");

        final List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT id, filtered FROM filtered_view").collect());

        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, new Integer[] {3, 4}),
                        Row.of(2, new Integer[] {}),
                        Row.of(3, null));
    }

    @Test
    void testArrayTransformInViewIsExpandable() {
        tEnv.executeSql(
                "CREATE VIEW transformed_view AS "
                        + "SELECT id, TRANSFORM(vals, x -> x * 10) AS transformed "
                        + "FROM (VALUES "
                        + "  (1, ARRAY[1, 2, 3]), "
                        + "  (2, ARRAY[5])"
                        + ") AS t(id, vals)");

        final List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT id, transformed FROM transformed_view").collect());

        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, new Integer[] {10, 20, 30}), Row.of(2, new Integer[] {50}));
    }

    @Test
    void testMapTransformInViewIsExpandable() {
        tEnv.executeSql(
                "CREATE VIEW map_transformed_view AS "
                        + "SELECT TRANSFORM(MAP['a', 1, 'b', 2], (k, v) -> v * 100) AS transformed");

        final List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("SELECT transformed FROM map_transformed_view").collect());

        assertThat(rows).hasSize(1);
        @SuppressWarnings("unchecked")
        final Map<String, Integer> transformed = (Map<String, Integer>) rows.get(0).getField(0);
        assertThat(transformed).containsEntry("a", 100).containsEntry("b", 200);
    }
}
