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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Built-in functions in a lambda body. A body is compiled into a class of its own, apart from the
 * operator, so each built-in function must give the same result in a body as outside a lambda.
 */
class BuiltInFunctionsInLambdaBodyITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    /**
     * A built-in function call, with the column whose value it is applied to marked by {@code @}.
     */
    private static final String[][] BUILT_IN_EXPRESSIONS = {
        {"s", "UPPER(@)"},
        {"s", "@ || '!'"},
        {"s", "SUBSTRING(@, 2, 3)"},
        {"s", "REGEXP_REPLACE(@, 'l+', 'L')"},
        {"s", "REGEXP_EXTRACT(@, '(W)(o)', 2)"},
        {"s", "REGEXP_EXTRACT_ALL(@, '(l)', 1)"},
        {"s", "@ LIKE 'He%'"},
        {"s", "@ SIMILAR TO 'H%'"},
        {"s", "SPLIT_INDEX(@, ' ', 1)"},
        {"s", "SPLIT(@, ' ')"},
        {"s", "LPAD(@, 15, '*')"},
        {"s", "INITCAP(LOWER(@))"},
        {"s", "POSITION('o' IN @)"},
        {"s", "OVERLAY(@ PLACING 'xx' FROM 2 FOR 3)"},
        {"s", "TRANSLATE(@, 'lo', 'LO')"},
        {"s", "PRINTF('%s!', @)"},
        {"s", "MD5(@)"},
        {"s", "SHA2(@, 512)"},
        {"s", "TO_BASE64(@)"},
        {"s", "URL_ENCODE(@)"},
        {"s", "STR_TO_MAP(@ || '=1,b=2')"},
        {"s", "PARSE_URL('http://h/p?q=' || @, 'QUERY')"},
        {"s", "JSON_QUOTE(@)"},
        {"s", "@ IN ('a', 'Hello World')"},
        {"s", "@ NOT IN ('a', 'b')"},
        {"i", "MOD(@, 2)"},
        {"i", "POWER(@, 2)"},
        {"i", "ROUND(@ / 7.0, 2)"},
        {"i", "BIN(@)"},
        {"i", "CHR(@ + 64)"},
        {"i", "TRY_CAST(CAST(@ AS STRING) || 'a' AS INT)"},
        {"i", "CASE WHEN @ > 2 THEN 'big' ELSE 'small' END"},
        {"i", "NULLIF(@, 3)"},
        {"i", "GREATEST(@, 5, 1)"},
        {"i", "@ BETWEEN 1 AND 5"},
        {"i", "@ IS DISTINCT FROM 3"},
        {"i", "@ IN (1, 2, 3)"},
        {"i", "@ IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)"},
        {"i", "@ IN (i, 5)"},
        {"i", "ARRAY_FILTER(arr, y -> y IN (@, 3))"},
        {"i", "ARRAY[@, @ * 2]"},
        {"i", "MAP['k', @]"},
        {"i", "ROW(@, 'a')"},
        {"i", "ARRAY_CONTAINS(arr, @)"},
        {"i", "ARRAY_POSITION(arr, @)"},
        {"i", "ARRAY_REMOVE(arr, @)"},
        {"i", "ARRAY_DISTINCT(ARRAY[@, @])"},
        {"i", "ARRAY_SORT(ARRAY[@, 1])"},
        {"i", "ARRAY_UNION(arr, ARRAY[@])"},
        {"i", "ARRAY_EXCEPT(arr, ARRAY[@])"},
        {"i", "ARRAY_JOIN(ARRAY[CAST(@ AS STRING), 'a'], '-')"},
        {"i", "MAP_FROM_ARRAYS(ARRAY['a'], ARRAY[@])"},
        {"i", "JSON_OBJECT('k' VALUE JSON_ARRAY(@))"},
        {"i", "JSON_STRING(@)"},
        {"i", "FROM_UNIXTIME(@)"},
        {"i", "CURRENT_DATABASE()"},
        {"i", "ARRAY_TRANSFORM(ARRAY[@], y -> y * @)"},
        {"j", "JSON_VALUE(@, '$.a' RETURNING INT)"},
        {"j", "JSON_VALUE(@, '$.z' DEFAULT 'd' ON EMPTY)"},
        {"j", "JSON_QUERY(@, '$.b')"},
        {"j", "JSON_EXISTS(@, '$.a')"},
        {"j", "@ IS JSON"},
        {"j", "PARSE_JSON(@)"},
        {"d", "@ * 2"},
        {"d", "ROUND(@, 1)"},
        {"d", "CAST(@ AS INT)"},
        {"ts", "DATE_FORMAT(@, 'yyyy-MM')"},
        {"ts", "EXTRACT(YEAR FROM @)"},
        {"ts", "TIMESTAMPADD(DAY, 1, @)"},
        {"ts", "FLOOR(@ TO HOUR)"},
        {"ts", "@ + INTERVAL '1' DAY"},
        {"ts", "CAST(@ AS DATE)"},
        {"ts", "TO_TIMESTAMP(DATE_FORMAT(@, 'yyyy-MM-dd HH:mm:ss'))"},
        {"ts", "UNIX_TIMESTAMP(DATE_FORMAT(@, 'yyyy-MM-dd HH:mm:ss'))"},
        {"ts", "CONVERT_TZ(DATE_FORMAT(@, 'yyyy-MM-dd HH:mm:ss'), 'UTC', 'Asia/Shanghai')"},
        {"dt", "@ + INTERVAL '1' MONTH"},
        {"dt", "DAYOFMONTH(@)"},
        {"b", "NOT @"},
        {"b", "CASE @ WHEN TRUE THEN 1 END"},
        {"r", "@.f"},
        {"r", "@.g || 'y'"},
        {"m", "@['k']"},
        {"m", "MAP_ENTRIES(@)"},
        {"arr", "@[1]"},
        {"arr", "ARRAY_FILTER(@, y -> y > 1)"},
        {"arr", "ARRAY_REDUCE(@, 0, (acc, y) -> acc + y)"},
        {"arr", "ARRAY_MIN(@)"},
        {"arr", "@ = ARRAY[1, 2, 3]"},
    };

    /**
     * Selects each call next to the same call in a body, applied to the same value, once for a row
     * of values and once for a row of NULLs. Some of the calls need members, initialization or
     * evaluators of their own in the generated code.
     */
    @Test
    void testBuiltInFunctionsGiveTheirPlainResults() throws Exception {
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(
                                        3,
                                        "Hello World",
                                        new BigDecimal("12.34"),
                                        LocalDateTime.parse("2024-01-01T10:20:30.123"),
                                        "{\"a\":1,\"b\":[1,2]}",
                                        true,
                                        new Integer[] {1, 2, 3},
                                        Map.of("k", 1),
                                        Row.of(7, "x"),
                                        LocalDate.parse("2024-01-15")),
                                Row.of(
                                        null, null, null, null, null, null, null, null, null,
                                        null)));
        final List<String> columns = new ArrayList<>();
        for (String[] expression : BUILT_IN_EXPRESSIONS) {
            columns.add(expression[1].replace("@", expression[0]));
            columns.add(
                    String.format(
                            "ARRAY_TRANSFORM(ARRAY[%s], x -> %s)[1]",
                            expression[0], expression[1].replace("@", "x")));
        }
        for (EnvironmentSettings settings :
                Arrays.asList(
                        EnvironmentSettings.inBatchMode(), EnvironmentSettings.inStreamingMode())) {
            final TableEnvironment tEnv = TableEnvironment.create(settings);
            tEnv.executeSql(
                    "CREATE TABLE v (i INT, s STRING, d DECIMAL(10, 2), ts TIMESTAMP(3), j STRING,"
                            + " b BOOLEAN, arr ARRAY<INT>, m MAP<STRING, INT>,"
                            + " r ROW<f INT, g STRING>, dt DATE) WITH ('connector' = 'values',"
                            + " 'bounded' = 'true', 'data-id' = '"
                            + dataId
                            + "')");
            final List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> iterator =
                    tEnv.sqlQuery("SELECT " + String.join(", ", columns) + " FROM v")
                            .execute()
                            .collect()) {
                iterator.forEachRemaining(rows::add);
            }
            assertThat(rows).hasSize(2);
            for (Row row : rows) {
                for (int k = 0; k < BUILT_IN_EXPRESSIONS.length; k++) {
                    assertThat(Arrays.deepToString(new Object[] {row.getField(2 * k + 1)}))
                            .as("%s in a lambda body", BUILT_IN_EXPRESSIONS[k][1])
                            .isEqualTo(Arrays.deepToString(new Object[] {row.getField(2 * k)}));
                }
            }
        }
    }
}
