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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Plan tests for higher-order functions. They pin the plan shapes that capture lifting produces in
 * the constructs that convert or rewrite a lambda body: projections, filters, join conditions,
 * aggregations, OVER windows, sub-queries and correlates. The printed plans show each lambda as
 * written, with its captures in the body; the plans themselves keep the lifted form. Capture
 * lifting does not depend on the runtime mode, and batch results are covered by {@code
 * HigherOrderFunctionsBatchITCase}.
 */
class HigherOrderFunctionTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T (a ARRAY<INT>, b ARRAY<INT>, base INT, c INT, unused STRING,"
                                + " proctime AS PROCTIME()) WITH ('connector' = 'values')");
        util.tableEnv().executeSql("CREATE TABLE U (k INT, d INT) WITH ('connector' = 'values')");
    }

    @Test
    void testCapturedColumnSurvivesProjectionPushDown() {
        // The source must keep base, which only the lambda body reads.
        util.verifyExecPlan("SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM T");
    }

    @Test
    void testNestedLambdaCapturesOuterParameterAndColumn() {
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(b, y -> x + y + base)) FROM T");
    }

    @Test
    void testExplainRenamesParameterThatHidesCapturedColumn() {
        // The parameter c hides the column c. The plans name columns, so they show the parameter
        // as c_1 to tell it from the column; the syntax tree numbers columns ($3) and needs no
        // renaming.
        util.verifyExecPlan("SELECT ARRAY_TRANSFORM(a, c -> c + T.c) FROM T");
    }

    @Test
    void testLambdasThatDifferOnlyInTheirCapturesAreNotMerged() {
        // After lifting, both bodies read parameters #0 and #1 unless captures are numbered apart.
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY[x, base]),"
                        + " ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y]) FROM T");
    }

    @Test
    void testLambdaInFilter() {
        util.verifyExecPlan(
                "SELECT c FROM T WHERE CARDINALITY(ARRAY_FILTER(a, x -> x > base)) > 0");
    }

    @Test
    void testCaptureInJoinCondition() {
        // The capture of U.d is lifted before the join condition is split, so the lambda stays in
        // the join condition instead of being pushed into an input.
        util.verifyExecPlan(
                "SELECT T.c, U.d FROM T JOIN U ON T.c = U.k"
                        + " AND CARDINALITY(ARRAY_FILTER(T.a, x -> x = U.d)) > 0");
    }

    @Test
    void testAggregateInLambdaBody() {
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(ARRAY_AGG(base), x -> x + SUM(base)) FROM T GROUP BY c");
    }

    @Test
    void testOverWindowInLambdaBody() {
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(a,"
                        + " x -> x + SUM(base) OVER (PARTITION BY c ORDER BY proctime)) FROM T");
    }

    @Test
    void testScalarSubqueryInLambdaBody() {
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(d) FROM U WHERE U.k = T.c))"
                        + " FROM T");
    }

    @Test
    void testCorrelatedSubqueryInLambdaBodyReadsOuterRowTwice() {
        // The sub-query is lifted out of the body, and its two references to T share one
        // correlation, which decorrelation removes.
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(d * T.base) FROM U"
                        + " WHERE U.k = T.c)) FROM T");
    }

    @Test
    void testLambdaInCorrelatedSubqueryCapturesOuterColumn() {
        // The body reads T.base through a correlation variable. It is lifted at conversion into a
        // correlated operand of the call, which decorrelation rewrites like any other operand.
        util.verifyExecPlan(
                "SELECT c, (SELECT COUNT(*) FROM U"
                        + " WHERE CARDINALITY(ARRAY_FILTER(ARRAY[U.d], x -> x > T.base)) > 0)"
                        + " FROM T");
    }

    @Test
    void testUnnestCapturesCorrelation() {
        // UNNEST keeps its correlation, so the capture is lifted from the correlation variable.
        util.verifyExecPlan(
                "SELECT c, v FROM T CROSS JOIN UNNEST(ARRAY_TRANSFORM(a, x -> x + base)) AS u(v)");
    }

    @Test
    void testWatermarkAndTimeAttributesInLambdaBody() {
        // CURRENT_WATERMARK and PROCTIME() are lifted out of the body, so the Calc computes them
        // once per row. A body that returns a time attribute casts its result, so the lambda itself
        // is not converted to a timestamp.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE E (a ARRAY<INT>, ts TIMESTAMP(3), WATERMARK FOR ts AS ts)"
                                + " WITH ('connector' = 'values')");
        util.verifyExecPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> CURRENT_WATERMARK(ts)),"
                        + " ARRAY_TRANSFORM(a, x -> ts), ARRAY_TRANSFORM(a, x -> PROCTIME()) FROM E");
    }
}
