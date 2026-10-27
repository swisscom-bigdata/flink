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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ExtractLambdaOverWindowRule}. Capture lifting has already turned each OVER window
 * in a lambda body into an operand of the higher-order call; the rule moves it into a projection
 * below.
 */
class ExtractLambdaOverWindowRuleTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        final CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(
                                        HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(RuleSets.ofList(ExtractLambdaOverWindowRule.INSTANCE))
                                .build());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T (a ARRAY<INT>, b INT, c INT)"
                                + " WITH ('connector' = 'values', 'bounded' = 'true')");
    }

    @Test
    void testOverWindowInLambdaBody() {
        util.verifyRelPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(b) OVER (PARTITION BY c)) FROM T");
    }

    @Test
    void testOverWindowInNestedLambdaBody() {
        // Lifting threads the window through both lambdas, so only the outer call carries it.
        util.verifyRelPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(a, y -> x + y + MAX(b) OVER ()))"
                        + " FROM T");
    }

    @Test
    void testSameOverWindowInTwoCallsIsExtractedOnce() {
        util.verifyRelPlan(
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(b) OVER ()),"
                        + " ARRAY_FILTER(a, x -> x > SUM(b) OVER ()),"
                        + " ARRAY_FILTER(a, x -> x > MAX(b) OVER ()) FROM T");
    }

    @Test
    void testOverWindowOutsideLambdaIsNotMoved() {
        util.verifyRelPlan("SELECT SUM(b) OVER (), ARRAY_TRANSFORM(a, x -> x + c) FROM T");
    }
}
