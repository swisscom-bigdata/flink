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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.functions.utils.HigherOrderFunctionUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SqlToRelConverter} that lifts the captures of every lambda in a join condition before
 * the condition is rewritten against the join inputs.
 *
 * <p>{@code RelBuilder#join} and {@code #asofJoin} push a single-sided predicate, or an equi-join
 * operand, down into the input it references, and the correlated branch of {@link #createJoin}
 * rewrites references to the left input. Capture lifting otherwise only runs after conversion
 * ({@code FlinkPlannerImpl#liftHigherOrderFunctionCaptures}), and on Calcite 1.41.0 neither {@code
 * RelOptUtil.InputFinder} (a {@code RexVisitorImpl}, whose {@code visitLambda} does not descend)
 * nor the rewriting {@code RexShuttle}s (whose {@code visitLambda} discards the rewritten body) see
 * a column that is captured inside a lambda body. A predicate capturing the other input would be
 * pushed into one input and fail, and one capturing a column of the right input would be pushed
 * without shifting the capture and silently read the wrong column. A lifted capture is an ordinary
 * trailing call operand that both see.
 *
 * <p><b>Upstream status.</b> CALCITE-7497 (fixVersion 1.42.0) and CALCITE-6242 (fixVersion 1.43.0)
 * fix the two traversals upstream, which makes this class redundant after upgrading Calcite to
 * 1.43.0.
 */
@Internal
public class FlinkSqlToRelConverter extends SqlToRelConverter {

    public FlinkSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            @Nullable SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    protected RelNode createJoin(
            Blackboard bb,
            RelNode leftRel,
            RelNode rightRel,
            RexNode joinCond,
            JoinRelType joinType) {
        return super.createJoin(bb, leftRel, rightRel, liftCaptures(joinCond), joinType);
    }

    @Override
    protected RelNode createAsofJoin(
            SqlParserPos pos,
            Blackboard bb,
            RelNode leftRel,
            RelNode rightRel,
            RexNode joinCond,
            RexNode matchCondition,
            JoinRelType joinType) {
        return super.createAsofJoin(
                pos, bb, leftRel, rightRel, liftCaptures(joinCond), matchCondition, joinType);
    }

    private RexNode liftCaptures(RexNode joinCond) {
        return joinCond.accept(HigherOrderFunctionUtil.captureLiftingShuttle(rexBuilder));
    }
}
