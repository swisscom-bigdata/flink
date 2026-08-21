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

import org.apache.flink.table.planner.functions.utils.HigherOrderFunctionUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that moves the OVER windows captured by a higher-order function's lambda into a
 * projection below the call, leaving a reference to that projection in their place.
 *
 * <p>A lambda body may reference an OVER window of the enclosing query ({@code ARRAY_TRANSFORM(a, x
 * -> x + SUM(base) OVER ())}). The window yields one value per row rather than per element, so
 * {@link HigherOrderFunctionUtil#liftCaptures} hoists it out of the body into a trailing operand of
 * the call. It cannot stay there: {@code ProjectToWindowRule} slices a projection containing a
 * window into a windowed and a non-windowed part, and the slicing works on the flattened {@link
 * org.apache.calcite.rex.RexProgram} of the projection, in which a lambda is an expression of its
 * own (see {@code RexProgramBuilder.RegisterShuttle#visitLambda}). The lambda would then be
 * materialized as an output field of the non-windowed part, where code generation rejects it -- a
 * lambda is only meaningful as an argument of the call that applies it.
 *
 * <p>Evaluating the window one projection lower gives the plan a user would get by writing the
 * window in a sub-query. This rule must run before {@code ProjectToWindowRule}; it applies to both
 * the SQL and the Table API surface, which both reach the optimizer with the lifted shape.
 */
public class ExtractLambdaOverWindowRule extends RelOptRule {

    public static final ExtractLambdaOverWindowRule INSTANCE = new ExtractLambdaOverWindowRule();

    private ExtractLambdaOverWindowRule() {
        super(operand(LogicalProject.class, any()), "ExtractLambdaOverWindowRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project project = call.rel(0);
        return project.getProjects().stream().anyMatch(ExtractLambdaOverWindowRule::hasLambdaOver);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final RelNode input = project.getInput();
        final int inputFieldCount = input.getRowType().getFieldCount();

        // digest -> the window, in first-seen order; its position in this map plus the input field
        // count is the index it is referenced by in the rewritten projection
        final Map<String, RexOver> extracted = new LinkedHashMap<>();
        final RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall rexCall) {
                        final RexCall visited = (RexCall) super.visitCall(rexCall);
                        if (!isLiftedHigherOrderCall(visited)) {
                            return visited;
                        }
                        final List<RexNode> newOperands = new ArrayList<>();
                        for (RexNode operand : visited.getOperands()) {
                            if (operand instanceof RexOver) {
                                final RexOver over = (RexOver) operand;
                                final int index =
                                        indexOf(extracted, over.toString(), over, inputFieldCount);
                                newOperands.add(new RexInputRef(index, over.getType()));
                            } else {
                                newOperands.add(operand);
                            }
                        }
                        return rexBuilder.makeCall(
                                visited.getType(), visited.getOperator(), newOperands);
                    }
                };

        final List<RexNode> newProjects = new ArrayList<>();
        for (RexNode expr : project.getProjects()) {
            newProjects.add(expr.accept(shuttle));
        }

        final List<RexNode> belowProjects = new ArrayList<>();
        for (int i = 0; i < inputFieldCount; i++) {
            belowProjects.add(rexBuilder.makeInputRef(input, i));
        }
        belowProjects.addAll(extracted.values());

        relBuilder.push(input).project(belowProjects);
        call.transformTo(
                project.copy(
                        project.getTraitSet(),
                        relBuilder.build(),
                        newProjects,
                        project.getRowType()));
    }

    private static int indexOf(
            Map<String, RexOver> extracted, String key, RexOver over, int offset) {
        int index = 0;
        for (String seen : extracted.keySet()) {
            if (seen.equals(key)) {
                return offset + index;
            }
            index++;
        }
        extracted.put(key, over);
        return offset + index;
    }

    /**
     * Whether the expression contains a higher-order call carrying a lifted OVER window operand.
     */
    private static boolean hasLambdaOver(RexNode expr) {
        final boolean[] found = new boolean[1];
        expr.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (isLiftedHigherOrderCall(call)) {
                            found[0] = true;
                        }
                        return super.visitCall(call);
                    }
                });
        return found[0];
    }

    private static boolean isLiftedHigherOrderCall(RexCall call) {
        boolean hasLambda = false;
        boolean hasOver = false;
        for (RexNode operand : call.getOperands()) {
            hasLambda |= operand instanceof RexLambda;
            hasOver |= operand instanceof RexOver;
        }
        return hasLambda && hasOver;
    }
}
