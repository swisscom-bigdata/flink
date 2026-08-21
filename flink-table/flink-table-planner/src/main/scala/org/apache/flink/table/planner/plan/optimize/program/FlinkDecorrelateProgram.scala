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
package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.functions.utils.HigherOrderFunctionUtil

import org.apache.calcite.rel.{RelHomogeneousShuttle, RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexCorrelVariable, RexLambda, RexNode, RexShuttle, RexVisitorImpl}
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
 * A FlinkOptimizeProgram that decorrelates a query and validates whether the result still has
 * correlate variables.
 *
 * @tparam OC
 *   OptimizeContext
 */
class FlinkDecorrelateProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] {

  def optimize(root: RelNode, context: OC): RelNode = {
    val result = RelDecorrelator.decorrelateQuery(root)
    val relifted = reliftLambdaCaptures(result)
    checkCorrelVariableExists(relifted)
    relifted
  }

  /**
   * Re-lifts the free-variable captures of every higher-order-function call in the plan.
   *
   * A cross-query correlation captured inside a lambda body enters the planner as a correlated
   * field access ({@code $cor0.a}) rather than an input reference, so it is not lifted out of the
   * lambda by [[HigherOrderFunctionUtil.liftCaptures]] at conversion time. Decorrelation rewrites
   * that field access into a plain [[org.apache.calcite.rex.RexInputRef]] of the decorrelated row,
   * which now sits un-lifted inside the lambda body and violates the closed-lambda invariant that
   * code generation relies on. Re-running capture lifting over the decorrelated plan hoists such a
   * reference into a trailing capture operand evaluated in the enclosing row scope, restoring the
   * invariant. The pass is a no-op for lambdas that are already closed (FLINK-31207 higher-order
   * functions and lambdas).
   */
  private def reliftLambdaCaptures(root: RelNode): RelNode = {
    val rexBuilder: RexBuilder = root.getCluster.getRexBuilder
    val rexShuttle: RexShuttle = new RexShuttle() {
      override def visitCall(call: RexCall): RexNode = {
        val visited = super.visitCall(call).asInstanceOf[RexCall]
        if (visited.getOperands.exists(_.isInstanceOf[RexLambda])) {
          HigherOrderFunctionUtil.liftCaptures(visited, rexBuilder)
        } else {
          visited
        }
      }

      override def visitLambda(lambda: RexLambda): RexNode = {
        // Descend into the body so that a higher-order-function call nested inside this lambda is
        // re-lifted before this lambda's enclosing call is (bottom-up). RexShuttle#visitLambda in
        // Calcite 1.41.0 (the pinned version) does not rewrite the body, so this override is
        // required; CALCITE-7497 (fixVersion 1.42.0) makes it redundant on that upgrade.
        val oldBody = lambda.getExpression
        val newBody = oldBody.accept(this)
        if (newBody eq oldBody) {
          lambda
        } else {
          rexBuilder.makeLambdaCall(newBody, lambda.getParameters)
        }
      }
    }
    val relShuttle: RelHomogeneousShuttle = new RelHomogeneousShuttle() {
      override def visit(rel: RelNode): RelNode = {
        val withVisitedChildren = super.visit(rel)
        withVisitedChildren.accept(rexShuttle)
      }
    }
    root.accept(relShuttle)
  }

  /**
   * Check if there is still correlate variables after decorrelating.
   *
   * NOTES: this method only checks correlate variables in join, project and filter, and will ignore
   * the correlate variables from UNNEST (inputs of Uncollect).
   */
  private def checkCorrelVariableExists(root: RelNode): Unit = {
    try {
      checkCorrelVariableOf(root)
    } catch {
      case fo: Util.FoundOne =>
        throw new TableException(
          s"unexpected correlate variable " +
            s"${fo.getNode.asInstanceOf[RexCorrelVariable].id} in the plan")
    }
  }

  private def checkCorrelVariableOf(input: RelNode): Unit = {
    val shuttle: RelShuttleImpl = new RelShuttleImpl() {
      final val visitor = new RexVisitorImpl[Void](true) {
        override def visitCorrelVariable(correlVariable: RexCorrelVariable): Void = {
          throw new Util.FoundOne(correlVariable)
        }

        override def visitLambda(lambda: RexLambda): Void = {
          // RexVisitorImpl#visitLambda does not visit the body (Calcite 1.41), so a correlate
          // variable left inside a lambda body after decorrelation would otherwise go undetected
          // here and only surface as a code-generation crash (FLINK-31207).
          lambda.getExpression.accept(this)
          null
        }
      }

      override def visit(filter: LogicalFilter): RelNode = {
        filter.getCondition.accept(visitor)
        super.visit(filter)
      }

      override def visit(project: LogicalProject): RelNode = {
        project.getProjects.foreach(_.accept(visitor))
        super.visit(project)
      }

      override def visit(join: LogicalJoin): RelNode = {
        join.getCondition.accept(visitor)
        super.visit(join)
      }

      override def visit(other: RelNode): RelNode = {
        other match {
          // ignore Uncollect's inputs due to the correlate variables are from UNNEST directly,
          // not from cases (project, filter and join) which RelDecorrelator handles
          case r: Uncollect => r
          case _ => super.visit(other)
        }
      }
    }
    input.accept(shuttle)
  }

}
