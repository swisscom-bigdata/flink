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

import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rex.{RexCorrelVariable, RexLambda, RexVisitorImpl}
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
   * Lifts lambda captures again after decorrelation (see [[HigherOrderFunctionUtil]]). Lambdas are
   * lifted right after conversion, including those in sub-queries, and a reference to an enclosing
   * query becomes a correlated call operand there, which decorrelation rewrites like any other
   * operand. This pass therefore normally changes nothing. It closes a lambda whose body a rewrite
   * has left with a column or a correlation variable, which code generation could not compile.
   * Calcite 1.41's decorrelator does not rewrite lambda bodies; from Calcite 1.42 (CALCITE-7497) on
   * it does, and can put a column into a body, for example in a null check around a literal.
   * Lambdas that are already closed are left unchanged.
   */
  private def reliftLambdaCaptures(root: RelNode): RelNode = {
    HigherOrderFunctionUtil.liftCaptures(root, root.getCluster.getRexBuilder)
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
