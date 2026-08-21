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
package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil.{generateFunctionAwareCall, DefaultExpressionEvaluatorFactory}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.functions.utils.HigherOrderFunctionUtil
import org.apache.flink.table.runtime.collector.WrappingCollector
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.{RexCall, RexCallBinding, RexLambda, RexLocalRef, RexNode, RexProgram}

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Generates a call to a user-defined [[ScalarFunction]] or [[TableFunction]].
 *
 * Table functions are a special case because they are using a collector. Thus, the result of this
 * generator will be a reference to a [[WrappingCollector]]. Furthermore, atomic types are wrapped
 * into a row by the collector.
 */
class BridgingSqlFunctionCallGen(call: RexCall, rexProgram: RexProgram) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val function = call.getOperator.asInstanceOf[BridgingSqlFunction]
    val definition = function.getDefinition
    val dataTypeFactory = function.getDataTypeFactory
    val rexFactory = function.getRexFactory

    // A lambda body may close over outer columns. Lambda-capture lifting turns those captures into
    // additional (closed) lambda parameters and appends the captured columns as trailing call
    // operands (see HigherOrderFunctionUtil). The user-defined function's type inference only knows
    // its declared arguments, so the trailing capture operands are stripped from the call used for
    // type inference / eval and instead bound behind the generated function object.
    val captureCount = lambdaCaptureCount(call)
    val declaredCall =
      if (captureCount == 0) {
        call
      } else {
        call
          .clone(call.getType, call.getOperands.asScala.dropRight(captureCount).asJava)
          .asInstanceOf[RexCall]
      }
    val declaredOperands = operands.dropRight(captureCount)
    val captureOperands = operands.takeRight(captureCount)

    // we could have implemented a dedicated code generation context but the closer we are to
    // Calcite the more consistent is the type inference during the data type enrichment
    val callContext = new OperatorBindingCallContext(
      dataTypeFactory,
      definition,
      RexCallBinding.create(
        function.getTypeFactory,
        declaredCall,
        rexProgram,
        Collections.emptyList()),
      declaredCall.getType)

    // create the final UDF for runtime
    val evaluatorFactory =
      new DefaultExpressionEvaluatorFactory(ctx.tableConfig, ctx.classLoader, rexFactory, ctx)
    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      function.getName,
      definition,
      callContext,
      classOf[PlannerBase].getClassLoader,
      ctx.tableConfig,
      evaluatorFactory
    )
    val inference = udf.getTypeInference(dataTypeFactory)

    generateFunctionAwareCall(
      ctx,
      declaredOperands,
      returnType,
      inference,
      callContext,
      udf,
      function.toString,
      skipIfArgsNull = false,
      evaluatorFactory,
      captureOperands)
  }

  /**
   * The total number of lifted capture parameters across all of this call's lambda operands
   * (parameters added by
   * [[org.apache.flink.table.planner.functions.utils.HigherOrderFunctionUtil]], numbered from
   * {@code CAPTURE_PARAM_INDEX_BASE}), which equals the number of trailing capture operands. A call
   * may carry more than one lambda operand (a user-defined function declaring several lambda
   * arguments); each lambda contributes its own captures. Zero if the call has no lambda operand or
   * no lambda captures anything.
   */
  private def lambdaCaptureCount(call: RexCall): Int = {
    call.getOperands.asScala
      .flatMap(resolveLambda)
      .map(_.getParameters.asScala.count(p => HigherOrderFunctionUtil.isCaptureParameter(p)))
      .sum
  }

  private def resolveLambda(node: RexNode): Option[RexLambda] = node match {
    case lambda: RexLambda => Some(lambda)
    case localRef: RexLocalRef if rexProgram != null =>
      resolveLambda(rexProgram.getExprList.get(localRef.getIndex))
    case _ => None
  }
}
