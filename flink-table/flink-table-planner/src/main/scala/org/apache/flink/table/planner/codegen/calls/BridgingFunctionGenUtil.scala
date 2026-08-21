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

import org.apache.flink.api.common.functions.{AbstractRichFunction, OpenContext, RichFunction}
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{DataTypes, TableException}
import org.apache.flink.table.api.Expressions.callSql
import org.apache.flink.table.data.{GenericRowData, RawValueData, StringData}
import org.apache.flink.table.data.binary.{BinaryRawValueData, BinaryStringData}
import org.apache.flink.table.expressions.ApiExpressionUtils.{typeLiteral, unresolvedCall, unresolvedRef}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions._
import org.apache.flink.table.functions.SpecializedFunction.{ExpressionEvaluator, ExpressionEvaluatorFactory}
import org.apache.flink.table.functions.UserDefinedFunctionHelper._
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexFactory}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.AsyncCodeGenerator.DEFAULT_DELEGATING_FUTURE_TERM
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.collector.WrappingCollector
import org.apache.flink.table.runtime.functions.{DefaultExpressionEvaluator, LambdaFunctionFactory}
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.correlate.async.DelegatingAsyncTableResultFuture
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper
import org.apache.flink.table.types.inference.{CallContext, LambdaInfo, RefinableLambdaInputTypeStrategy, TypeInference, TypeInferenceUtil}
import org.apache.flink.table.types.logical.{FunctionType, LogicalType, LogicalTypeRoot, RowType}
import org.apache.flink.table.types.logical.RowType.RowField
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType
import org.apache.flink.table.types.utils.DataTypeUtils
import org.apache.flink.table.types.utils.DataTypeUtils.{isInternal, validateInputDataType, validateOutputDataType}
import org.apache.flink.util.Preconditions

import org.apache.calcite.rex.{RexCall, RexCallBinding, RexProgram}

import java.util.Collections
import java.util.concurrent.CompletableFuture

import scala.collection.JavaConverters._

/**
 * Helps in generating a call to a user-defined [[ScalarFunction]], [[TableFunction]],
 * [[ProcessTableFunction]] or [[AsyncTableFunction]].
 *
 * Table functions are a special case because they are using a collector. Thus, the result of the
 * generation will be a reference to a [[WrappingCollector]]. Furthermore, atomic types are wrapped
 * into a row by the collector.
 *
 * Async table functions don't support atomic types.
 */
object BridgingFunctionGenUtil {

  def generateFunctionAwareCall(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean,
      evaluatorFactory: ExpressionEvaluatorFactory = null,
      lambdaCaptureOperands: Seq[GeneratedExpression] = Nil): GeneratedExpression = {

    val (call, _) = generateFunctionAwareCallWithDataType(
      ctx,
      operands,
      returnType,
      inference,
      callContext,
      udf,
      functionName,
      skipIfArgsNull,
      evaluatorFactory,
      lambdaCaptureOperands)

    call
  }

  def generateFunctionAwareCallWithDataType(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean,
      evaluatorFactory: ExpressionEvaluatorFactory = null,
      lambdaCaptureOperands: Seq[GeneratedExpression] = Nil): (GeneratedExpression, DataType) = {
    val result = generateFunctionAwareCallWithDataTypeAndTimeout(
      ctx,
      operands,
      returnType,
      inference,
      callContext,
      udf,
      functionName,
      skipIfArgsNull,
      evaluatorFactory,
      lambdaCaptureOperands)
    (result._1, result._3)
  }

  /**
   * Resolves the [[BridgingSqlFunction]] wrapping a [[RexCall]] to a [[UserDefinedFunction]] and
   * generates both the eval-call expression and (for async table functions) an optional timeout-
   * call expression. Mirrors [[BridgingSqlFunctionCallGen.generate]] but additionally returns the
   * timeout-call so that callers driving an [[AsyncFunction]] codegen path (currently
   * [[org.apache.flink.table.planner.codegen.AsyncCorrelateCodeGenerator]]) can render a custom
   * `timeout(...)` method on the generated subclass.
   *
   * <p>Used in correlate-query codegen where the top-level [[RexCall]] is known to be an async
   * table function invocation; lookup-join codegen still uses
   * [[generateFunctionAwareCallWithDataTypeAndTimeout]] directly because it constructs the type
   * inference from the [[org.apache.flink.table.connector.source.LookupTableSource]] schema rather
   * than the function definition.
   */
  def generateBridgingFunctionCallWithTimeout(
      ctx: CodeGeneratorContext,
      call: RexCall,
      rexProgram: RexProgram,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      skipIfArgsNull: Boolean): (GeneratedExpression, Option[GeneratedExpression], DataType) = {
    val function = call.getOperator.asInstanceOf[BridgingSqlFunction]
    val definition = function.getDefinition
    val dataTypeFactory = function.getDataTypeFactory
    val rexFactory = function.getRexFactory

    val callContext = new OperatorBindingCallContext(
      dataTypeFactory,
      definition,
      RexCallBinding.create(function.getTypeFactory, call, rexProgram, Collections.emptyList()),
      call.getType)

    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      function.getName,
      definition,
      callContext,
      classOf[PlannerBase].getClassLoader,
      ctx.tableConfig,
      new DefaultExpressionEvaluatorFactory(ctx.tableConfig, ctx.classLoader, rexFactory, ctx)
    )
    val inference = udf.getTypeInference(dataTypeFactory)

    generateFunctionAwareCallWithDataTypeAndTimeout(
      ctx,
      operands,
      returnType,
      inference,
      callContext,
      udf,
      function.toString,
      skipIfArgsNull)
  }

  /**
   * Like [[generateFunctionAwareCallWithDataType]] but additionally returns an optional
   * timeout-call expression when the UDF is an [[AsyncTableFunction]] (or its
   * [[org.apache.flink.table.functions.AsyncLookupFunction]] subclass) that declares a public,
   * non-static `timeout(CompletableFuture, ...)` method whose parameter list matches the call site.
   * When the user UDF does not declare such a method, the returned timeout-call is empty and the
   * generated `AsyncFunction` falls back to the framework default that completes the future with a
   * [[java.util.concurrent.TimeoutException]].
   *
   * <p>An illegal `timeout` signature (e.g., parameter count or types incompatible with the lookup
   * keys) triggers a [[org.apache.flink.table.api.ValidationException]] that bubbles up to fail the
   * job at submit / operator init time, so misconfigurations never reach the data path.
   */
  def generateFunctionAwareCallWithDataTypeAndTimeout(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType,
      inference: TypeInference,
      callContext: CallContext,
      udf: UserDefinedFunction,
      functionName: String,
      skipIfArgsNull: Boolean,
      evaluatorFactory: ExpressionEvaluatorFactory = null,
      lambdaCaptureOperands: Seq[GeneratedExpression] = Nil)
      : (GeneratedExpression, Option[GeneratedExpression], DataType) = {

    // enrich argument types with conversion class
    val castCallContext = TypeInferenceUtil.castArguments(inference, callContext, null)
    val enrichedArgumentDataTypes = toScala(castCallContext.getArgumentDataTypes)

    // A lambda argument (FUNCTION type) of a higher-order function is passed to eval() as a
    // first-class function object (java.util.function.Function / BiFunction) that wraps the compiled
    // lambda body (see generateLambdaFunctionObject). All other operands are passed through as usual.
    // The FUNCTION type of a lifted lambda already excludes the lifted capture parameters (see
    // OperatorBindingCallContext#lambdaDataType), so it carries the user-visible arity and thus the
    // eval() parameter class Function / BiFunction / TriFunction.
    //
    // A call may carry more than one lambda argument. The trailing capture operands are then the
    // concatenation, in left-to-right operand order, of each lambda's own captures. A lambda's
    // captures are exactly the parameters beyond the user-visible arity that its FUNCTION type
    // records, so we partition the trailing operands back to the owning lambda by consuming, per
    // lambda, as many as it declares.
    var captureOffset = 0
    val (runtimeOperands, runtimeArgumentDataTypes) = enrichedArgumentDataTypes.indices.map {
      i =>
        val dataType = enrichedArgumentDataTypes(i)
        if (dataType.getLogicalType.is(LogicalTypeRoot.FUNCTION)) {
          val lambdaInfo = toScala(callContext.getLambdaArgument(i)).getOrElse(
            throw new CodeGenException(
              s"Missing lambda argument at position $i for function '$functionName'."))
          val captureCount = lambdaInfo.getParameterFields.size() -
            dataType.getLogicalType.asInstanceOf[FunctionType].getParameterCount
          val lambdaCaptures =
            lambdaCaptureOperands.slice(captureOffset, captureOffset + captureCount)
          captureOffset += captureCount
          val functionObject = generateLambdaFunctionObject(
            ctx,
            evaluatorFactory,
            lambdaInfo,
            dataType,
            requiredLambdaResultDataType(castCallContext, i),
            lambdaCaptures)
          (functionObject, dataType)
        } else {
          (operands(i), dataType)
        }
    }.unzip

    verifyArgumentTypes(runtimeOperands.map(_.resultType), runtimeArgumentDataTypes)

    // enrich output types with conversion class
    val enrichedOutputDataType =
      TypeInferenceUtil.inferOutputType(castCallContext, inference.getOutputTypeStrategy)
    verifyFunctionAwareOutputType(returnType, enrichedOutputDataType, udf)

    // find runtime method and generate call
    verifyFunctionAwareImplementation(
      runtimeArgumentDataTypes,
      enrichedOutputDataType,
      udf,
      functionName)

    val functionTerm = ctx.addReusableFunction(udf)
    val externalOperands = prepareExternalOperands(ctx, runtimeOperands, runtimeArgumentDataTypes)

    val call = generateFunctionAwareCallFromPreparedOperands(
      ctx,
      functionTerm,
      externalOperands,
      enrichedOutputDataType,
      returnType,
      udf,
      skipIfArgsNull,
      None)

    val timeoutCall = if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      generateAsyncTableFunctionTimeoutCall(
        udf,
        functionName,
        runtimeArgumentDataTypes,
        functionTerm,
        externalOperands,
        returnType,
        enrichedOutputDataType,
        skipIfArgsNull)
    } else {
      None
    }
    (call, timeoutCall, enrichedOutputDataType)
  }

  private def generateFunctionAwareCallFromPreparedOperands(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputDataType: DataType,
      returnType: LogicalType,
      udf: UserDefinedFunction,
      skipIfArgsNull: Boolean,
      contextTerm: Option[String]): GeneratedExpression = {
    if (udf.getKind == FunctionKind.TABLE || udf.getKind == FunctionKind.PROCESS_TABLE) {
      generateTableFunctionCall(
        ctx,
        functionTerm,
        externalOperands,
        outputDataType,
        returnType,
        skipIfArgsNull,
        contextTerm
      )
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      generateAsyncTableFunctionCall(
        functionTerm,
        externalOperands,
        returnType,
        outputDataType,
        skipIfArgsNull)
    } else if (udf.getKind == FunctionKind.ASYNC_SCALAR) {
      generateAsyncScalarFunctionCall(
        ctx,
        functionTerm,
        externalOperands,
        returnType,
        outputDataType)
    } else {
      generateScalarFunctionCall(ctx, functionTerm, externalOperands, outputDataType)
    }
  }

  /**
   * Generates the body of the `timeout(...)` method that is rendered into a codegen
   * `RichAsyncFunction` subclass when the user UDF declares a legal `timeout` method. Mirrors
   * [[generateAsyncTableFunctionCall]] except the generated code invokes `function.timeout(...)`
   * instead of `function.eval(...)` and reuses the SAME UDF instance registered by the eval path.
   */
  private def generateAsyncTableFunctionTimeoutCall(
      udf: UserDefinedFunction,
      functionName: String,
      enrichedArgumentDataTypes: Seq[DataType],
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      returnType: LogicalType,
      outputDataType: DataType,
      skipIfArgsNull: Boolean): Option[GeneratedExpression] = {
    val argumentClasses = enrichedArgumentDataTypes.map(_.getConversionClass).toArray
    val hasTimeout =
      validateAsyncTableFunctionTimeoutClass(udf.getClass, argumentClasses, functionName)
    if (!hasTimeout) {
      None
    } else {
      val DELEGATE_ASYNC_TABLE = className[DelegatingAsyncTableResultFuture]
      val outputType = outputDataType.getLogicalType
      val needsWrapping = !isCompositeType(outputType)
      val isInternal = DataTypeUtils.isInternal(outputDataType)
      // Enforce AsyncTableFunction's synchronous-completion contract for timeout(...):
      // the handler MUST complete the future before it returns. If it doesn't (typically
      // because the user issued another async call and stored the future for a later
      // callback), the AsyncWaitOperator's ResultHandler would never be released and the
      // downstream record would hang until shutdown. Fail fast here instead.
      //
      // We deliberately do NOT wrap the call in try/catch. Synchronous throws are already
      // caught by AsyncCorrelateRunner.timeout / AsyncLookupJoinRunner.timeout and forwarded
      // to the ResultFuture, so duplicating that here would only obscure the contract.
      val callTimeoutCode =
        s"""
           |$functionTerm.timeout(
           |  delegates.getCompletableFuture(),
           |  ${externalOperands.map(_.resultTerm).mkString(", ")});
           |if (!delegates.getCompletableFuture().isDone()) {
           |  delegates.getCompletableFuture().completeExceptionally(
           |    new IllegalStateException(
           |      "AsyncTableFunction.timeout(...) must complete the future synchronously; "
           |        + "issuing another async call from inside timeout() is not allowed."));
           |}
           |""".stripMargin
      val functionCallCode = if (skipIfArgsNull) {
        s"""
           |${externalOperands.map(_.code).mkString("\n")}
           |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
           |  $DEFAULT_COLLECTOR_TERM.complete(java.util.Collections.emptyList());
           |} else {
           |  $DELEGATE_ASYNC_TABLE delegates = new $DELEGATE_ASYNC_TABLE($DEFAULT_COLLECTOR_TERM,
           |      $needsWrapping, $isInternal);
           |  $callTimeoutCode
           |}
           |""".stripMargin
      } else {
        s"""
           |${externalOperands.map(_.code).mkString("\n")}
           |$DELEGATE_ASYNC_TABLE delegates = new $DELEGATE_ASYNC_TABLE($DEFAULT_COLLECTOR_TERM,
           |      $needsWrapping, $isInternal);
           |$callTimeoutCode
           |""".stripMargin
      }
      Some(GeneratedExpression(NO_CODE, NEVER_NULL, functionCallCode, returnType))
    }
  }

  def generateTableFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      functionOutputDataType: DataType,
      outputType: LogicalType,
      skipIfArgsNull: Boolean,
      contextTerm: Option[String] = None): GeneratedExpression = {
    val resultCollectorTerm = generateResultCollector(ctx, functionOutputDataType, outputType)

    val setCollectorCode = s"""
                              |$functionTerm.setCollector($resultCollectorTerm);
                              |""".stripMargin
    ctx.addReusableOpenStatement(setCollectorCode)

    val contextOperand = contextTerm.map(c => c + ", ").getOrElse("")

    val functionCallCode = if (skipIfArgsNull) {
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
         |  // skip
         |} else {
         |  $functionTerm.eval($contextOperand${externalOperands.map(_.resultTerm).mkString(", ")});
         |}
         |""".stripMargin
    } else {
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |$functionTerm.eval($contextOperand${externalOperands.map(_.resultTerm).mkString(", ")});
         |""".stripMargin
    }

    // has no result
    GeneratedExpression(resultCollectorTerm, NEVER_NULL, functionCallCode, outputType)
  }

  private def generateAsyncTableFunctionCall(
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      returnType: LogicalType,
      outputDataType: DataType,
      skipIfArgsNull: Boolean): GeneratedExpression = {

    val DELEGATE_ASYNC_TABLE = className[DelegatingAsyncTableResultFuture]
    val outputType = outputDataType.getLogicalType

    // If we need to wrap data in a row, it's done in the delegating class.
    val needsWrapping = !isCompositeType(outputType)
    val isInternal = DataTypeUtils.isInternal(outputDataType);
    val arguments = Seq(
      s"""
         |delegates.getCompletableFuture()
         |""".stripMargin
    ) ++ externalOperands.map(_.resultTerm)
    val anyNull = externalOperands.map(_.nullTerm) ++ Seq("false")

    val functionCallCode = {
      if (skipIfArgsNull) {
        s"""
           |${externalOperands.map(_.code).mkString("\n")}
           |if (${anyNull.mkString(" || ")}) {
           |  $DEFAULT_COLLECTOR_TERM.complete(java.util.Collections.emptyList());
           |} else {
           |  $DELEGATE_ASYNC_TABLE delegates = new $DELEGATE_ASYNC_TABLE($DEFAULT_COLLECTOR_TERM,
           |      $needsWrapping, $isInternal);
           |  $functionTerm.eval(${arguments.mkString(", ")});
           |}
           |""".stripMargin
      } else {
        s"""
           |${externalOperands.map(_.code).mkString("\n")}
           |$DELEGATE_ASYNC_TABLE delegates = new $DELEGATE_ASYNC_TABLE($DEFAULT_COLLECTOR_TERM,
           |      $needsWrapping, $isInternal);
           |  $functionTerm.eval(${arguments.mkString(", ")});
           |""".stripMargin
      }
    }

    // has no result
    GeneratedExpression(NO_CODE, NEVER_NULL, functionCallCode, returnType)
  }

  private def generateAsyncScalarFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputType: LogicalType,
      outputDataType: DataType): GeneratedExpression = {
    val converterTerm = ctx.addReusableConverter(outputDataType)
    val functionCallCode =
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |if (${externalOperands.map(_.nullTerm).mkString(" || ")}) {
         |  $DEFAULT_DELEGATING_FUTURE_TERM.createAsyncFuture($converterTerm).complete(null);
         |} else {
         |  $functionTerm.eval(
         |    $DEFAULT_DELEGATING_FUTURE_TERM.createAsyncFuture($converterTerm),
         |    ${externalOperands.map(_.resultTerm).mkString(", ")});
         |}
         |""".stripMargin

    GeneratedExpression(NO_CODE, NEVER_NULL, functionCallCode, outputType)
  }

  /**
   * Generates a collector that converts the output of a table function (possibly as an atomic type)
   * into an internal row type. Returns a collector term for referencing the collector.
   */
  def generateResultCollector(
      ctx: CodeGeneratorContext,
      outputDataType: DataType,
      returnType: LogicalType): String = {
    val outputType = outputDataType.getLogicalType

    val collectorCtx = new CodeGeneratorContext(ctx.tableConfig, ctx.classLoader, ctx)
    val externalResultTerm = newName(ctx, "externalResult")

    // code for wrapping atomic types
    val collectorCode = if (!isCompositeType(outputType)) {
      val resultGenerator = new ExprCodeGenerator(collectorCtx, outputType.isNullable)
        .bindInput(outputType, externalResultTerm)
      val wrappedResult = resultGenerator.generateConverterResultExpression(
        returnType.asInstanceOf[RowType],
        classOf[GenericRowData])
      s"""
         |${wrappedResult.code}
         |outputResult(${wrappedResult.resultTerm});
         |""".stripMargin
    } else {
      s"""
         |if ($externalResultTerm != null) {
         |  outputResult($externalResultTerm);
         |}
         |""".stripMargin
    }

    // collector for converting to internal types then wrapping atomic types
    val resultCollector = CollectorCodeGenerator.generateWrappingCollector(
      collectorCtx,
      "TableFunctionResultConverterCollector",
      outputType,
      externalResultTerm,
      // nullability is handled by the expression code generator if necessary
      genToInternalConverter(ctx, outputDataType),
      collectorCode
    )
    val resultCollectorTerm = newName(ctx, "resultConverterCollector")
    CollectorCodeGenerator.addToContext(ctx, resultCollectorTerm, resultCollector)

    resultCollectorTerm
  }

  private def generateScalarFunctionCall(
      ctx: CodeGeneratorContext,
      functionTerm: String,
      externalOperands: Seq[GeneratedExpression],
      outputDataType: DataType): GeneratedExpression = {

    // result conversion
    val externalResultClass = outputDataType.getConversionClass
    val externalResultTypeTerm = typeTerm(externalResultClass)
    // Janino does not fully support the JVM spec:
    // boolean b = (boolean) f(); where f returns Object
    // This is not supported and we need to box manually.
    val externalResultClassBoxed = primitiveToWrapper(externalResultClass)
    val externalResultCasting = if (externalResultClass == externalResultClassBoxed) {
      s"($externalResultTypeTerm)"
    } else {
      s"($externalResultTypeTerm) (${typeTerm(externalResultClassBoxed)})"
    }
    val externalResultTerm = ctx.addReusableLocalVariable(externalResultTypeTerm, "externalResult")
    val externalCode =
      s"""
         |${externalOperands.map(_.code).mkString("\n")}
         |$externalResultTerm = $externalResultCasting $functionTerm
         |  .$SCALAR_EVAL(${externalOperands.map(_.resultTerm).mkString(", ")});
         |""".stripMargin

    val internalExpr = genToInternalConverterAll(ctx, outputDataType, externalResultTerm)

    val copy = internalExpr.copy(code = s"""
                                           |$externalCode
                                           |${internalExpr.code}
                                           |""".stripMargin)

    ExternalGeneratedExpression.fromGeneratedExpression(
      outputDataType,
      externalResultTerm,
      externalCode,
      copy)
  }

  def prepareExternalOperands(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      argumentDataTypes: Seq[DataType]): Seq[GeneratedExpression] = {
    operands
      .zip(argumentDataTypes)
      .map {
        case (operand, dataType) =>
          if (dataType.getLogicalType.is(LogicalTypeRoot.FUNCTION)) {
            // A lambda (FUNCTION-typed) argument is already an external function object generated by
            // generateLambdaFunctionObject; pass it through without conversion (the FUNCTION type
            // has no internal representation).
            operand match {
              case external: ExternalGeneratedExpression =>
                operand.copy(resultTerm = external.getExternalTerm, code = external.getExternalCode)
              case _ => operand
            }
          } else {
            operand match {
              case external: ExternalGeneratedExpression
                  if !isInternal(dataType) && (external.getDataType == dataType) =>
                operand.copy(resultTerm = external.getExternalTerm, code = external.getExternalCode)
              case _ =>
                operand.copy(resultTerm = genToExternalConverterAll(ctx, dataType, operand))
            }
          }
      }
  }

  /**
   * The type a function requires the body of its lambda argument at `pos` to produce, if any. Only
   * a built-in [[RefinableLambdaInputTypeStrategy]] declares one; see
   * [[RefinableLambdaInputTypeStrategy#getRequiredLambdaResultType]].
   *
   * The strategy is taken from the function *definition* rather than from the specialized
   * function's own [[TypeInference]]: a specialized function restates its already-resolved argument
   * types as an explicit sequence (see `BuiltInScalarFunction#getTypeInference`), which no longer
   * carries the declaring strategy.
   */
  private def requiredLambdaResultDataType(callContext: CallContext, pos: Int): Option[DataType] = {
    val declaredStrategy = callContext.getFunctionDefinition
      .getTypeInference(callContext.getDataTypeFactory)
      .getInputTypeStrategy
    declaredStrategy match {
      case strategy: RefinableLambdaInputTypeStrategy =>
        toScala(strategy.getRequiredLambdaResultType(callContext, pos))
      case _ => None
    }
  }

  /**
   * Generates the first-class function object passed to a user-defined higher-order function for a
   * lambda (FUNCTION-typed) argument: a [[java.util.function.Function]] for a one-parameter lambda,
   * a [[java.util.function.BiFunction]] for a two-parameter lambda, or an
   * [[org.apache.flink.util.function.TriFunction]] for a three-parameter lambda. The lambda body is
   * compiled into an [[ExpressionEvaluator]] that is opened once in the operator and doubles as the
   * factory for these objects; the generated object casts its arguments to the lambda parameters'
   * external classes and calls the compiled body directly.
   *
   * A failure in the body surfaces from `apply` the same way for a user-defined and for a built-in
   * higher-order function, since both take this path: an unchecked exception propagates unchanged,
   * while a checked exception is wrapped because the functional interface cannot declare it.
   */
  private def generateLambdaFunctionObject(
      ctx: CodeGeneratorContext,
      evaluatorFactory: ExpressionEvaluatorFactory,
      lambdaInfo: LambdaInfo,
      lambdaDataType: DataType,
      requiredResultDataType: Option[DataType],
      captureOperands: Seq[GeneratedExpression]): ExternalGeneratedExpression = {
    if (evaluatorFactory == null) {
      throw new CodeGenException(
        "A lambda argument can only be generated for a user-defined higher-order function call.")
    }
    // The values a function feeds its lambda are its own, so the body is compiled in the
    // representation the function declared for this argument (see FunctionType). LambdaInfo
    // describes the lambda as it was written, which is representation-neutral.
    val internalLambdaData = DataTypeUtils.isInternal(lambdaDataType)
    // The evaluator is compiled against all lambda parameters (the user-visible ones followed by the
    // lifted captures); the function object only exposes the user-visible parameters and binds the
    // lifted captures to the (loop-invariant) values of the trailing capture operands.
    val toLambdaDataType: DataTypes.Field => DataTypes.Field = field =>
      if (internalLambdaData) {
        DataTypes.FIELD(field.getName, DataTypeUtils.toInternalDataType(field.getDataType))
      } else {
        field
      }
    val allParamFields = toScala(lambdaInfo.getParameterFields).map(toLambdaDataType)
    // A function may require the body to produce a type of its own choosing rather than the type the
    // body happens to have (ARRAY_REDUCE coerces its reducer to the accumulator type, see
    // RefinableLambdaInputTypeStrategy#getRequiredLambdaResultType). The coercion is expressed as a
    // CAST around the body so that it is folded into the compiled body instead of costing an extra
    // conversion per application.
    // Nullability is deliberately not part of the comparison: a nullable body is only ever handed to
    // a nullable parameter (the refinement pass above widens the accumulator for it), and casting
    // would only restate the body's own type. The body keeps its own nullability so that it stays
    // the type the expression actually has.
    val needsCoercion = requiredResultDataType.exists(
      dataType =>
        !dataType.getLogicalType
          .copy(true)
          .equals(lambdaInfo.getReturnDataType.getLogicalType.copy(true)))
    val bodyResultDataType = if (needsCoercion) {
      requiredResultDataType.get
    } else {
      lambdaInfo.getReturnDataType
    }
    val body = if (needsCoercion) {
      unresolvedCall(
        BuiltInFunctionDefinitions.CAST,
        lambdaInfo.getBody,
        typeLiteral(bodyResultDataType))
    } else {
      lambdaInfo.getBody
    }
    val returnDataType = if (internalLambdaData) {
      DataTypeUtils.toInternalDataType(bodyResultDataType)
    } else {
      bodyResultDataType
    }
    val captureCount = captureOperands.size
    val paramFields = allParamFields.dropRight(captureCount)
    val captureFields = allParamFields.takeRight(captureCount)
    // The conversion class of the FUNCTION type is the functional interface the receiving function
    // declared, so it settles both the arity and the representation of the function object.
    val lambdaSpec = LambdaFactorySpec(paramFields.size, lambdaDataType.getConversionClass)
    val interfaceClass = typeTerm(lambdaSpec.interfaceClass)

    // compile the lambda body into an evaluator and open it in the operator
    val evaluator = evaluatorFactory match {
      case defaultFactory: DefaultExpressionEvaluatorFactory =>
        defaultFactory.createLambdaEvaluator(body, returnDataType, lambdaSpec, allParamFields)
      case _ =>
        throw new CodeGenException(
          s"A lambda argument requires a ${classOf[DefaultExpressionEvaluatorFactory].getSimpleName}, " +
            s"but got '${evaluatorFactory.getClass.getName}'.")
    }
    val evaluatorTerm =
      ctx.addReusableObject(
        evaluator,
        "lambdaEvaluator",
        classOf[DefaultExpressionEvaluator].getCanonicalName)
    val factoryTerm = newName(ctx, "lambdaFactory")
    ctx.addReusableMember(s"private transient ${className[LambdaFunctionFactory]} $factoryTerm;")
    // A lambda may itself be nested in another lambda body; the enclosing generated class is then an
    // expression evaluator without a runtime context, which supplies a context of its own.
    ctx.addReusableOpenStatement(
      s"$factoryTerm = $evaluatorTerm.openLambdaFactory(${ctx.functionContextCode()});")
    // The framework owns the evaluator's lifecycle: it is closed with the enclosing generated class
    // so that a function called from the lambda body is closed like anywhere else.
    ctx.addReusableCloseStatement(s"$evaluatorTerm.close();")

    // Evaluate the captured values and convert each to the class the compiled body expects.
    // Captures are not necessarily loop-invariant: an enclosing lambda parameter capture changes per
    // outer iteration, and the function object is then rebuilt for each iteration.
    val captureBindings = captureOperands.zip(captureFields).map {
      case (operand, field) =>
        val externalTerm = genToExternalConverterAll(ctx, field.getDataType, operand)
        val captureTerm = newName(ctx, "lambdaCapture")
        val captureClass = typeTerm(field.getDataType.getConversionClass)
        val bindingCode =
          s"""
             |${operand.code}
             |final $captureClass $captureTerm = ($captureClass) ($externalTerm);
             |""".stripMargin
        (captureTerm, bindingCode)
    }
    val captureCode = captureBindings.map(_._2).mkString("\n")
    val captureArray = captureBindings.map(_._1).mkString(", ")

    val funcTerm = newName(ctx, "lambdaFunction")
    // The captures are boxed into one array per function object, i.e. once per row, and unpacked
    // behind the object. The per-element path is a plain interface call into the compiled body, so
    // it carries neither the Object[] allocation nor the asType adaptation that a MethodHandle's
    // invokeWithArguments performs on every application.
    val code =
      s"""
         |$captureCode
         |final $interfaceClass $funcTerm =
         |    ($interfaceClass) $factoryTerm.bindLambda(new Object[] {$captureArray});
         |""".stripMargin
    ExternalGeneratedExpression.fromGeneratedExpression(
      lambdaDataType,
      funcTerm,
      code,
      GeneratedExpression(funcTerm, NEVER_NULL, NO_CODE, lambdaDataType.getLogicalType))
  }

  private def verifyArgumentTypes(
      operandTypes: Seq[LogicalType],
      enrichedDataTypes: Seq[DataType]): Unit = {
    val enrichedTypes = enrichedDataTypes.map(_.getLogicalType)
    operandTypes.zip(enrichedTypes).foreach {
      case (operandType, enrichedType) =>
        // check that the logical type has not changed during the enrichment
        if (!supportsAvoidingCast(operandType, enrichedType)) {
          throw new CodeGenException(
            s"Mismatch of function's argument data type '$enrichedType' and actual " +
              s"argument type '$operandType'.")
        }
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    enrichedDataTypes.foreach(validateOutputDataType)
  }

  def verifyFunctionAwareOutputType(
      returnType: LogicalType,
      enrichedDataType: DataType,
      udf: UserDefinedFunction): Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    if (
      (udf.getKind == FunctionKind.TABLE || udf.getKind == FunctionKind.ASYNC_TABLE || udf.getKind == FunctionKind.PROCESS_TABLE) && !isCompositeType(
        enrichedType)
    ) {
      // logically table functions wrap atomic types into ROW, however, the physical function might
      // return an atomic type
      Preconditions.checkState(
        returnType.is(LogicalTypeRoot.ROW) && returnType.getChildren.size() == 1,
        "Logical output type of function call should be a ROW wrapping an atomic type.",
        Seq(): _*
      )
      val atomicOutputType = returnType.asInstanceOf[RowType].getChildren.get(0)
      verifyOutputType(atomicOutputType, enrichedDataType)
    } else if (
      udf.getKind == FunctionKind.TABLE || udf.getKind == FunctionKind.PROCESS_TABLE || udf.getKind == FunctionKind.ASYNC_TABLE
    ) {
      // null values are skipped therefore, the result top level row will always be not null
      verifyOutputType(returnType.copy(true), enrichedDataType)
    } else {
      verifyOutputType(returnType, enrichedDataType)
    }
  }

  private def verifyOutputType(returnType: LogicalType, enrichedDataType: DataType): Unit = {
    val enrichedType = enrichedDataType.getLogicalType
    // check that the logical type has not changed during the enrichment
    if (!supportsAvoidingCast(enrichedType, returnType)) {
      throw new CodeGenException(
        s"Mismatch of expected output data type '$returnType' and function's " +
          s"output type '$enrichedType'.")
    }
    // the data type class can only partially verify the conversion class,
    // now is the time for the final check
    validateInputDataType(enrichedDataType)
  }

  def verifyFunctionAwareImplementation(
      argumentDataTypes: Seq[DataType],
      outputDataType: DataType,
      udf: UserDefinedFunction,
      functionName: String): Unit = {
    if (udf.getKind == FunctionKind.TABLE) {
      verifyImplementation(TABLE_EVAL, argumentDataTypes, None, udf, functionName)
    } else if (udf.getKind == FunctionKind.ASYNC_TABLE) {
      verifyImplementation(
        ASYNC_TABLE_EVAL,
        DataTypes.NULL.bridgedTo(classOf[CompletableFuture[_]]) +: argumentDataTypes,
        None,
        udf,
        functionName)
    } else if (udf.getKind == FunctionKind.SCALAR) {
      verifyImplementation(SCALAR_EVAL, argumentDataTypes, Some(outputDataType), udf, functionName)
    } else if (udf.getKind == FunctionKind.ASYNC_SCALAR) {
      verifyImplementation(
        ASYNC_SCALAR_EVAL,
        DataTypes.NULL.bridgedTo(classOf[CompletableFuture[_]]) +: argumentDataTypes,
        None,
        udf,
        functionName)
    } else {
      throw new CodeGenException(
        s"Unsupported function kind '${udf.getKind}' for function '$functionName'.")
    }
  }

  private def verifyImplementation(
      methodName: String,
      argumentDataTypes: Seq[DataType],
      outputDataType: Option[DataType],
      udf: UserDefinedFunction,
      functionName: String): Unit = {
    val argumentClasses = argumentDataTypes.map(_.getConversionClass).toArray
    val outputClass = outputDataType.map(_.getConversionClass).getOrElse(classOf[Unit])
    validateClassForRuntime(udf.getClass, methodName, argumentClasses, outputClass, functionName)
  }

  /**
   * Describes the function object a lambda body is compiled for: the functional interface it
   * implements and how many of the evaluator's leading arguments that interface exposes. The
   * remaining arguments are the lifted captures, which are bound once per function object.
   */
  case class LambdaFactorySpec(userParamCount: Int, interfaceClass: Class[_])

  class DefaultExpressionEvaluatorFactory(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      rexFactory: RexFactory,
      parentCtx: CodeGeneratorContext)
    extends ExpressionEvaluatorFactory {

    override def createEvaluator(
        function: BuiltInFunctionDefinition,
        outputDataType: DataType,
        args: DataType*): ExpressionEvaluator = {
      val (argFields, call) = function match {
        case BuiltInFunctionDefinitions.CAST | BuiltInFunctionDefinitions.TRY_CAST =>
          Preconditions.checkArgument(args.length == 1, "Casting expects one arguments.", Seq(): _*)
          val field = DataTypes.FIELD("arg0", args.head)
          (
            Seq(field),
            unresolvedCall(function, unresolvedRef(field.getName), typeLiteral(outputDataType)))
        case _ =>
          val fields = args.zipWithIndex
            .map { case (dataType, i) => DataTypes.FIELD(s"arg$i", dataType) }
          val argRefs = fields.map(arg => unresolvedRef(arg.getName))
          (fields, unresolvedCall(function, argRefs: _*))
      }

      createEvaluator(call, outputDataType, argFields: _*)
    }

    override def createEvaluator(
        sqlExpression: String,
        outputDataType: DataType,
        args: DataTypes.Field*): ExpressionEvaluator = {
      createEvaluator(callSql(sqlExpression), outputDataType, args: _*)
    }

    override def createEvaluator(
        expression: Expression,
        outputDataType: DataType,
        args: DataTypes.Field*): ExpressionEvaluator = {
      createEvaluator(expression, outputDataType, None, args)
    }

    /**
     * Creates an evaluator for a lambda body. The generated class additionally implements
     * [[LambdaFunctionFactory]], so that the function object handed to a higher-order function can
     * call the compiled body directly instead of through a [[java.lang.invoke.MethodHandle]].
     *
     * `args` covers the lambda's user-visible parameters followed by its lifted captures; the
     * function object exposes only the leading `spec.userParamCount` fields.
     */
    def createLambdaEvaluator(
        expression: Expression,
        outputDataType: DataType,
        spec: LambdaFactorySpec,
        args: Seq[DataTypes.Field]): DefaultExpressionEvaluator = {
      createEvaluator(expression, outputDataType, Some(spec), args)
        .asInstanceOf[DefaultExpressionEvaluator]
    }

    private def createEvaluator(
        expression: Expression,
        outputDataType: DataType,
        lambdaSpec: Option[LambdaFactorySpec],
        args: Seq[DataTypes.Field]): ExpressionEvaluator = {
      args.foreach(f => validateInputDataType(f.getDataType))
      validateOutputDataType(outputDataType)

      try {
        createEvaluatorOrError(expression, outputDataType, lambdaSpec, args)
      } catch {
        case t: Throwable =>
          throw new TableException(
            s"Unable to create an expression evaluator for expression: $expression",
            t)
      }
    }

    /**
     * This method generates code and wraps it into a [[DefaultExpressionEvaluator]].
     *
     * For example, executing the following:
     * {{{
     *   createEvaluator("a = b", BOOLEAN(), FIELD("a", DataTypes.INT()), FIELD("b", INT()))
     * }}}
     * would result in:
     * {{{
     * public class ExpressionEvaluator$20 extends org.apache.flink.api.common.functions.AbstractRichFunction {
     *
     *   public ExpressionEvaluator$20(Object[] references) throws Exception {}
     *
     *   public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {}
     *
     *   public void close() throws Exception {}
     *
     *   public java.lang.Boolean eval(java.lang.Integer arg0, java.lang.Integer arg1) throws Exception {
     *     int result$16;
     *     boolean isNull$16;
     *     int result$17;
     *     boolean isNull$17;
     *     boolean isNull$18;
     *     boolean result$19;
     *
     *     isNull$16 = arg0 == null;
     *     result$16 = -1;
     *     if (!isNull$16) {
     *       result$16 = arg0;
     *     }
     *     isNull$17 = arg1 == null;
     *     result$17 = -1;
     *     if (!isNull$17) {
     *       result$17 = arg1;
     *     }
     *
     *     boolean $0IsNull = isNull$16;
     *     int $0 = result$16;
     *
     *     boolean $1IsNull = isNull$17;
     *     int $1 = result$17;
     *
     *     isNull$18 = $0IsNull || $1IsNull;
     *     result$19 = false;
     *     if (!isNull$18) {
     *       result$19 = $0 == $1;
     *     }
     *
     *     return (java.lang.Boolean) (isNull$18 ? null : ((java.lang.Boolean) result$19));
     *   }
     * }
     * }}}
     */
    private def createEvaluatorOrError(
        expression: Expression,
        outputDataType: DataType,
        lambdaSpec: Option[LambdaFactorySpec],
        args: Seq[DataTypes.Field]): ExpressionEvaluator = {
      val argFields = args.map(f => new RowField(f.getName, f.getDataType.getLogicalType))
      val outputType = outputDataType.getLogicalType

      val ctx = new EvaluatorCodeGeneratorContext(tableConfig, classLoader, parentCtx)

      val externalOutputClass = outputDataType.getConversionClass
      val externalOutputTypeTerm = typeTerm(externalOutputClass)

      // arguments
      val externalArgClasses = args
        .map(_.getDataType.getConversionClass)
        .map {
          clazz =>
            // special cases to be in sync with typeTerm(...)
            if (clazz == classOf[StringData]) {
              classOf[BinaryStringData]
            } else if (clazz == classOf[RawValueData[_]]) {
              classOf[BinaryRawValueData[_]]
            } else {
              clazz
            }
        }
      val externalArgTypeTerms = externalArgClasses.map(typeTerm)
      val argsSignatureCode = externalArgTypeTerms.zipWithIndex
        .map { case (t, i) => s"$t arg$i" }
        .mkString(", ")
      val argToInternalExprs = args
        .map(_.getDataType)
        .zipWithIndex
        .map {
          case (argDataType, i) =>
            genToInternalConverterAll(ctx, argDataType, s"arg$i")
        }

      // map arguments
      val argMappingCode = argToInternalExprs.zipWithIndex
        .map {
          case (srcExpr, i) =>
            val newResultTerm = "$" + i
            val newResultTypeTerm = primitiveTypeTermForType(srcExpr.resultType)
            val newNullTerm = newResultTerm + "IsNull"
            s"""
               |boolean $newNullTerm = ${srcExpr.nullTerm};
               |$newResultTypeTerm $newResultTerm = ${srcExpr.resultTerm};
               |""".stripMargin
        }
        .mkString("\n")

      // expression
      val rexNode = rexFactory.convertExpressionToRex(argFields.asJava, expression, outputType)
      val rexNodeType = FlinkTypeFactory.toLogicalType(rexNode.getType)
      if (!supportsAvoidingCast(rexNodeType, outputType)) {
        throw new CodeGenException(
          s"Mismatch between expression type '$rexNodeType' and expected type '$outputType'.")
      }
      val exprCodeGen = new ExprCodeGenerator(ctx, false)
      val genExpr = exprCodeGen.generateExpression(rexNode)

      // output
      val resultTerm = genToExternalConverterAll(ctx, outputDataType, genExpr)
      val externalResultClass = outputDataType.getConversionClass
      val externalResultTypeTerm = typeTerm(externalResultClass)
      val externalResultClassBoxed = primitiveToWrapper(externalResultClass)
      val externalResultCasting = if (externalResultClass == externalResultClassBoxed) {
        s"($externalResultTypeTerm)"
      } else {
        s"($externalResultTypeTerm) (${typeTerm(externalResultClassBoxed)})"
      }

      val evaluatorName = newName(ctx, "ExpressionEvaluator")
      val lambdaFactoryCode = lambdaSpec
        .map(spec => generateBindLambda(spec, externalArgTypeTerms, externalOutputTypeTerm))
        .getOrElse("")
      val lambdaFactoryInterface = lambdaSpec
        .map(_ => s" implements ${className[LambdaFunctionFactory]}")
        .getOrElse("")
      val evaluatorCode =
        s"""
           |public class $evaluatorName extends ${className[AbstractRichFunction]}$lambdaFactoryInterface {
           |
           |  ${ctx.reuseMemberCode()}
           |  ${ctx.reuseInnerClassDefinitionCode()}
           |  public $evaluatorName(Object[] references) throws Exception {
           |    ${ctx.reuseInitCode()}
           |  }
           |
           |  public void open(${className[OpenContext]} openContext) throws Exception {
           |    ${ctx.reuseOpenCode()}
           |  }
           |
           |  public void close() throws Exception {
           |    ${ctx.reuseCloseCode()}
           |  }
           |
           |  $lambdaFactoryCode
           |
           |  public $externalOutputTypeTerm eval($argsSignatureCode) throws Exception {
           |    ${ctx.reuseLocalVariableCode()}
           |    ${argToInternalExprs.map(_.code).mkString("\n")}
           |    $argMappingCode
           |    ${genExpr.code}
           |    return $externalResultCasting ($resultTerm);
           |  }
           |}
           |""".stripMargin

      val genClass = new GeneratedFunction[RichFunction](
        evaluatorName,
        evaluatorCode,
        ctx.references.toArray,
        ctx.tableConfig)
      new DefaultExpressionEvaluator(
        genClass,
        externalResultClass,
        externalArgClasses.toArray,
        rexNode.toString)
    }

    /**
     * Generates the [[LambdaFunctionFactory]] implementation for a lambda body: a `bindLambda`
     * method that converts the lifted capture values once and returns a function object whose
     * `apply` calls the typed `eval` method directly.
     *
     * The captures are cast outside the returned object so that the per-element path is only the
     * interface call, the user-visible argument casts, and `eval` itself.
     *
     * A lambda body is arbitrary generated expression code and may therefore throw a checked
     * exception, which the functional interface's `apply` cannot declare. Such an exception is
     * wrapped so that it still surfaces with the body's own cause attached.
     */
    private def generateBindLambda(
        spec: LambdaFactorySpec,
        externalArgTypeTerms: Seq[String],
        externalOutputTypeTerm: String): String = {
      val userParamCount = spec.userParamCount
      val interfaceClass = typeTerm(spec.interfaceClass)
      val paramTypeTerms = externalArgTypeTerms.take(userParamCount)
      val captureTypeTerms = externalArgTypeTerms.drop(userParamCount)
      val captureTerms = captureTypeTerms.indices.map(i => s"capture$i")
      val captureBindings = captureTerms
        .zip(captureTypeTerms)
        .zipWithIndex
        .map {
          case ((captureTerm, captureTypeTerm), i) =>
            s"final $captureTypeTerm $captureTerm = ($captureTypeTerm) captures[$i];"
        }
        .mkString("\n")
      val applyParams = (0 until userParamCount).map(i => s"Object arg$i").mkString(", ")
      val evalArgs = paramTypeTerms.zipWithIndex.map {
        case (paramTypeTerm, i) => s"($paramTypeTerm) arg$i"
      } ++ captureTerms

      s"""
         |@Override
         |public Object bindLambda(Object[] captures) {
         |  $captureBindings
         |  return new $interfaceClass() {
         |    @Override
         |    public Object apply($applyParams) {
         |      try {
         |        return ($externalOutputTypeTerm) eval(${evalArgs.mkString(", ")});
         |      } catch (RuntimeException lambdaError) {
         |        throw lambdaError;
         |      } catch (Exception lambdaError) {
         |        throw new ${classOf[org.apache.flink.util.FlinkRuntimeException].getCanonicalName}(
         |          "Could not evaluate lambda body.", lambdaError);
         |      }
         |    }
         |  };
         |}
         |""".stripMargin
    }
  }

  /**
   * A context for an expression evaluator's generated class.
   *
   * The parent context is the context of the generated class that holds the evaluator. It is not
   * used for sharing reusable statements (an evaluator is a class of its own) but for sharing the
   * name counter: an evaluator whose expression contains a lambda holds a nested evaluator, and the
   * nested one is compiled with the enclosing evaluator's class loader as its parent. Equal class
   * names would therefore make the nested class resolve to the enclosing one.
   */
  private class EvaluatorCodeGeneratorContext(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      parentCtx: CodeGeneratorContext)
    extends CodeGeneratorContext(tableConfig, classLoader, parentCtx) {

    override def addReusableConverter(
        dataType: DataType,
        classLoaderTerm: String = null): String = {
      super.addReusableConverter(dataType, "this.getClass().getClassLoader()")
    }

    override def addReusableFunction(
        function: UserDefinedFunction,
        functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
        contextArgs: Seq[String] = null): String = {
      super.addReusableFunction(
        function,
        classOf[FunctionContext],
        Seq("null, this.getClass().getClassLoader()", "null"))
    }

    // An evaluator is not an operator and thus has no runtime context.
    override def functionContextCode(): String =
      s"new ${classOf[FunctionContext].getCanonicalName}(" +
        "null, this.getClass().getClassLoader(), null)"
  }
}
