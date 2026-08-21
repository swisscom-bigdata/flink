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

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LambdaExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ModelReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ModelSemantics;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.operations.PartitionQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInfo;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaTypeValidation;
import org.apache.flink.table.types.inference.RefinableLambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.inference.TypeInferenceUtil.Result;
import org.apache.flink.table.types.inference.TypeInferenceUtil.SurroundingInfo;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunction;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasLegacyTypes;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * This rule checks if a {@link UnresolvedCallExpression} can work with the given arguments and
 * infers the output data type. All function calls are resolved {@link CallExpression} after
 * applying this rule.
 *
 * <p>This rule also resolves {@code flatten()} calls on composite types.
 *
 * <p>If the call expects different types of arguments, but the given arguments have types that can
 * be cast, a {@link BuiltInFunctionDefinitions#CAST} expression is inserted.
 *
 * <p>It validates and prepares inline, unregistered {@link UserDefinedFunction}s.
 */
@Internal
final class ResolveCallByArgumentsRule implements ResolverRule {

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        // only the top-level expressions may access the output data type
        final SurroundingInfo surroundingInfo =
                context.getOutputDataType().map(SurroundingInfo::of).orElse(null);
        return expression.stream()
                .flatMap(e -> e.accept(new ResolvingCallVisitor(context, surroundingInfo)).stream())
                .collect(Collectors.toList());
    }

    // --------------------------------------------------------------------------------------------

    private static class ResolvingCallVisitor
            extends RuleExpressionVisitor<List<ResolvedExpression>> {

        private final @Nullable SurroundingInfo surroundingInfo;

        ResolvingCallVisitor(ResolutionContext context, @Nullable SurroundingInfo surroundingInfo) {
            super(context);
            this.surroundingInfo = surroundingInfo;
        }

        @Override
        public List<ResolvedExpression> visit(UnresolvedCallExpression unresolvedCall) {
            final FunctionDefinition definition;
            // clean functions that were not registered in a catalog
            if (unresolvedCall.getFunctionIdentifier().isEmpty()) {
                definition =
                        prepareInlineUserDefinedFunction(unresolvedCall.getFunctionDefinition());
            } else {
                definition = unresolvedCall.getFunctionDefinition();
            }

            final String functionName =
                    unresolvedCall
                            .getFunctionIdentifier()
                            .map(FunctionIdentifier::toString)
                            .orElseGet(definition::toString);

            final TypeInference typeInference = getTypeInferenceOrNull(definition);

            // Reorder named arguments and add replacements for optional ones
            final UnresolvedCallExpression adaptedCall =
                    executeAssignment(functionName, definition, typeInference, unresolvedCall);

            // resolve the children with information from the current call
            final List<ResolvedExpression> resolvedArgs = new ArrayList<>();
            final int argCount = adaptedCall.getChildren().size();

            for (int i = 0; i < argCount; i++) {
                final Expression child = adaptedCall.getChildren().get(i);
                if (child instanceof UnresolvedLambdaExpression) {
                    // A lambda is resolved with its parameter types bound from the preceding
                    // (already resolved) arguments of the enclosing higher-order function.
                    resolvedArgs.add(
                            resolveLambda(
                                    functionName,
                                    definition,
                                    i,
                                    (UnresolvedLambdaExpression) child,
                                    resolvedArgs));
                    continue;
                }
                final SurroundingInfo surroundingInfo;
                if (typeInference == null) {
                    surroundingInfo = null;
                } else {
                    surroundingInfo =
                            SurroundingInfo.of(
                                    functionName,
                                    definition,
                                    typeInference,
                                    argCount,
                                    i,
                                    resolutionContext.isGroupedAggregation());
                }
                final ResolvingCallVisitor childResolver =
                        new ResolvingCallVisitor(resolutionContext, surroundingInfo);
                resolvedArgs.addAll(child.accept(childResolver));
            }

            if (definition == BuiltInFunctionDefinitions.FLATTEN) {
                return executeFlatten(resolvedArgs);
            }

            return Collections.singletonList(
                    runTypeInference(
                            functionName,
                            adaptedCall,
                            typeInference,
                            resolvedArgs,
                            surroundingInfo));
        }

        @Override
        protected List<ResolvedExpression> defaultMethod(Expression expression) {
            if (expression instanceof ResolvedExpression) {
                return Collections.singletonList((ResolvedExpression) expression);
            }
            if (expression instanceof UnresolvedLambdaExpression) {
                // a lambda is resolved by its enclosing call (see visit(UnresolvedCallExpression));
                // reaching this point means it is not an argument of a higher-order function
                throw new ValidationException(
                        String.format(
                                "Unexpected lambda expression: %s. A lambda expression is only "
                                        + "supported as an argument of a function that declares a "
                                        + "lambda argument at this position.",
                                expression));
            }
            throw new TableException("Unexpected unresolved expression: " + expression);
        }

        /**
         * Resolves a lambda argument of a higher-order function. The parameter types are bound from
         * the already resolved sibling arguments and the body is resolved in a scope containing the
         * parameters.
         */
        private ResolvedExpression resolveLambda(
                String functionName,
                FunctionDefinition definition,
                int lambdaPos,
                UnresolvedLambdaExpression lambda,
                List<ResolvedExpression> precedingArgs) {
            List<DataType> parameterTypes =
                    inferLambdaParameterTypes(functionName, definition, lambdaPos, precedingArgs);
            final List<String> names = lambda.getParameterNames();
            if (names.size() != parameterTypes.size()) {
                throw new ValidationException(
                        String.format(
                                "The lambda expression expects %d parameter(s) but %d were "
                                        + "provided.",
                                parameterTypes.size(), names.size()));
            }
            List<LocalReferenceExpression> parameters =
                    buildLambdaParameters(names, parameterTypes);
            ResolvedExpression body =
                    resolutionContext.resolveLambdaBody(lambda.getBody(), parameters);
            // Give the input type strategy a single, monotonic feedback pass: it may refine the
            // parameter types now that the body type is known (ARRAY_REDUCE widens the accumulator
            // parameter to nullable once the reducer body is nullable). If it does, re-resolve the
            // original body with rebuilt local references so the refined types take effect.
            final Optional<List<DataType>> adjusted =
                    adjustLambdaParameterTypes(
                            functionName,
                            definition,
                            lambdaPos,
                            precedingArgs,
                            parameterTypes,
                            body.getOutputDataType());
            if (adjusted.isPresent()) {
                parameterTypes = adjusted.get();
                parameters = buildLambdaParameters(names, parameterTypes);
                body = resolutionContext.resolveLambdaBody(lambda.getBody(), parameters);
            }
            validateLambdaBody(body);
            return new LambdaExpression(parameters, body);
        }

        private List<LocalReferenceExpression> buildLambdaParameters(
                List<String> names, List<DataType> parameterTypes) {
            return IntStream.range(0, names.size())
                    .mapToObj(
                            pos ->
                                    ApiExpressionUtils.localRef(
                                            names.get(pos), parameterTypes.get(pos)))
                    .collect(Collectors.toList());
        }

        /**
         * Rejects the constructs that a lambda body cannot contain. A lambda body is compiled into
         * an expression that is evaluated per element, so an aggregate or OVER window over a lambda
         * <em>parameter</em> has no meaning: the parameter exists per element and there is no group
         * to evaluate it over. The same constructs over the columns of the enclosing query are
         * fine, and do not reach here -- an aggregate is hoisted into the enclosing aggregation
         * before resolution ({@code OperationExpressionsUtils#extractAggregationsAndProperties}),
         * and an OVER window resolves against the enclosing scope's windows. A table function
         * produces rows rather than a value, and an asynchronous function completes its future only
         * after the per-element expression has been evaluated; both are rejected outright. Mirrors
         * the check that {@code SqlValidatorImpl#validateLambda} performs for SQL; without it these
         * would surface as an internal error during expression conversion.
         */
        private void validateLambdaBody(ResolvedExpression body) {
            if (body instanceof CallExpression) {
                final FunctionDefinition definition =
                        ((CallExpression) body).getFunctionDefinition();
                final String construct = unsupportedLambdaBodyConstruct(definition);
                if (construct != null) {
                    if (isUnsupportedInLambdaBody(definition)) {
                        throw new ValidationException(
                                String.format(
                                        "%s are not supported in the body of a lambda expression. "
                                                + "A lambda body must be a scalar expression over "
                                                + "its parameters and the columns it captures.",
                                        construct));
                    }
                    if (referencesLambdaParameter(body, Collections.emptySet())) {
                        throw new ValidationException(
                                OperationExpressionsUtils.unsupportedOverLambdaParameter(
                                        construct));
                    }
                }
            }
            body.getResolvedChildren().forEach(this::validateLambdaBody);
        }

        /**
         * Whether {@code expression} references a lambda parameter. A lambda nested below it
         * shadows the parameters it declares: those are bound within it and may legitimately be
         * aggregated over by an aggregate in between. In a resolved lambda body every {@link
         * LocalReferenceExpression} is a lambda parameter, of this lambda or of an enclosing one
         * (see {@code ExpressionResolver#resolveLambdaBody}).
         */
        private static boolean referencesLambdaParameter(
                ResolvedExpression expression, Set<String> shadowed) {
            if (expression instanceof LocalReferenceExpression) {
                return !shadowed.contains(((LocalReferenceExpression) expression).getName());
            }
            if (expression instanceof LambdaExpression) {
                final LambdaExpression lambda = (LambdaExpression) expression;
                final Set<String> nested = new HashSet<>(shadowed);
                lambda.getParameters().forEach(parameter -> nested.add(parameter.getName()));
                return referencesLambdaParameter(lambda.getBody(), nested);
            }
            return expression.getResolvedChildren().stream()
                    .anyMatch(child -> referencesLambdaParameter(child, shadowed));
        }

        /**
         * The user-facing name of the construct that the given function definition represents in a
         * lambda body, or {@code null} if it may appear there unconditionally.
         */
        private @Nullable String unsupportedLambdaBodyConstruct(FunctionDefinition definition) {
            if (definition == BuiltInFunctionDefinitions.OVER) {
                return "OVER windows";
            }
            if (isTableFunction(definition)) {
                return "Table functions";
            }
            switch (definition.getKind()) {
                case AGGREGATE:
                case TABLE_AGGREGATE:
                    return "Aggregate functions";
                case ASYNC_SCALAR:
                    return "Asynchronous scalar functions";
                default:
                    return null;
            }
        }

        /**
         * Whether the construct cannot appear in a lambda body at all, as opposed to only when it
         * involves a lambda parameter: a table function produces rows rather than a value, and an
         * asynchronous function completes a future only after the per-element expression that would
         * consume its result has been evaluated.
         */
        private static boolean isUnsupportedInLambdaBody(FunctionDefinition definition) {
            return isTableFunction(definition) || definition.getKind() == FunctionKind.ASYNC_SCALAR;
        }

        private static boolean isTableFunction(FunctionDefinition definition) {
            switch (definition.getKind()) {
                case TABLE:
                case ASYNC_TABLE:
                case PROCESS_TABLE:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * Binds the lambda parameter types from the preceding (already resolved) arguments by
         * consulting the function's {@link LambdaInputTypeStrategy}. This covers both the built-in
         * higher-order functions and user-defined functions that declare a lambda argument.
         */
        private List<DataType> inferLambdaParameterTypes(
                String functionName,
                FunctionDefinition definition,
                int lambdaPos,
                List<ResolvedExpression> precedingArgs) {
            final TypeInference inference =
                    definition.getTypeInference(resolutionContext.typeFactory());
            final InputTypeStrategy inputTypeStrategy = inference.getInputTypeStrategy();
            if (inputTypeStrategy instanceof LambdaInputTypeStrategy) {
                final CallContext callContext =
                        new TableApiCallContext(
                                resolutionContext.typeFactory(),
                                functionName,
                                definition,
                                precedingArgs,
                                resolutionContext.isGroupedAggregation(),
                                inference.getStaticArguments().orElse(null));
                final Optional<List<DataType>> parameterTypes =
                        ((LambdaInputTypeStrategy) inputTypeStrategy)
                                .getExpectedLambdaParameterTypes(callContext, lambdaPos);
                if (parameterTypes.isPresent()) {
                    // Nothing validates the arity a strategy derives on its behalf.
                    LambdaTypeValidation.checkDerivedParameterTypes(parameterTypes.get());
                    return parameterTypes.get();
                }
            }
            throw new ValidationException(
                    String.format(
                            "Function '%s' does not accept a lambda expression at position %d.",
                            functionName, lambdaPos));
        }

        /**
         * Gives the function's {@link RefinableLambdaInputTypeStrategy} a single feedback pass to
         * refine the lambda parameter types now that the body has been resolved once (see {@link
         * RefinableLambdaInputTypeStrategy#adjustLambdaParameterTypes}). Returns an empty optional
         * when the strategy keeps the current types or does not support refinement at all. The
         * refinement is internal: a user-supplied {@link LambdaInputTypeStrategy} declares its
         * parameter types once, so that it behaves identically in SQL and in the Table API.
         */
        private Optional<List<DataType>> adjustLambdaParameterTypes(
                String functionName,
                FunctionDefinition definition,
                int lambdaPos,
                List<ResolvedExpression> precedingArgs,
                List<DataType> currentParameterTypes,
                DataType lambdaResultType) {
            final TypeInference inference =
                    definition.getTypeInference(resolutionContext.typeFactory());
            final InputTypeStrategy inputTypeStrategy = inference.getInputTypeStrategy();
            if (!(inputTypeStrategy instanceof RefinableLambdaInputTypeStrategy)) {
                return Optional.empty();
            }
            final CallContext callContext =
                    new TableApiCallContext(
                            resolutionContext.typeFactory(),
                            functionName,
                            definition,
                            precedingArgs,
                            resolutionContext.isGroupedAggregation(),
                            inference.getStaticArguments().orElse(null));
            final Optional<List<DataType>> adjusted =
                    ((RefinableLambdaInputTypeStrategy) inputTypeStrategy)
                            .adjustLambdaParameterTypes(
                                    callContext,
                                    lambdaPos,
                                    currentParameterTypes,
                                    lambdaResultType);
            adjusted.ifPresent(
                    types -> {
                        if (types.size() != currentParameterTypes.size()) {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid input type strategy of function '%s'. The "
                                                    + "refined lambda parameter types at position "
                                                    + "%d must have %d parameter(s) but %d were "
                                                    + "returned.",
                                            functionName,
                                            lambdaPos,
                                            currentParameterTypes.size(),
                                            types.size()));
                        }
                    });
            return adjusted;
        }

        private List<ResolvedExpression> executeFlatten(List<ResolvedExpression> args) {
            if (args.size() != 1) {
                throw new ValidationException("Invalid number of arguments for flattening.");
            }
            final ResolvedExpression composite = args.get(0);
            final LogicalType compositeType = composite.getOutputDataType().getLogicalType();
            if (hasLegacyTypes(compositeType)) {
                return flattenLegacyCompositeType(composite);
            }
            return flattenCompositeType(composite);
        }

        private List<ResolvedExpression> flattenCompositeType(ResolvedExpression composite) {
            final DataType dataType = composite.getOutputDataType();
            final LogicalType type = dataType.getLogicalType();
            if (!isCompositeType(type)) {
                return singletonList(composite);
            }
            final List<DataType> fieldDataTypes = DataTypeUtils.flattenToDataTypes(dataType);
            final List<String> fieldNames = DataTypeUtils.flattenToNames(dataType);
            return IntStream.range(0, fieldDataTypes.size())
                    .mapToObj(
                            idx -> {
                                final DataType fieldDataType = fieldDataTypes.get(idx);
                                final DataType nullableFieldDataType;
                                if (type.isNullable()) {
                                    nullableFieldDataType = fieldDataType.nullable();
                                } else {
                                    nullableFieldDataType = fieldDataType;
                                }
                                return resolutionContext
                                        .postResolutionFactory()
                                        .get(
                                                composite,
                                                valueLiteral(fieldNames.get(idx)),
                                                nullableFieldDataType);
                            })
                    .collect(Collectors.toList());
        }

        private List<ResolvedExpression> flattenLegacyCompositeType(ResolvedExpression composite) {
            final TypeInformation<?> resultType =
                    fromDataTypeToLegacyInfo(composite.getOutputDataType());
            if (!(resultType instanceof CompositeType)) {
                return singletonList(composite);
            }
            final CompositeType<?> compositeType = (CompositeType<?>) resultType;
            return IntStream.range(0, resultType.getArity())
                    .mapToObj(
                            idx ->
                                    resolutionContext
                                            .postResolutionFactory()
                                            .get(
                                                    composite,
                                                    valueLiteral(
                                                            compositeType.getFieldNames()[idx]),
                                                    fromLegacyInfoToDataType(
                                                            compositeType.getTypeAt(idx))))
                    .collect(Collectors.toList());
        }

        /** Temporary method until all calls define a type inference. */
        private @Nullable TypeInference getTypeInferenceOrNull(FunctionDefinition definition) {
            final TypeInference inference =
                    definition.getTypeInference(resolutionContext.typeFactory());
            if (inference.getOutputTypeStrategy() != TypeStrategies.MISSING) {
                return SystemTypeInference.of(definition.getKind(), inference);
            } else {
                return null;
            }
        }

        private UnresolvedCallExpression executeAssignment(
                String functionName,
                FunctionDefinition definition,
                @Nullable TypeInference inference,
                UnresolvedCallExpression unresolvedCall) {
            // Assignment cannot be a top-level expression,
            // it must be located within a function call
            if (definition == BuiltInFunctionDefinitions.ASSIGNMENT) {
                throw new ValidationException(
                        "Named arguments via asArgument() can only be used within function calls.");
            }
            // Skip assignment for special calls
            if (inference == null) {
                return unresolvedCall;
            }

            final List<Expression> actualArgs = unresolvedCall.getChildren();
            final List<StaticArgument> declaredArgs = inference.getStaticArguments().orElse(null);

            final Map<String, Expression> namedArgs = collectAssignments(functionName, actualArgs);
            if (namedArgs.isEmpty()) {
                // Use position-based call but append defaults for
                // optional arguments at the end if necessary.
                final List<Expression> reorderedArgs =
                        appendDefaultPositionedArguments(declaredArgs, actualArgs);
                fillInPtfSpecificPositionedArguments(
                        functionName, definition, declaredArgs, reorderedArgs);
                return unresolvedCall.replaceArgs(reorderedArgs);
            }

            if (declaredArgs == null) {
                throw new ValidationException(
                        String.format(
                                "Invalid call to function '%s'. "
                                        + "The function does not support named arguments. "
                                        + "Please pass the arguments based on positions (i.e. without asArgument()).",
                                functionName));
            }

            SystemTypeInference.checkNoSystemArguments(
                    inference.disableSystemArguments(), namedArgs.keySet(), functionName);

            fillInDefaultNamedArguments(declaredArgs, namedArgs);
            fillInPtfSpecificNamedArguments(
                    functionName, definition, declaredArgs, namedArgs, actualArgs);

            try {
                validateAssignments(declaredArgs, namedArgs);
            } catch (ValidationException e) {
                throw new ValidationException(
                        String.format(
                                "Invalid call to function '%s'. If the call uses named arguments, "
                                        + "a valid name has to be provided for all passed arguments. %s",
                                functionName, e.getMessage()));
            }

            final List<Expression> reorderedArgs =
                    declaredArgs.stream()
                            .map(arg -> namedArgs.get(arg.getName()))
                            .collect(Collectors.toList());
            return unresolvedCall.replaceArgs(reorderedArgs);
        }

        private Map<String, Expression> collectAssignments(
                String functionName, List<Expression> actualArgs) {
            final Map<String, Expression> namedArgs = new HashMap<>();
            actualArgs.stream()
                    .map(this::extractAssignment)
                    .filter(Objects::nonNull)
                    .forEach(
                            assignment -> {
                                if (namedArgs.containsKey(assignment.getKey())) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Invalid call to function '%s'. "
                                                            + "Duplicate named argument found: %s",
                                                    functionName, assignment.getKey()));
                                }
                                namedArgs.put(assignment.getKey(), assignment.getValue());
                            });
            return namedArgs;
        }

        private Map.Entry<String, Expression> extractAssignment(Expression e) {
            final List<Expression> children = e.getChildren();
            if (!isFunction(e, BuiltInFunctionDefinitions.ASSIGNMENT) || children.size() != 2) {
                return null;
            }
            final String name = ExpressionUtils.stringValue(children.get(0));
            if (name == null) {
                return null;
            }
            return Map.entry(name, children.get(1));
        }

        private void fillInPtfSpecificNamedArguments(
                String functionName,
                FunctionDefinition definition,
                List<StaticArgument> declaredArgs,
                Map<String, Expression> namedArgs,
                List<Expression> actualArgs) {
            // Since functions can be unregistered (i.e. inline in Table API), the API helps PTFs in
            // finding arguments.
            if (definition.getKind() != FunctionKind.PROCESS_TABLE) {
                return;
            }

            // The 'uid' argument will be derived from the toString of FunctionDefinition.
            // For UDFs, this is the simple class name.
            final Expression uid =
                    namedArgs.get(SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_UID);
            if (isFunction(uid, BuiltInFunctionDefinitions.DEFAULT)
                    && !SystemTypeInference.isInvalidUidForProcessTableFunction(functionName)) {
                namedArgs.put(
                        SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_UID,
                        valueLiteral(functionName));
            }

            // For Table.process() automatically make the table argument named
            final List<StaticArgument> declaredTableArgs =
                    declaredArgs.stream()
                            .filter(declaredArg -> declaredArg.is(StaticArgumentTrait.TABLE))
                            .collect(Collectors.toList());
            final List<Expression> actualTableArgs =
                    actualArgs.stream()
                            .filter(TableReferenceExpression.class::isInstance)
                            .collect(Collectors.toList());
            if (declaredTableArgs.size() == 1 && actualTableArgs.size() == 1) {
                namedArgs.put(declaredTableArgs.get(0).getName(), actualTableArgs.get(0));
            }
        }

        private void fillInPtfSpecificPositionedArguments(
                String functionName,
                FunctionDefinition definition,
                List<StaticArgument> declaredArgs,
                List<Expression> actualArgs) {
            // Since functions can be unregistered (i.e. inline in Table API), the API helps PTFs in
            // finding arguments.
            if (definition.getKind() != FunctionKind.PROCESS_TABLE
                    || declaredArgs.size() != actualArgs.size()) {
                return;
            }
            final int uidPos =
                    actualArgs.size()
                            - 1
                            - SystemTypeInference.PROCESS_TABLE_FUNCTION_ARG_UID_OFFSET;
            final Expression uidArg = actualArgs.get(uidPos);
            if (isFunction(uidArg, BuiltInFunctionDefinitions.DEFAULT)
                    && !SystemTypeInference.isInvalidUidForProcessTableFunction(functionName)) {
                actualArgs.set(uidPos, valueLiteral(functionName));
            }
        }

        private List<Expression> appendDefaultPositionedArguments(
                @Nullable List<StaticArgument> declaredArgs, List<Expression> actualArgs) {
            if (declaredArgs == null || actualArgs.size() >= declaredArgs.size()) {
                return actualArgs;
            }
            final List<Expression> enrichedArgs = new ArrayList<>(actualArgs);
            IntStream.range(actualArgs.size(), declaredArgs.size())
                    .forEach(
                            pos -> {
                                final StaticArgument declaredArg = declaredArgs.get(pos);
                                if (declaredArgs.get(pos).isOptional()) {
                                    enrichedArgs.add(createDefaultExpression(declaredArg));
                                }
                            });
            return enrichedArgs;
        }

        private void fillInDefaultNamedArguments(
                List<StaticArgument> declaredArgs, Map<String, Expression> namedArgs) {
            declaredArgs.forEach(
                    declaredArg -> {
                        if (declaredArg.isOptional()) {
                            namedArgs.putIfAbsent(
                                    declaredArg.getName(), createDefaultExpression(declaredArg));
                        }
                    });
        }

        private Expression createDefaultExpression(StaticArgument declaredArg) {
            // All optional arguments have a type.
            // This is checked in StaticArgument.
            final DataType dataType =
                    declaredArg.getDataType().orElseThrow(IllegalStateException::new);
            return CallExpression.permanent(
                    BuiltInFunctionDefinitions.DEFAULT, List.of(), dataType);
        }

        private void validateAssignments(
                List<StaticArgument> declaredArgs, Map<String, Expression> namedArgs) {
            final Set<String> providedArgs = namedArgs.keySet();
            final Set<String> knownArgs =
                    declaredArgs.stream().map(StaticArgument::getName).collect(Collectors.toSet());
            final Set<String> unknownArgs =
                    providedArgs.stream()
                            .filter(arg -> !knownArgs.contains(arg))
                            .collect(Collectors.toSet());
            if (!unknownArgs.isEmpty()) {
                throw new ValidationException("Unknown argument names: " + unknownArgs);
            }
            final List<StaticArgument> missingArgs =
                    declaredArgs.stream()
                            .filter(arg -> !providedArgs.contains(arg.getName()))
                            .collect(Collectors.toList());
            if (!missingArgs.isEmpty()) {
                throw new ValidationException("Missing required arguments: " + missingArgs);
            }
        }

        private ResolvedExpression runTypeInference(
                String functionName,
                UnresolvedCallExpression unresolvedCall,
                TypeInference inference,
                List<ResolvedExpression> resolvedArgs,
                @Nullable SurroundingInfo surroundingInfo) {
            if (inference == null) {
                throw new TableException(
                        "Could not get a type inference for function: " + functionName);
            }

            final Result inferenceResult =
                    TypeInferenceUtil.runTypeInference(
                            inference,
                            new TableApiCallContext(
                                    resolutionContext.typeFactory(),
                                    functionName,
                                    unresolvedCall.getFunctionDefinition(),
                                    resolvedArgs,
                                    resolutionContext.isGroupedAggregation(),
                                    inference.getStaticArguments().orElse(null)),
                            surroundingInfo);

            final List<ResolvedExpression> adaptedArguments =
                    castArguments(inferenceResult, resolvedArgs);

            return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
        }

        /** Casts the arguments according to the properties of the {@link Result}. */
        private List<ResolvedExpression> castArguments(
                Result inferenceResult, List<ResolvedExpression> resolvedArgs) {

            return IntStream.range(0, resolvedArgs.size())
                    .mapToObj(
                            pos -> {
                                final ResolvedExpression argument = resolvedArgs.get(pos);
                                // A lambda argument carries a FUNCTION type and must never be cast.
                                if (argument instanceof LambdaExpression) {
                                    return argument;
                                }
                                final DataType argumentType = argument.getOutputDataType();
                                final DataType expectedType =
                                        inferenceResult.getExpectedArgumentTypes().get(pos);

                                if (!supportsAvoidingCast(
                                        argumentType.getLogicalType(),
                                        expectedType.getLogicalType())) {
                                    return resolutionContext
                                            .postResolutionFactory()
                                            .cast(argument, expectedType);
                                }
                                return argument;
                            })
                    .collect(Collectors.toList());
        }

        /** Validates and cleans an inline, unregistered {@link UserDefinedFunction}. */
        private FunctionDefinition prepareInlineUserDefinedFunction(FunctionDefinition definition) {
            if (definition instanceof ScalarFunctionDefinition) {
                final ScalarFunctionDefinition sf = (ScalarFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), sf.getScalarFunction());
                return new ScalarFunctionDefinition(sf.getName(), sf.getScalarFunction());
            } else if (definition instanceof TableFunctionDefinition) {
                final TableFunctionDefinition tf = (TableFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), tf.getTableFunction());
                return new TableFunctionDefinition(
                        tf.getName(), tf.getTableFunction(), tf.getResultType());
            } else if (definition instanceof AggregateFunctionDefinition) {
                final AggregateFunctionDefinition af = (AggregateFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), af.getAggregateFunction());
                return new AggregateFunctionDefinition(
                        af.getName(),
                        af.getAggregateFunction(),
                        af.getResultTypeInfo(),
                        af.getAccumulatorTypeInfo());
            } else if (definition instanceof TableAggregateFunctionDefinition) {
                final TableAggregateFunctionDefinition taf =
                        (TableAggregateFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), taf.getTableAggregateFunction());
                return new TableAggregateFunctionDefinition(
                        taf.getName(),
                        taf.getTableAggregateFunction(),
                        taf.getResultTypeInfo(),
                        taf.getAccumulatorTypeInfo());
            } else if (definition instanceof UserDefinedFunction) {
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), (UserDefinedFunction) definition);
            }
            return definition;
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TableApiCallContext implements CallContext {

        private final DataTypeFactory typeFactory;
        private final String functionName;
        private final FunctionDefinition definition;
        private final List<ResolvedExpression> resolvedArgs;
        private final boolean isGroupedAggregation;
        private final @Nullable List<StaticArgument> staticArguments;

        public TableApiCallContext(
                DataTypeFactory typeFactory,
                String functionName,
                FunctionDefinition definition,
                List<ResolvedExpression> resolvedArgs,
                boolean isGroupedAggregation,
                @Nullable List<StaticArgument> staticArguments) {
            this.typeFactory = typeFactory;
            this.functionName = functionName;
            this.definition = definition;
            this.resolvedArgs = resolvedArgs;
            this.isGroupedAggregation = isGroupedAggregation;
            this.staticArguments = staticArguments;
        }

        @Override
        public DataTypeFactory getDataTypeFactory() {
            return typeFactory;
        }

        @Override
        public FunctionDefinition getFunctionDefinition() {
            return definition;
        }

        @Override
        public boolean isArgumentLiteral(int pos) {
            final ResolvedExpression arg = getArgument(pos);
            return arg instanceof ValueLiteralExpression || arg instanceof TypeLiteralExpression;
        }

        @Override
        public boolean isArgumentNull(int pos) {
            final ResolvedExpression arg = getArgument(pos);
            if (isFunction(arg, BuiltInFunctionDefinitions.DEFAULT)) {
                return true;
            }
            if (arg instanceof ValueLiteralExpression) {
                final ValueLiteralExpression literal = (ValueLiteralExpression) arg;
                return literal.isNull();
            }
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
            final ResolvedExpression arg = getArgument(pos);
            if (arg instanceof TypeLiteralExpression) {
                if (!DataType.class.isAssignableFrom(clazz)) {
                    return Optional.empty();
                }
                return Optional.of((T) arg.getOutputDataType());
            }
            if (arg instanceof ValueLiteralExpression) {
                final ValueLiteralExpression literal = (ValueLiteralExpression) arg;
                return literal.getValueAs(clazz);
            }
            return Optional.empty();
        }

        @Override
        public Optional<LambdaInfo> getLambdaArgument(int pos) {
            if (pos >= resolvedArgs.size()) {
                return Optional.empty();
            }
            final ResolvedExpression arg = resolvedArgs.get(pos);
            if (!(arg instanceof LambdaExpression)) {
                return Optional.empty();
            }
            final LambdaExpression lambda = (LambdaExpression) arg;
            final List<DataTypes.Field> parameterFields =
                    lambda.getParameters().stream()
                            .map(p -> DataTypes.FIELD(p.getName(), p.getOutputDataType()))
                            .collect(Collectors.toList());
            // The body is deliberately withheld during type inference, mirroring the SQL surface
            // (CallBindingCallContext), where validation has no Expression body to offer. A
            // function's type inference must therefore derive from the parameter and result types
            // alone, so that it behaves identically on both surfaces. The body becomes available
            // during specialization, which happens in the planner for Table API calls too.
            return Optional.of(
                    new LambdaInfo(null, parameterFields, lambda.getBody().getOutputDataType()));
        }

        @Override
        public Optional<TableSemantics> getTableSemantics(int pos) {
            final StaticArgument staticArg =
                    Optional.ofNullable(staticArguments).map(args -> args.get(pos)).orElse(null);
            if (staticArg == null || !staticArg.is(StaticArgumentTrait.TABLE)) {
                return Optional.empty();
            }
            final ResolvedExpression arg = getArgument(pos);
            if (!(arg instanceof TableReferenceExpression)) {
                return Optional.empty();
            }
            final TableReferenceExpression tableRef = (TableReferenceExpression) arg;
            final TableSemantics semantics =
                    new TableApiTableSemantics(
                            tableRef.getQueryOperation(),
                            DataTypeUtils.removeTimeAttribute(tableRef.getOutputDataType()),
                            staticArg);
            return Optional.of(semantics);
        }

        @Override
        public Optional<ModelSemantics> getModelSemantics(int pos) {
            final StaticArgument staticArg =
                    Optional.ofNullable(staticArguments).map(args -> args.get(pos)).orElse(null);
            if (staticArg == null || !staticArg.is(StaticArgumentTrait.MODEL)) {
                return Optional.empty();
            }
            final ResolvedExpression arg = getArgument(pos);
            if (!(arg instanceof ModelReferenceExpression)) {
                return Optional.empty();
            }
            final ModelReferenceExpression modelRef = (ModelReferenceExpression) arg;
            final ModelSemantics semantics = new TableApiModelSemantics(modelRef);
            return Optional.of(semantics);
        }

        @Override
        public String getName() {
            return functionName;
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            return resolvedArgs.stream()
                    .map(ResolvedExpression::getOutputDataType)
                    .collect(Collectors.toList());
        }

        @Override
        public Optional<String> getArgumentName(int pos) {
            final ResolvedExpression arg = getArgument(pos);

            if (arg instanceof CallExpression) {
                final CallExpression call = (CallExpression) arg;
                if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AS) {
                    final List<ResolvedExpression> children = call.getResolvedChildren();
                    if (children.size() >= 2 && children.get(1) instanceof ValueLiteralExpression) {
                        return ((ValueLiteralExpression) children.get(1)).getValueAs(String.class);
                    }
                }
            }

            return Optional.empty();
        }

        @Override
        public Optional<DataType> getOutputDataType() {
            return Optional.empty();
        }

        @Override
        public boolean isGroupedAggregation() {
            return isGroupedAggregation;
        }

        private ResolvedExpression getArgument(int pos) {
            if (pos >= resolvedArgs.size()) {
                throw new IndexOutOfBoundsException(
                        String.format(
                                "Not enough arguments to access literal at position %d for function '%s'.",
                                pos, functionName));
            }
            return resolvedArgs.get(pos);
        }
    }

    private static class TableApiTableSemantics implements TableSemantics {

        private final QueryOperation operation;
        private final DataType dataType;
        private final StaticArgument staticArg;

        private TableApiTableSemantics(
                QueryOperation operation, DataType dataType, StaticArgument staticArg) {
            this.operation = operation;
            this.dataType = dataType;
            this.staticArg = staticArg;
        }

        @Override
        public DataType dataType() {
            final DataType typed = staticArg.getDataType().orElse(null);
            if (typed != null) {
                // Typed table argument
                return typed;
            }
            // Untyped table arguments
            return dataType;
        }

        @Override
        public int[] partitionByColumns() {
            final PartitionQueryOperation partitionOperation = findPartitionOperation(operation);
            if (partitionOperation == null) {
                return new int[0];
            }
            return partitionOperation.getPartitionKeys();
        }

        @Override
        public int[] orderByColumns() {
            final PartitionQueryOperation partitionOperation = findPartitionOperation(operation);
            if (partitionOperation == null) {
                return new int[0];
            }
            return extractOrderByColumns(partitionOperation);
        }

        @Override
        public SortDirection[] orderByDirections() {
            final PartitionQueryOperation partitionOperation = findPartitionOperation(operation);
            if (partitionOperation == null) {
                return new SortDirection[0];
            }
            return extractOrderByDirections(partitionOperation);
        }

        @Override
        public int timeColumn() {
            return -1;
        }

        @Override
        public Optional<ChangelogMode> changelogMode() {
            return Optional.empty();
        }

        @Override
        public List<int[]> upsertKeyColumns() {
            return Collections.emptyList();
        }

        private PartitionQueryOperation findPartitionOperation(QueryOperation op) {
            if (op instanceof PartitionQueryOperation) {
                return (PartitionQueryOperation) op;
            }
            return null;
        }

        private int[] extractOrderByColumns(PartitionQueryOperation partitionOperation) {
            return partitionOperation.getOrderExpressions().stream()
                    .mapToInt(this::extractFieldIndex)
                    .toArray();
        }

        private SortDirection[] extractOrderByDirections(
                PartitionQueryOperation partitionOperation) {
            return partitionOperation.getOrderExpressions().stream()
                    .map(this::extractSortDirection)
                    .toArray(SortDirection[]::new);
        }

        private int extractFieldIndex(ResolvedExpression orderExpr) {
            // Order expressions are typically CallExpressions wrapping ORDER_ASC or ORDER_DESC
            if (orderExpr instanceof CallExpression) {
                final CallExpression call = (CallExpression) orderExpr;
                if (call.getChildren().size() == 1) {
                    final Expression child = call.getChildren().get(0);
                    if (child instanceof FieldReferenceExpression) {
                        return ((FieldReferenceExpression) child).getFieldIndex();
                    }
                }
            }
            // Fallback: if it's directly a field reference
            if (orderExpr instanceof FieldReferenceExpression) {
                return ((FieldReferenceExpression) orderExpr).getFieldIndex();
            }
            throw new TableException(
                    "Unable to extract field index from order expression: " + orderExpr);
        }

        private SortDirection extractSortDirection(ResolvedExpression orderExpr) {
            // Check if wrapped in ORDER_ASC or ORDER_DESC
            if (orderExpr instanceof CallExpression) {
                final CallExpression call = (CallExpression) orderExpr;
                final FunctionDefinition functionDef = call.getFunctionDefinition();

                if (functionDef == BuiltInFunctionDefinitions.ORDER_DESC) {
                    // DESC defaults to NULLS FIRST in SQL
                    return SortDirection.DESC_NULLS_FIRST;
                } else if (functionDef == BuiltInFunctionDefinitions.ORDER_ASC) {
                    // ASC defaults to NULLS LAST in SQL
                    return SortDirection.ASC_NULLS_LAST;
                }
            }
            // Default is ascending with nulls last
            return SortDirection.ASC_NULLS_LAST;
        }
    }

    private static class TableApiModelSemantics implements ModelSemantics {

        private final ModelReferenceExpression modelRef;

        private TableApiModelSemantics(ModelReferenceExpression modelRef) {
            this.modelRef = modelRef;
        }

        @Override
        public DataType inputDataType() {
            return modelRef.getInputDataType();
        }

        @Override
        public DataType outputDataType() {
            return modelRef.getOutputDataType();
        }
    }
}
