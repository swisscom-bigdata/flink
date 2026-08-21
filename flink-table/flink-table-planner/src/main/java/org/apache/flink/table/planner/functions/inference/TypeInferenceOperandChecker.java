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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.type.SqlRawTypeNameSpec;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaTypeValidation;
import org.apache.flink.table.types.inference.RefinableLambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter.toRelDataType;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.castArguments;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidCallException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidInputException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

/**
 * A {@link SqlOperandTypeChecker} backed by {@link TypeInference}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
@Internal
public final class TypeInferenceOperandChecker
        implements SqlOperandTypeChecker, SqlOperandMetadata {

    private final DataTypeFactory dataTypeFactory;

    private final FunctionDefinition definition;

    private final TypeInference typeInference;

    private final SqlOperandCountRange countRange;

    public TypeInferenceOperandChecker(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            TypeInference typeInference) {
        this.dataTypeFactory = dataTypeFactory;
        this.definition = definition;
        this.typeInference = typeInference;
        this.countRange = new ArgumentCountRange(deriveArgumentCount(typeInference));
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        final CallContext callContext =
                new CallBindingCallContext(
                        dataTypeFactory,
                        definition,
                        callBinding,
                        null,
                        typeInference.getStaticArguments().orElse(null));
        try {
            return checkOperandTypesOrError(callBinding, callContext);
        } catch (LambdaBindingException e) {
            if (!throwOnFailure) {
                return false;
            }
            if (e.reportWithCallSignature) {
                // Calcite renders the call signature from the validator, which describes an
                // unbound lambda operand as FUNCTION(ANY) -> ... and so can report the actual
                // arguments next to the expected ones. The supported form is this function's
                // generated signature, see #getAllowedSignatures.
                throw callBinding.newValidationSignatureError();
            }
            // Reported as is: an unbound lambda parameter is a plain ANY, so the call's argument
            // types are not renderable and enriching the message would fail on them instead.
            throw new ValidationException(e.getMessage());
        } catch (ValidationException e) {
            if (throwOnFailure) {
                throw createInvalidCallException(callContext, e);
            }
            return false;
        } catch (Throwable t) {
            throw createUnexpectedException(callContext, t);
        }
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return countRange;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return TypeInferenceUtil.generateSignature(typeInference, opName, definition);
    }

    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i) {
        if (typeInference.getStaticArguments().isEmpty()) {
            return false;
        }
        final List<StaticArgument> staticArgs = typeInference.getStaticArguments().get();
        return staticArgs.get(i).isOptional();
    }

    @Override
    public boolean isFixedParameters() {
        // Calcite's parameter check is very strict.
        // (e.g. implicit cast from TIMESTAMP to TIMESTAMP_LTZ is not supported)
        // For now, we only fall back to Calcite's logic if an argument is optional
        // (i.e. the default for functions like PTFs with optional 'uid' argument).
        return typeInference
                .getStaticArguments()
                .map(args -> args.stream().anyMatch(StaticArgument::isOptional))
                .orElse(false);
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return typeInference
                .getStaticArguments()
                .map(
                        args ->
                                args.stream()
                                        .map(arg -> toParamType(typeFactory, arg))
                                        .collect(Collectors.toList()))
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Unsupported function signature. "
                                                + "Function must not be overloaded or use varargs."));
    }

    @Override
    public List<String> paramNames() {
        return typeInference
                .getStaticArguments()
                .map(
                        args ->
                                args.stream()
                                        .map(StaticArgument::getName)
                                        .collect(Collectors.toList()))
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Unsupported function signature. "
                                                + "Function must not be overloaded or use varargs."));
    }

    // --------------------------------------------------------------------------------------------

    private RelDataType toParamType(RelDataTypeFactory typeFactory, StaticArgument arg) {
        final LogicalType type = arg.getDataType().map(DataType::getLogicalType).orElse(null);
        if (type == null) {
            // Untyped table arguments
            return typeFactory.createSqlType(SqlTypeName.ANY);
        }
        if (arg.is(StaticArgumentTrait.TABLE)) {
            // A table argument must be a row type for Calcite,
            // although they might enter the UDF as a structured type
            if (LogicalTypeChecks.isCompositeType(type)) {
                return typeFactory.createStructType(
                        StructKind.FULLY_QUALIFIED,
                        LogicalTypeChecks.getFieldTypes(type).stream()
                                .map(t -> toRelDataType(t, typeFactory))
                                .collect(Collectors.toList()),
                        LogicalTypeChecks.getFieldNames(type));
            }
        }
        return toRelDataType(type, typeFactory);
    }

    private boolean checkOperandTypesOrError(SqlCallBinding callBinding, CallContext callContext) {
        // This call may be nested inside an enclosing lambda whose parameter it depends on: either
        // a
        // nested higher-order function, or an ordinary function applied to the parameter such as
        // IFNULL(param, 0). During the first validation of the enclosing lambda body the parameter
        // is not yet bound (its operands are transiently ANY), so defer: the enclosing operand
        // checker re-validates the lambda body once it has bound its parameter. Mirrors the
        // built-in
        // array higher-order function operand checkers (e.g. ArrayTransformOperandTypeChecker).
        if (hasUnresolvedLambdaParameterOperand(callBinding)) {
            return true;
        }

        // Bind the parameter types of any lambda arguments from the sibling arguments before the
        // regular input type strategy runs, so that the lambda body can be (re)validated with
        // concrete parameter types.
        bindLambdaArguments(callBinding, callContext, typeInference);

        final CallContext castCallContext;
        try {
            castCallContext = castArguments(typeInference, callContext, null);
        } catch (ValidationException e) {
            throw createInvalidInputException(typeInference, callContext, e);
        }

        insertImplicitCasts(callBinding, castCallContext.getArgumentDataTypes());

        return true;
    }

    /**
     * Whether this call depends on an <em>enclosing</em> lambda parameter that has not been bound
     * yet. This happens while validating the body of an enclosing higher-order function's lambda
     * before that lambda's parameter has been bound: the parameter, and hence anything derived from
     * it, is a plain {@code ANY} until the enclosing operand checker binds it and re-validates the
     * body. Any Flink {@link TypeInference}-backed call in that body must be deferred to the later
     * pass rather than fail type inference on the {@code ANY}.
     *
     * <p>The dependency shows up in two places:
     *
     * <ul>
     *   <li>a regular operand that is (or is derived from) the unresolved parameter — a nested
     *       higher-order function whose sibling is the parameter, or an ordinary function applied
     *       to it such as {@code IFNULL(param, 0)};
     *   <li>the body of a lambda operand of this call, i.e. this call is itself a higher-order
     *       function whose lambda captures the enclosing parameter, as in {@code ARRAY_TRANSFORM(a,
     *       e -> my_array_transform(a, x -> x + e))}. This call's <em>own</em> lambda parameters
     *       are legitimately unbound here (they are bound by {@link #bindLambdaArguments} right
     *       after this check), so only references to parameters declared further out count.
     * </ul>
     */
    static boolean hasUnresolvedLambdaParameterOperand(SqlCallBinding callBinding) {
        // Note: SqlCallBinding#operands() permutes named arguments and pads optional ones with
        // DEFAULT, so it must not be indexed into the (raw) call via getOperandType(pos). The type
        // is therefore derived from the operand node itself.
        for (SqlNode operand : callBinding.operands()) {
            if (operand.getKind() == SqlKind.DEFAULT) {
                continue;
            }
            if (operand instanceof SqlLambda) {
                if (capturesUnresolvedLambdaParameter((SqlLambda) operand, callBinding)) {
                    return true;
                }
                continue;
            }
            final RelDataType operandType =
                    callBinding.getValidator().deriveType(callBinding.getScope(), operand);
            if (SqlValidatorUtils.containsUnresolvedLambdaParameter(operandType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the given lambda's body references a lambda parameter declared outside of it that is
     * still unbound. Parameters declared by the lambda itself, or by a lambda nested in its body,
     * are excluded: those are bound by the operand checker of the call that owns them.
     */
    private static boolean capturesUnresolvedLambdaParameter(
            SqlLambda lambda, SqlCallBinding callBinding) {
        final SqlValidator validator = callBinding.getValidator();
        final SqlValidatorScope scope = validator.getLambdaScope(lambda);
        if (scope == null) {
            return false;
        }
        final UnresolvedCaptureFinder finder = new UnresolvedCaptureFinder(validator, scope);
        finder.declare(lambda);
        lambda.getExpression().accept(finder);
        return finder.found;
    }

    /**
     * Visitor that looks for a reference to an unbound lambda parameter declared outside of the
     * visited lambda body. Identifiers declared as parameters by the visited lambda or by a lambda
     * nested in it are skipped, so that only captures of enclosing parameters are reported.
     */
    private static final class UnresolvedCaptureFinder extends SqlBasicVisitor<Void> {

        private final SqlValidator validator;
        private final SqlValidatorScope scope;
        private final Set<String> declaredParameters = new HashSet<>();

        private boolean found;

        private UnresolvedCaptureFinder(SqlValidator validator, SqlValidatorScope scope) {
            this.validator = validator;
            this.scope = scope;
        }

        private void declare(SqlLambda lambda) {
            lambda.getParameters()
                    .forEach(parameter -> declaredParameters.add(parameter.toString()));
        }

        @Override
        public Void visit(SqlIdentifier id) {
            if (found || declaredParameters.contains(id.toString())) {
                return null;
            }
            try {
                if (SqlValidatorUtils.containsUnresolvedLambdaParameter(
                        validator.deriveType(scope, id))) {
                    found = true;
                }
            } catch (RuntimeException e) {
                // An identifier that cannot be resolved at all is not an unbound lambda parameter;
                // regular validation reports it with a proper message.
            }
            return null;
        }

        @Override
        public Void visit(SqlCall call) {
            if (call instanceof SqlLambda) {
                // the nested lambda's parameters are bound by the checker of the call that owns it
                declare((SqlLambda) call);
            }
            return super.visit(call);
        }
    }

    /**
     * Binds the lambda parameter types of a higher-order function's lambda arguments (declared via
     * a {@link org.apache.flink.table.types.inference.LambdaInputTypeStrategy}) from the sibling
     * arguments and re-validates the lambda bodies accordingly. Mirrors the built-in array
     * higher-order function operand checkers for user-defined functions.
     */
    static void bindLambdaArguments(
            SqlCallBinding callBinding, CallContext callContext, TypeInference typeInference) {
        final List<SqlNode> operands = callBinding.operands();
        if (operands.stream().noneMatch(SqlLambda.class::isInstance)) {
            return;
        }
        if (!(typeInference.getInputTypeStrategy() instanceof LambdaInputTypeStrategy)) {
            // The function takes no lambda at all. Reported here because the call's argument types
            // are not renderable: an unbound lambda parameter remains a plain ANY, so the regular
            // "invalid input" message would fail while rendering the signature.
            throw new LambdaBindingException(
                    String.format(
                            "Invalid lambda expression at position %d. Function '%s' does not "
                                    + "accept a lambda expression at any position.",
                            IntStream.range(0, operands.size())
                                    .filter(pos -> operands.get(pos) instanceof SqlLambda)
                                    .findFirst()
                                    .orElseThrow(IllegalStateException::new),
                            callBinding.getOperator().getName()));
        }
        final LambdaInputTypeStrategy lambdaStrategy =
                (LambdaInputTypeStrategy) typeInference.getInputTypeStrategy();
        final SqlValidator validator = callBinding.getValidator();
        final FlinkTypeFactory typeFactory = unwrapTypeFactory(callBinding);

        // This runs in the resolved pass (deferred calls never reach here). A stale operand-type
        // list may have been recorded for this call by SqlOperator#inferReturnType during an
        // earlier deferred pass, while an enclosing lambda parameter was still ANY. Because that
        // list is only refreshed when an operand is ANY, the now-resolved pass never overwrites it.
        // It is stale in two ways: the recorded lambda type was derived from unbound (ANY)
        // parameters, and lifting the lambda's captures both changes its arity and appends the
        // capture operands, so the recorded entries no longer line up with the call's operands.
        // SqlToRelConverter#convertOperands would then cast an operand to the type recorded at that
        // position. Drop it: a lambda operand is never cast and the remaining operands are already
        // validated.
        if (validator instanceof SqlValidatorImpl) {
            ((SqlValidatorImpl) validator).callToOperandTypesMap.remove(callBinding.getCall());
        }
        // An invalid number of arguments is reported by the argument count check, which produces a
        // better message than anything derivable here; leave the lambdas unbound for that pass.
        // Note: the operand count is taken from the nodes rather than from the call context, whose
        // argument data types are not renderable while a lambda parameter is still unbound.
        if (!TypeInferenceUtil.validateArgumentCount(
                typeInference.getInputTypeStrategy().getArgumentCount(), operands.size(), false)) {
            return;
        }
        for (int pos = 0; pos < operands.size(); pos++) {
            final SqlNode operand = operands.get(pos);
            if (!(operand instanceof SqlLambda)) {
                continue;
            }
            final SqlLambda lambda = (SqlLambda) operand;
            final List<DataType> parameterTypes =
                    lambdaStrategy.getExpectedLambdaParameterTypes(callContext, pos).orElse(null);
            // Without expected parameter types the lambda parameters stay unbound and anything
            // derived from them remains a plain ANY, which would later fail deep inside type
            // inference. Report the cause instead.
            if (parameterTypes == null) {
                // The lambda is not necessarily at fault: a sibling argument of the wrong type
                // leaves nothing to derive from. Report the call, not the lambda.
                throw new LambdaBindingException(
                        String.format(
                                "Invalid lambda expression at position %d. Either the function does "
                                        + "not declare a lambda argument at this position, or the "
                                        + "lambda parameter types cannot be derived from the other "
                                        + "arguments.\nExpected signatures are:\n%s",
                                pos,
                                TypeInferenceUtil.generateSignature(
                                        typeInference,
                                        callContext.getName(),
                                        callContext.getFunctionDefinition())),
                        true);
            }
            // Nothing validates the arity a strategy derives on its behalf, so it is checked
            // here before it is compared with the number of declared lambda parameters.
            if (!LambdaTypeValidation.isSupportedParameterCount(parameterTypes.size())) {
                throw new LambdaBindingException(
                        LambdaTypeValidation.derivedParameterCountError(parameterTypes.size()));
            }
            final List<SqlNode> parameters = lambda.getParameters().getList();
            if (parameters.size() != parameterTypes.size()) {
                throw new LambdaBindingException(
                        String.format(
                                "The lambda expression at position %d expects %d parameter(s) but "
                                        + "%d were provided.",
                                pos, parameterTypes.size(), parameters.size()));
            }
            final SqlLambdaScope scope = (SqlLambdaScope) validator.getLambdaScope(lambda);
            bindAndValidate(validator, typeFactory, scope, lambda, parameterTypes);
            refineLambdaParameterTypes(
                    validator,
                    typeFactory,
                    scope,
                    lambda,
                    callContext,
                    lambdaStrategy,
                    pos,
                    parameterTypes);
        }
    }

    /**
     * Binds the given parameter types on the lambda's scope, clears the cached validated types of
     * the lambda body and re-validates it against the new bindings.
     */
    private static void bindAndValidate(
            SqlValidator validator,
            FlinkTypeFactory typeFactory,
            SqlLambdaScope scope,
            SqlLambda lambda,
            List<DataType> parameterTypes) {
        final List<SqlNode> parameters = lambda.getParameters().getList();
        for (int i = 0; i < parameters.size(); i++) {
            final RelDataType parameterRelType =
                    typeFactory.createFieldTypeFromLogicalType(
                            parameterTypes.get(i).getLogicalType());
            scope.getParameterTypes().put(parameters.get(i).toString(), parameterRelType);
        }
        lambda.accept(new TypeRemover(validator));
        validator.validateLambda(lambda);
    }

    /**
     * Gives a {@link RefinableLambdaInputTypeStrategy} its single feedback pass now that the body
     * has been validated once, and re-validates the body against the refined parameter types.
     *
     * <p>This is the SQL-side counterpart of the refinement the expression resolver performs on the
     * Table API path, so a call that reaches the shared strategy from either surface ends up with
     * the same lambda parameter types. {@code ARRAY_REDUCE} is the only user: its accumulator
     * parameter has to become nullable once the reducer body is found to be nullable, because an
     * iteration that produces {@code NULL} carries it into the next one. The pass is monotonic and
     * therefore applied exactly once.
     */
    private static void refineLambdaParameterTypes(
            SqlValidator validator,
            FlinkTypeFactory typeFactory,
            SqlLambdaScope scope,
            SqlLambda lambda,
            CallContext callContext,
            LambdaInputTypeStrategy lambdaStrategy,
            int argumentPos,
            List<DataType> parameterTypes) {
        if (!(lambdaStrategy instanceof RefinableLambdaInputTypeStrategy)) {
            return;
        }
        final DataType bodyDataType = validatedLambdaResultType(validator, lambda);
        // An unresolved body carries no information to refine from. It is re-checked in a later
        // validation pass, once the enclosing lambda parameter it closes over has been bound.
        if (bodyDataType == null) {
            return;
        }
        ((RefinableLambdaInputTypeStrategy) lambdaStrategy)
                .adjustLambdaParameterTypes(callContext, argumentPos, parameterTypes, bodyDataType)
                .ifPresent(
                        refined -> bindAndValidate(validator, typeFactory, scope, lambda, refined));
    }

    /**
     * The validated result type of the lambda body, or {@code null} while it is not resolved yet.
     * The lambda's own type is unresolved during an early validation pass, and its result type
     * stays {@code ANY} while an enclosing lambda parameter the body closes over is still unbound.
     */
    private static @Nullable DataType validatedLambdaResultType(
            SqlValidator validator, SqlLambda lambda) {
        final RelDataType lambdaType = validator.getValidatedNodeTypeIfKnown(lambda);
        if (!(lambdaType instanceof FunctionSqlType)) {
            return null;
        }
        final RelDataType resultType = ((FunctionSqlType) lambdaType).getReturnType();
        if (resultType.getSqlTypeName() == SqlTypeName.ANY) {
            return null;
        }
        return TypeConversions.fromLogicalToDataType(FlinkTypeFactory.toLogicalType(resultType));
    }

    private void insertImplicitCasts(SqlCallBinding callBinding, List<DataType> expectedDataTypes) {
        final FlinkTypeFactory flinkTypeFactory = unwrapTypeFactory(callBinding);
        final List<SqlNode> operands = callBinding.operands();
        for (int i = 0; i < operands.size(); i++) {
            final LogicalType expectedType = expectedDataTypes.get(i).getLogicalType();
            final SqlNode sqlNode = operands.get(i);
            // skip default node
            if (sqlNode.getKind() == SqlKind.DEFAULT) {
                continue;
            }
            // a lambda argument is never cast
            if (sqlNode instanceof SqlLambda) {
                continue;
            }
            final LogicalType argumentType =
                    toLogicalType(SqlTypeUtil.deriveType(callBinding, operands.get(i)));

            if (!supportsAvoidingCast(argumentType, expectedType)) {
                final RelDataType expectedRelDataType =
                        flinkTypeFactory.createFieldTypeFromLogicalType(expectedType);
                final SqlNode castedOperand = castTo(operands.get(i), expectedRelDataType);
                callBinding.getCall().setOperand(i, castedOperand);
                updateInferredType(callBinding.getValidator(), castedOperand, expectedRelDataType);
            }
        }
    }

    /** Adopted from {@link org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion}. */
    private SqlNode castTo(SqlNode node, RelDataType type) {
        final SqlDataTypeSpec dataType;
        if (type instanceof RawRelDataType) {
            dataType = createRawDataTypeSpec((RawRelDataType) type);
        } else {
            dataType = SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable());
        }

        return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node, dataType);
    }

    private SqlDataTypeSpec createRawDataTypeSpec(RawRelDataType type) {
        final RawType<?> rawType = type.getRawType();

        SqlNode className =
                SqlLiteral.createCharString(
                        rawType.getOriginatingClass().getName(), SqlParserPos.ZERO);
        SqlNode serializer =
                SqlLiteral.createCharString(rawType.getSerializerString(), SqlParserPos.ZERO);

        SqlTypeNameSpec rawSpec = new SqlRawTypeNameSpec(className, serializer, SqlParserPos.ZERO);

        return new SqlDataTypeSpec(rawSpec, null, type.isNullable(), SqlParserPos.ZERO);
    }

    /** Adopted from {@link org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion}. */
    private void updateInferredType(SqlValidator validator, SqlNode node, RelDataType type) {
        validator.setValidatedNodeType(node, type);
        final SqlValidatorNamespace namespace = validator.getNamespace(node);
        if (namespace != null) {
            namespace.setType(type);
        }
    }

    /**
     * Visitor that clears the previously validated types of a lambda body so that it can be
     * re-validated after its parameter types have been bound. Mirrors the equivalent helper used by
     * the built-in array higher-order function operand checkers.
     */
    private static class TypeRemover extends SqlBasicVisitor<Void> {
        private final SqlValidator validator;

        private TypeRemover(SqlValidator validator) {
            this.validator = validator;
        }

        @Override
        public Void visit(SqlIdentifier id) {
            validator.removeValidatedNodeType(id);
            return super.visit(id);
        }

        @Override
        public Void visit(SqlCall call) {
            validator.removeValidatedNodeType(call);
            return super.visit(call);
        }
    }

    /**
     * Signals that a lambda argument of a higher-order function could not be bound to parameter
     * types. Such a call cannot be described by its argument types (an unbound lambda parameter
     * remains a plain {@code ANY}), so {@link #checkOperandTypes} reports the message as is instead
     * of enriching it with the expected and actual signatures.
     *
     * <p>When {@link #reportWithCallSignature} is set, the cause is not the lambda itself but the
     * call as a whole -- typically a sibling argument of the wrong type, from which no parameter
     * type can be derived. Blaming the lambda would then misdiagnose the error, so the call is
     * reported with its signature instead. Calcite renders that signature from the validator, which
     * can describe an unbound lambda as {@code FUNCTION(ANY) -> ...} where the call context cannot.
     */
    private static final class LambdaBindingException extends ValidationException {

        private static final long serialVersionUID = 1L;

        private final boolean reportWithCallSignature;

        private LambdaBindingException(String message) {
            this(message, false);
        }

        private LambdaBindingException(String message, boolean reportWithCallSignature) {
            super(message);
            this.reportWithCallSignature = reportWithCallSignature;
        }
    }

    private static ArgumentCount deriveArgumentCount(TypeInference typeInference) {
        final int staticArgs = typeInference.getStaticArguments().map(List::size).orElse(-1);
        if (staticArgs == -1) {
            return typeInference.getInputTypeStrategy().getArgumentCount();
        }
        final int optionalArgs =
                typeInference
                        .getStaticArguments()
                        .map(args -> (int) args.stream().filter(StaticArgument::isOptional).count())
                        .orElse(0);
        return ConstantArgumentCount.between(staticArgs - optionalArgs, staticArgs);
    }
}
