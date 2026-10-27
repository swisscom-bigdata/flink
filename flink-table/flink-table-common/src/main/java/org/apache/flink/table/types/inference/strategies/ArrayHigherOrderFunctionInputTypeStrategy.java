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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInfo;
import org.apache.flink.table.types.inference.RefinableLambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link InputTypeStrategy} for the built-in array higher-order functions {@link
 * BuiltInFunctionDefinitions#ARRAY_TRANSFORM}, {@link BuiltInFunctionDefinitions#ARRAY_FILTER} and
 * {@link BuiltInFunctionDefinitions#ARRAY_REDUCE}.
 *
 * <p>It verifies that the first argument is an {@code ARRAY} and that the lambda argument carries a
 * {@code FUNCTION} type (its parameter types are bound to the array element and, for {@code
 * ARRAY_REDUCE}, the accumulator type while the expression is resolved). The arguments are returned
 * unchanged so that no implicit cast is inserted for the lambda argument.
 *
 * <p>If a required lambda-result {@link LogicalTypeRoot} is given (as it is for {@code
 * ARRAY_FILTER}, whose predicate must be {@code BOOLEAN}), the lambda body type is additionally
 * constrained to that root.
 *
 * <p>{@code ARRAY_REDUCE} types its accumulator as follows. The accumulator starts with the type of
 * the initial value and keeps it when the reducer's result is assignable to it. Otherwise the
 * accumulator is widened once, without loss and within the same type family, to the smallest stable
 * supertype of the initial value and the result: {@code STRING} for character strings, {@code
 * BYTES} for binary strings, {@code DECIMAL(38, s)} for decimals (and integers combined with
 * decimals), and the common type of both otherwise. Constructed types are widened field by field.
 * The body is then re-resolved and checked again; if its result still does not fit, the call is
 * rejected with a hint to cast. A widened accumulator is also the type the initial value is cast to
 * and the result type of the call.
 */
@Internal
class ArrayHigherOrderFunctionInputTypeStrategy implements RefinableLambdaInputTypeStrategy {

    /** The families within which {@code ARRAY_REDUCE}'s accumulator may change its type root. */
    private static final List<LogicalTypeFamily> WIDENING_FAMILIES =
            Arrays.asList(
                    LogicalTypeFamily.CHARACTER_STRING,
                    LogicalTypeFamily.BINARY_STRING,
                    LogicalTypeFamily.EXACT_NUMERIC,
                    LogicalTypeFamily.APPROXIMATE_NUMERIC);

    private final int argumentCount;
    private final int lambdaPos;
    private final @Nullable LogicalTypeRoot requiredLambdaResultRoot;

    ArrayHigherOrderFunctionInputTypeStrategy(int argumentCount, int lambdaPos) {
        this(argumentCount, lambdaPos, null);
    }

    ArrayHigherOrderFunctionInputTypeStrategy(
            int argumentCount, int lambdaPos, @Nullable LogicalTypeRoot requiredLambdaResultRoot) {
        this.argumentCount = argumentCount;
        this.lambdaPos = lambdaPos;
        this.requiredLambdaResultRoot = requiredLambdaResultRoot;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(argumentCount);
    }

    @Override
    public Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos) {
        if (argumentPos != lambdaPos) {
            return Optional.empty();
        }
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final DataType arrayType = argumentDataTypes.get(0);
        if (!(arrayType instanceof CollectionDataType)) {
            return Optional.empty();
        }
        final DataType elementType = ((CollectionDataType) arrayType).getElementDataType();
        if (lambdaPos == 2) {
            // ARRAY_REDUCE: (accumulator = initial value type, element)
            return Optional.of(
                    new ArrayList<>(Arrays.asList(argumentDataTypes.get(1), elementType)));
        }
        // ARRAY_TRANSFORM / ARRAY_FILTER: (element)
        return Optional.of(Collections.singletonList(elementType));
    }

    @Override
    public Optional<DataType> getRequiredLambdaResultType(
            CallContext callContext, int argumentPos) {
        // ARRAY_REDUCE only: the reducer body only has to be assignable to the accumulator, so it
        // may be narrower (an INT body for a BIGINT accumulator). Coerce it to the accumulator's
        // own type -- the first lambda parameter, i.e. the one adjustLambdaParameterTypes may
        // have widened -- so every iteration hands the next one a value its accumulator parameter
        // can hold.
        if (argumentPos != lambdaPos || lambdaPos != 2) {
            return Optional.empty();
        }
        return callContext
                .getLambdaArgument(argumentPos)
                .map(LambdaInfo::getParameterFields)
                .filter(fields -> fields.size() == 2)
                .map(fields -> fields.get(0).getDataType());
    }

    @Override
    public Optional<List<DataType>> adjustLambdaParameterTypes(
            CallContext callContext,
            int argumentPos,
            List<DataType> currentParameterTypes,
            DataType lambdaResultType) {
        // ARRAY_REDUCE only. Two refinements of the accumulator parameter (position 0), both
        // monotonic, so that the resolver reaches a fixed point after at most two adjustments:
        //  - once the reducer body is known to be nullable, the accumulator becomes nullable, so a
        //    later iteration can observe an accumulator that an earlier iteration set to NULL;
        //  - if the body is not assignable to the accumulator, the accumulator is widened once to a
        //    lossless supertype within the same type family (see the class Javadoc). It is widened
        //    at most once: if the re-resolved body still does not fit, inferInputTypes rejects the
        //    call rather than chasing a type that keeps growing.
        if (argumentPos != lambdaPos || lambdaPos != 2 || currentParameterTypes.size() != 2) {
            return Optional.empty();
        }
        final DataType accDataType = currentParameterTypes.get(0);
        final LogicalType accType = accDataType.getLogicalType();
        final LogicalType bodyType = lambdaResultType.getLogicalType();
        final boolean nullable = accType.isNullable() || bodyType.isNullable();
        if (!isAssignable(accType, bodyType) && !isWidened(callContext, accType)) {
            final LogicalType widenedType = widenAccumulator(accType, bodyType);
            if (widenedType != null) {
                return Optional.of(
                        Arrays.asList(
                                TypeConversions.fromLogicalToDataType(widenedType.copy(nullable)),
                                currentParameterTypes.get(1)));
            }
        }
        if (nullable && !accType.isNullable()) {
            return Optional.of(Arrays.asList(accDataType.nullable(), currentParameterTypes.get(1)));
        }
        return Optional.empty();
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentDataTypes.size() != argumentCount) {
            return Optional.empty();
        }
        if (!argumentDataTypes.get(0).getLogicalType().is(LogicalTypeRoot.ARRAY)) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "The first argument of a higher-order array function must be an ARRAY.");
            }
            return Optional.empty();
        }
        if (!argumentDataTypes.get(lambdaPos).getLogicalType().is(LogicalTypeRoot.FUNCTION)) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "The argument at position %d must be a lambda expression.", lambdaPos);
            }
            return Optional.empty();
        }
        if (requiredLambdaResultRoot != null) {
            final LogicalType lambdaResultType =
                    LambdaStrategyUtils.requireLambdaResultType(callContext, lambdaPos);
            if (!lambdaResultType.is(requiredLambdaResultRoot)) {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "The lambda expression at position %d must return %s, but its body"
                                    + " returns %s.",
                            lambdaPos,
                            requiredLambdaResultRoot,
                            lambdaResultType.asSummaryString());
                }
                return Optional.empty();
            }
        }
        if (argumentCount == 3) {
            // ARRAY_REDUCE: the reducer body must be assignable to the accumulator without
            // narrowing, otherwise the result cannot safely be folded back into the accumulator.
            // The accumulator is the lambda's first parameter: the type of the initial value
            // (argument 1), possibly widened by adjustLambdaParameterTypes. Assignability is
            // decided by Flink's common type derivation rather than supportsImplicitCast, so it
            // respects all logical-type attributes (decimal precision/scale, char/binary length,
            // timestamp precision, and the children of constructed types). Nullability is ignored
            // here: it is captured by the result type strategy, and the initial value may be NOT
            // NULL while the body is nullable.
            final LogicalType initialType = argumentDataTypes.get(1).getLogicalType();
            final LogicalType accType = accumulatorType(callContext).orElse(initialType);
            final LogicalType bodyType =
                    LambdaStrategyUtils.requireLambdaResultType(callContext, lambdaPos);
            if (!isAssignable(accType, bodyType)) {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "%s", reducerMismatchMessage(initialType, accType, bodyType));
                }
                return Optional.empty();
            }
            if (!accType.copy(true).equals(initialType.copy(true))) {
                // The accumulator was widened: cast the initial value to it. The initial value
                // keeps its own nullability, which the result type strategy accounts for.
                final List<DataType> castTypes = new ArrayList<>(argumentDataTypes);
                castTypes.set(
                        1,
                        TypeConversions.fromLogicalToDataType(
                                accType.copy(initialType.isNullable())));
                return Optional.of(castTypes);
            }
        }
        // Return the argument types unchanged: the lambda argument must not be cast and the other
        // arguments already carry the types the lambda parameters were bound to during resolution.
        return Optional.of(argumentDataTypes);
    }

    /**
     * The type of {@code ARRAY_REDUCE}'s accumulator, i.e. the lambda's first parameter, if the
     * lambda has been bound.
     */
    private Optional<LogicalType> accumulatorType(CallContext callContext) {
        return callContext
                .getLambdaArgument(lambdaPos)
                .map(LambdaInfo::getParameterFields)
                .filter(fields -> fields.size() == 2)
                .map(fields -> fields.get(0).getDataType().getLogicalType());
    }

    /** Whether the accumulator was already widened beyond the type of the initial value. */
    private static boolean isWidened(CallContext callContext, LogicalType accType) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        return argumentDataTypes.size() > 1
                && !argumentDataTypes.get(1).getLogicalType().copy(true).equals(accType.copy(true));
    }

    /**
     * Whether a value of {@code sourceType} can be assigned to {@code targetType} without
     * narrowing, ignoring nullability: their common type must be {@code targetType} itself.
     */
    private static boolean isAssignable(LogicalType targetType, LogicalType sourceType) {
        return LogicalTypeMerging.findCommonType(Arrays.asList(targetType, sourceType))
                .map(commonType -> commonType.copy(true).equals(targetType.copy(true)))
                .orElse(false);
    }

    /**
     * Returns the type {@code ARRAY_REDUCE}'s accumulator is widened to so that it can also hold
     * the reducer's result, or {@code null} if it cannot be widened without loss within its type
     * family. Starting from the common type of both (the accumulator first, so that row field names
     * are kept), every attribute that has to grow is widened to the bound a fold converges to: an
     * accumulator that grows once usually grows again on the next iteration (a concatenation, or
     * the precision of a decimal sum), so widening it by just enough for one step would not be
     * stable.
     */
    private static @Nullable LogicalType widenAccumulator(
            LogicalType accType, LogicalType bodyType) {
        final LogicalType commonType =
                LogicalTypeMerging.findCommonType(Arrays.asList(accType, bodyType)).orElse(null);
        if (commonType == null || !isSameFamily(accType, commonType)) {
            return null;
        }
        final LogicalType widenedType = widenToStableType(accType, commonType);
        if (widenedType == null || !isAssignable(widenedType, accType)) {
            return null;
        }
        return widenedType;
    }

    private static @Nullable LogicalType widenToStableType(
            LogicalType accType, LogicalType commonType) {
        if (accType.copy(true).equals(commonType.copy(true))) {
            return commonType;
        }
        final boolean nullable = commonType.isNullable();
        if (commonType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return new VarCharType(nullable, VarCharType.MAX_LENGTH);
        }
        if (commonType.is(LogicalTypeFamily.BINARY_STRING)) {
            return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
        }
        if (commonType.is(LogicalTypeRoot.DECIMAL)) {
            // The common decimal type of two decimals that together need more than 38 digits
            // gives up scale. That would round every accumulated value, so it is not a widening.
            final int scale = LogicalTypeChecks.getScale(commonType);
            if (scale < LogicalTypeChecks.getScale(accType)) {
                return null;
            }
            return new DecimalType(nullable, DecimalType.MAX_PRECISION, scale);
        }
        switch (commonType.getTypeRoot()) {
            case ARRAY:
                {
                    final LogicalType elementType =
                            widenToStableType(
                                    ((ArrayType) accType).getElementType(),
                                    ((ArrayType) commonType).getElementType());
                    return elementType == null ? null : new ArrayType(nullable, elementType);
                }
            case MULTISET:
                {
                    final LogicalType elementType =
                            widenToStableType(
                                    ((MultisetType) accType).getElementType(),
                                    ((MultisetType) commonType).getElementType());
                    return elementType == null ? null : new MultisetType(nullable, elementType);
                }
            case MAP:
                {
                    final MapType accMapType = (MapType) accType;
                    final MapType commonMapType = (MapType) commonType;
                    final LogicalType keyType =
                            widenToStableType(accMapType.getKeyType(), commonMapType.getKeyType());
                    final LogicalType valueType =
                            widenToStableType(
                                    accMapType.getValueType(), commonMapType.getValueType());
                    return keyType == null || valueType == null
                            ? null
                            : new MapType(nullable, keyType, valueType);
                }
            case ROW:
                {
                    final List<RowField> accFields = ((RowType) accType).getFields();
                    final List<RowField> commonFields = ((RowType) commonType).getFields();
                    final List<RowField> widenedFields = new ArrayList<>(commonFields.size());
                    for (int i = 0; i < commonFields.size(); i++) {
                        final RowField commonField = commonFields.get(i);
                        final LogicalType fieldType =
                                widenToStableType(
                                        accFields.get(i).getType(), commonField.getType());
                        if (fieldType == null) {
                            return null;
                        }
                        widenedFields.add(
                                new RowField(
                                        commonField.getName(),
                                        fieldType,
                                        commonField.getDescription().orElse(null)));
                    }
                    return new RowType(nullable, widenedFields);
                }
            default:
                return commonType;
        }
    }

    /**
     * Whether widening {@code accType} to {@code commonType} stays within the type family: both are
     * character strings, binary strings, exact numerics or approximate numerics, or they share the
     * type root and, for constructed types, each pair of children stays within its family. An
     * {@code INT} accumulator may thus become a {@code DECIMAL}, but not a {@code DOUBLE}, and a
     * {@code DATE} does not become a {@code TIMESTAMP}.
     */
    private static boolean isSameFamily(LogicalType accType, LogicalType commonType) {
        for (LogicalTypeFamily family : WIDENING_FAMILIES) {
            if (accType.is(family) && commonType.is(family)) {
                return true;
            }
        }
        if (accType.getTypeRoot() != commonType.getTypeRoot()) {
            return false;
        }
        final List<LogicalType> accChildren = accType.getChildren();
        final List<LogicalType> commonChildren = commonType.getChildren();
        if (accChildren.size() != commonChildren.size()) {
            return false;
        }
        for (int i = 0; i < accChildren.size(); i++) {
            if (!isSameFamily(accChildren.get(i), commonChildren.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static String reducerMismatchMessage(
            LogicalType initialType, LogicalType accType, LogicalType bodyType) {
        // The accumulator is reported with the nullability of the initial value: a nullable body
        // has already made the accumulator parameter nullable, which is not what the user wrote.
        final StringBuilder message =
                new StringBuilder(
                                "The reducer of ARRAY_REDUCE must return a type assignable to the"
                                        + " accumulator type ")
                        .append(accType.copy(initialType.isNullable()).asSummaryString());
        if (!accType.copy(true).equals(initialType.copy(true))) {
            message.append(" (widened from the initial value's type ")
                    .append(initialType.asSummaryString())
                    .append(')');
        }
        message.append(", but its body returns ").append(bodyType.asSummaryString()).append('.');
        final LogicalType commonType =
                LogicalTypeMerging.findCommonType(Arrays.asList(accType, bodyType)).orElse(null);
        final String accTypeName = accType.copy(true).asSummaryString();
        final LogicalType furtherWidenedType = widenAccumulator(accType, bodyType);
        if (commonType == null) {
            message.append(
                    String.format(
                            " The two types have no common type. Cast the reducer's result to %s.",
                            accTypeName));
        } else if (furtherWidenedType != null) {
            // Only reachable once the accumulator has been widened: it is widened at most once.
            message.append(
                    String.format(
                            " The accumulator is widened at most once. Cast the initial value to"
                                    + " %s.",
                            furtherWidenedType.copy(true).asSummaryString()));
        } else if (!isSameFamily(accType, commonType)) {
            message.append(
                    String.format(
                            " The accumulator is only widened within its type family. Cast the"
                                    + " initial value to %s to accumulate in that type, or cast the"
                                    + " reducer's result to %s.",
                            commonType.copy(true).asSummaryString(), accTypeName));
        } else {
            message.append(
                    String.format(
                            " The accumulator cannot be widened to hold the result without loss."
                                    + " Cast the reducer's result to %s.",
                            accTypeName));
        }
        return message.toString();
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        final String resultSignature =
                requiredLambdaResultRoot != null ? requiredLambdaResultRoot.name() : "ANY";
        if (argumentCount == 3) {
            // ARRAY_REDUCE's reducer is binary and must return the accumulator type, which starts
            // as the type of the initial value.
            return Collections.singletonList(
                    Signature.of(
                            Argument.of("array", "ARRAY"),
                            Argument.of("initial", "INIT"),
                            Argument.of(
                                    "lambda",
                                    "FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE")));
        }
        return Collections.singletonList(
                Signature.of(
                        Argument.of("array", "ARRAY"),
                        Argument.of("lambda", "FUNCTION(ARRAY_ELEMENT_TYPE)->" + resultSignature)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ArrayHigherOrderFunctionInputTypeStrategy that =
                (ArrayHigherOrderFunctionInputTypeStrategy) o;
        return argumentCount == that.argumentCount
                && lambdaPos == that.lambdaPos
                && requiredLambdaResultRoot == that.requiredLambdaResultRoot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentCount, lambdaPos, requiredLambdaResultRoot);
    }
}
