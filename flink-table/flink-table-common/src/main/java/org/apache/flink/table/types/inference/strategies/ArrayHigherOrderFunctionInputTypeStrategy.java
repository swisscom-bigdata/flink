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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

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
 */
@Internal
class ArrayHigherOrderFunctionInputTypeStrategy implements RefinableLambdaInputTypeStrategy {

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
        // own type -- the first lambda parameter, i.e. the one the refinement pass above may
        // already have widened to nullable -- so every iteration hands the next one a value its
        // accumulator parameter can hold.
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
        // ARRAY_REDUCE only: once the reducer body is known to be nullable, widen the accumulator
        // parameter (position 0) to nullable so a later iteration can observe an accumulator that
        // an
        // earlier iteration set to NULL. This mirrors the SQL-side ArrayReduceOperandTypeChecker's
        // second inference pass and is monotonic (nullability only flips from non-null to
        // nullable),
        // so a single adjustment is sufficient. Every other logical-type attribute is preserved.
        if (argumentPos != lambdaPos || lambdaPos != 2 || currentParameterTypes.size() != 2) {
            return Optional.empty();
        }
        final DataType accType = currentParameterTypes.get(0);
        if (lambdaResultType.getLogicalType().isNullable()
                && !accType.getLogicalType().isNullable()) {
            return Optional.of(Arrays.asList(accType.nullable(), currentParameterTypes.get(1)));
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
            // ARRAY_REDUCE: the accumulator type A is the type of the initial value (argument 1).
            // The reducer body must be assignable to A without narrowing, otherwise the result
            // cannot safely be folded back into the accumulator. This mirrors the SQL-side
            // ArrayReduceOperandTypeChecker: the common type of the accumulator and the body must
            // be
            // the accumulator type itself (ignoring only nullability). Deriving the common type
            // rather than calling supportsImplicitCast respects all logical-type attributes
            // (decimal precision/scale, char/binary length, timestamp precision, and the children
            // of
            // constructed types), so e.g. a DECIMAL(10, 2) body is rejected for a DECIMAL(5, 0)
            // accumulator instead of silently overflowing to NULL at runtime. Nullability is
            // ignored
            // here: whether the accumulator becomes NULL is captured by the result type strategy,
            // and
            // the initial value may be NOT NULL while the body is nullable.
            final LogicalType accType = argumentDataTypes.get(1).getLogicalType();
            final LogicalType bodyType =
                    LambdaStrategyUtils.requireLambdaResultType(callContext, lambdaPos);
            final Optional<LogicalType> commonType =
                    LogicalTypeMerging.findCommonType(Arrays.asList(accType, bodyType));
            final boolean assignable =
                    commonType.isPresent()
                            && commonType.get().copy(true).equals(accType.copy(true));
            if (!assignable) {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "The reducer of ARRAY_REDUCE must return a type assignable to the"
                                    + " accumulator type %s, but its body returns %s.",
                            accType.asSummaryString(), bodyType.asSummaryString());
                }
                return Optional.empty();
            }
        }
        // Return the argument types unchanged: the lambda argument must not be cast and the other
        // arguments already carry the types the lambda parameters were bound to during resolution.
        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        final String resultSignature =
                requiredLambdaResultRoot != null ? requiredLambdaResultRoot.name() : "ANY";
        if (argumentCount == 3) {
            // ARRAY_REDUCE's reducer is binary and must return the accumulator type. The argument
            // names mirror the SQL-side ArrayReduceOperandTypeChecker so both surfaces report the
            // same expected form.
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
