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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
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
 * {@link InputTypeStrategy} for the built-in map higher-order functions {@link
 * BuiltInFunctionDefinitions#MAP_FILTER}, {@link BuiltInFunctionDefinitions#MAP_TRANSFORM_KEYS},
 * {@link BuiltInFunctionDefinitions#MAP_TRANSFORM_VALUES} (all {@code (map, (k, v) -> body)}) and
 * {@link BuiltInFunctionDefinitions#MAP_ZIP_WITH} ({@code (map1, map2, (k, v1, v2) -> body)}).
 *
 * <p>It verifies that the leading argument(s) are {@code MAP}s and that the lambda argument carries
 * a {@code FUNCTION} type (its parameter types are bound to the map key and value type(s) while the
 * expression is resolved). For {@code MAP_ZIP_WITH} it additionally generalizes the two key types
 * to their common type, so that maps with compatible key types (e.g. {@code INT} and {@code
 * BIGINT}) are merged like {@code MAP_UNION} does. The lambda argument is always returned unchanged
 * so that no implicit cast is inserted for it.
 *
 * <p>If a required lambda-result {@link LogicalTypeRoot} is given (as it is for {@code MAP_FILTER},
 * whose predicate must be {@code BOOLEAN}), the lambda body type is additionally constrained to
 * that root. This mirrors the SQL-side {@code MapTransformOperandTypeChecker} so the same rule is
 * enforced on both the SQL and Table API paths.
 */
@Internal
class MapHigherOrderFunctionInputTypeStrategy implements LambdaInputTypeStrategy {

    private final int argumentCount;
    private final int lambdaPos;
    private final @Nullable LogicalTypeRoot requiredLambdaResultRoot;

    MapHigherOrderFunctionInputTypeStrategy(int argumentCount, int lambdaPos) {
        this(argumentCount, lambdaPos, null);
    }

    MapHigherOrderFunctionInputTypeStrategy(
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
        if (!(argumentDataTypes.get(0) instanceof KeyValueDataType)) {
            return Optional.empty();
        }
        final KeyValueDataType map1 = (KeyValueDataType) argumentDataTypes.get(0);
        if (lambdaPos == 2) {
            // MAP_ZIP_WITH: (key, value1, value2). A key may be absent from either map, so both
            // value parameters are nullable.
            if (!(argumentDataTypes.get(1) instanceof KeyValueDataType)) {
                return Optional.empty();
            }
            final KeyValueDataType map2 = (KeyValueDataType) argumentDataTypes.get(1);
            // A missing common key type is reported by inferInputTypes, which can describe the
            // mismatch; fall back to the first map's key type so that the lambda body still gets
            // bound and the call fails with that message instead of an unbound parameter.
            final DataType keyType = commonKeyType(map1, map2).orElseGet(map1::getKeyDataType);
            return Optional.of(
                    Arrays.asList(
                            keyType,
                            map1.getValueDataType().nullable(),
                            map2.getValueDataType().nullable()));
        }
        // MAP_FILTER / MAP_TRANSFORM_KEYS / MAP_TRANSFORM_VALUES: (key, value)
        return Optional.of(Arrays.asList(map1.getKeyDataType(), map1.getValueDataType()));
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentDataTypes.size() != argumentCount) {
            return Optional.empty();
        }
        for (int i = 0; i < lambdaPos; i++) {
            if (!argumentDataTypes.get(i).getLogicalType().is(LogicalTypeRoot.MAP)) {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "The argument at position %d of a higher-order map function must be a"
                                    + " MAP.",
                            i);
                }
                return Optional.empty();
            }
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
        if (lambdaPos == 2) {
            // MAP_ZIP_WITH: the two maps are merged over the union of their keys, so both key
            // types are generalized to their common type and each map is implicitly cast to it
            // (as MAP_UNION does for the whole map type). Code generation materializes the keys of
            // both maps under one key wrapper and looks the union keys up in it, which requires
            // both sides to carry the same key type; the cast inserted here establishes that and
            // makes the widening visible in the plan.
            if (!(argumentDataTypes.get(0) instanceof KeyValueDataType)
                    || !(argumentDataTypes.get(1) instanceof KeyValueDataType)) {
                return Optional.empty();
            }
            final KeyValueDataType map1 = (KeyValueDataType) argumentDataTypes.get(0);
            final KeyValueDataType map2 = (KeyValueDataType) argumentDataTypes.get(1);
            final Optional<DataType> keyType = commonKeyType(map1, map2);
            if (!keyType.isPresent()) {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "The two maps of MAP_ZIP_WITH must have a common key type, but the key"
                                    + " types are %s and %s. Please CAST one of the maps to align"
                                    + " the key types.",
                            map1.getKeyDataType().getLogicalType().asSummaryString(),
                            map2.getKeyDataType().getLogicalType().asSummaryString());
                }
                return Optional.empty();
            }
            final List<DataType> coercedTypes = new ArrayList<>(argumentDataTypes);
            coercedTypes.set(0, withKeyType(map1, keyType.get()));
            coercedTypes.set(1, withKeyType(map2, keyType.get()));
            return Optional.of(coercedTypes);
        }
        // Return the argument types unchanged: the lambda argument must not be cast and the other
        // arguments already carry the types the lambda parameters were bound to during resolution.
        return Optional.of(argumentDataTypes);
    }

    /**
     * Returns the key type of the map produced by {@code MAP_ZIP_WITH(map1, map2, ...)}, which is
     * also the type its lambda's key parameter is bound to and the type both maps are cast to. Keys
     * from both maps end up in one merged map, so the common type is derived exactly like for any
     * other Flink function that generalizes its arguments (e.g. {@code MAP_UNION}): implicitly
     * castable key types such as {@code INT} and {@code BIGINT} are widened, and the result is
     * nullable if either map's key type is. Returns an empty {@link Optional} if the two key types
     * have no common type at all.
     */
    static Optional<DataType> commonKeyType(KeyValueDataType map1, KeyValueDataType map2) {
        final DataType key1 = map1.getKeyDataType();
        final DataType key2 = map2.getKeyDataType();
        final LogicalType key1Type = key1.getLogicalType();
        final LogicalType key2Type = key2.getLogicalType();
        if (key1Type.copy(true).equals(key2Type.copy(true))) {
            // Identical apart from nullability: keep the declared data type (and its conversion
            // class) instead of deriving a new one, so that no cast is inserted for it.
            return Optional.of(key2Type.isNullable() ? key1.nullable() : key1);
        }
        return LogicalTypeMerging.findCommonType(Arrays.asList(key1Type, key2Type))
                .map(TypeConversions::fromLogicalToDataType);
    }

    private static DataType withKeyType(KeyValueDataType map, DataType keyType) {
        final DataType mapType = DataTypes.MAP(keyType, map.getValueDataType());
        return map.getLogicalType().isNullable() ? mapType.nullable() : mapType.notNull();
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        final String resultSignature =
                requiredLambdaResultRoot != null ? requiredLambdaResultRoot.name() : "ANY";
        if (lambdaPos == 2) {
            return Collections.singletonList(
                    Signature.of(
                            Argument.of("map1", "MAP"),
                            Argument.of("map2", "MAP"),
                            Argument.of(
                                    "lambda",
                                    "FUNCTION(MAP_KEY_TYPE, MAP1_VALUE_TYPE, MAP2_VALUE_TYPE)->"
                                            + resultSignature)));
        }
        return Collections.singletonList(
                Signature.of(
                        Argument.of("map", "MAP"),
                        Argument.of(
                                "lambda",
                                "FUNCTION(MAP_KEY_TYPE, MAP_VALUE_TYPE)->" + resultSignature)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MapHigherOrderFunctionInputTypeStrategy that =
                (MapHigherOrderFunctionInputTypeStrategy) o;
        return argumentCount == that.argumentCount
                && lambdaPos == that.lambdaPos
                && requiredLambdaResultRoot == that.requiredLambdaResultRoot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentCount, lambdaPos, requiredLambdaResultRoot);
    }
}
