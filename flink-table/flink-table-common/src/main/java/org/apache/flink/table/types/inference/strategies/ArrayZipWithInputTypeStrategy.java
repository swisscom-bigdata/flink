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
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link InputTypeStrategy} for the built-in {@link BuiltInFunctionDefinitions#ARRAY_ZIP_WITH}
 * ({@code (array1, array2, (x, y) -> body)}).
 *
 * <p>It verifies that the first two arguments are {@code ARRAY}s and that the lambda argument
 * carries a {@code FUNCTION} type (its parameter types are bound to the two array element types,
 * both nullable because the shorter array is padded, while the expression is resolved). The
 * arguments are returned unchanged so that no implicit cast is inserted for the lambda argument.
 */
@Internal
class ArrayZipWithInputTypeStrategy implements LambdaInputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(3);
    }

    @Override
    public Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos) {
        if (argumentPos != 2) {
            return Optional.empty();
        }
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (!(argumentDataTypes.get(0) instanceof CollectionDataType)
                || !(argumentDataTypes.get(1) instanceof CollectionDataType)) {
            return Optional.empty();
        }
        final DataType element1Type =
                ((CollectionDataType) argumentDataTypes.get(0)).getElementDataType();
        final DataType element2Type =
                ((CollectionDataType) argumentDataTypes.get(1)).getElementDataType();
        return Optional.of(Arrays.asList(element1Type.nullable(), element2Type.nullable()));
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentDataTypes.size() != 3) {
            return Optional.empty();
        }
        if (!argumentDataTypes.get(0).getLogicalType().is(LogicalTypeRoot.ARRAY)
                || !argumentDataTypes.get(1).getLogicalType().is(LogicalTypeRoot.ARRAY)) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "The first two arguments of ARRAY_ZIP_WITH must be arrays.");
            }
            return Optional.empty();
        }
        if (!argumentDataTypes.get(2).getLogicalType().is(LogicalTypeRoot.FUNCTION)) {
            if (throwOnFailure) {
                throw callContext.newValidationError(
                        "The argument at position 2 must be a lambda expression.");
            }
            return Optional.empty();
        }
        // Return the argument types unchanged: the lambda argument must not be cast and the other
        // arguments already carry the types the lambda parameters were bound to during resolution.
        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        // The placeholder names are the expected form on both surfaces.
        return Collections.singletonList(
                Signature.of(
                        Argument.of("array1", "ARRAY"),
                        Argument.of("array2", "ARRAY"),
                        Argument.of(
                                "lambda",
                                "FUNCTION(ARRAY1_ELEMENT_TYPE, ARRAY2_ELEMENT_TYPE)->ANY")));
    }
}
