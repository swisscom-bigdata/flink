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

package org.apache.flink.table.types.inference;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SystemTypeInference}. */
class SystemTypeInferenceTest {

    /**
     * Only scalar and table functions compile into a scalar expression that can host a lambda body.
     */
    private static final EnumSet<FunctionKind> LAMBDA_HOSTING_KINDS =
            EnumSet.of(FunctionKind.SCALAR, FunctionKind.TABLE);

    @ParameterizedTest
    @EnumSource(FunctionKind.class)
    void testLambdaArgumentIsRejectedForNonHostingFunctionKind(FunctionKind functionKind) {
        final InputTypeStrategy strategy = new TestLambdaInputTypeStrategy();

        if (LAMBDA_HOSTING_KINDS.contains(functionKind)) {
            assertThatNoException()
                    .isThrownBy(() -> SystemTypeInference.checkLambdaArgs(functionKind, strategy));
            return;
        }

        assertThatThrownBy(() -> SystemTypeInference.checkLambdaArgs(functionKind, strategy))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        String.format(
                                "Lambda arguments are not supported for functions of kind '%s'.",
                                functionKind));
    }

    /**
     * {@link SystemTypeInference#of(FunctionKind, TypeInference)} is the entry point that both the
     * SQL and the Table API surface share, so the guard must also reject there and not only on a
     * direct call to {@link SystemTypeInference#checkLambdaArgs}.
     */
    @ParameterizedTest
    @EnumSource(
            value = FunctionKind.class,
            names = {"ASYNC_SCALAR", "ASYNC_TABLE", "AGGREGATE", "TABLE_AGGREGATE", "OTHER"})
    void testLambdaArgumentIsRejectedByCommonEntryPoint(FunctionKind functionKind) {
        final TypeInference typeInference =
                TypeInference.newBuilder()
                        .inputTypeStrategy(new TestLambdaInputTypeStrategy())
                        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                        .build();

        assertThatThrownBy(() -> SystemTypeInference.of(functionKind, typeInference))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        String.format(
                                "Lambda arguments are not supported for functions of kind '%s'.",
                                functionKind));
    }

    /**
     * A strategy that does not implement {@link LambdaInputTypeStrategy} declares no lambda and
     * must pass the guard for every function kind.
     */
    @ParameterizedTest
    @EnumSource(FunctionKind.class)
    void testNonLambdaStrategyIsAccepted(FunctionKind functionKind) {
        assertThatNoException()
                .isThrownBy(
                        () ->
                                SystemTypeInference.checkLambdaArgs(
                                        functionKind, InputTypeStrategies.WILDCARD));
    }

    @Test
    void testLambdaArgumentIsAcceptedForHostingFunctionKinds() {
        final TypeInference typeInference =
                TypeInference.newBuilder()
                        .inputTypeStrategy(new TestLambdaInputTypeStrategy())
                        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                        .build();

        for (FunctionKind functionKind : LAMBDA_HOSTING_KINDS) {
            assertThat(SystemTypeInference.of(functionKind, typeInference)).isNotNull();
        }
    }

    /**
     * A strategy that declares a lambda by implementing {@link LambdaInputTypeStrategy}. It
     * deliberately derives parameter types for the lambda position, which is what the binding paths
     * read, so the guard cannot be bypassed by a strategy that reports the two differently.
     */
    private static class TestLambdaInputTypeStrategy implements LambdaInputTypeStrategy {

        @Override
        public Optional<List<DataType>> getExpectedLambdaParameterTypes(
                CallContext callContext, int argumentPos) {
            return Optional.of(Collections.singletonList(DataTypes.INT()));
        }

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(1);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            return Optional.of(callContext.getArgumentDataTypes());
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return Collections.singletonList(Signature.of(Signature.Argument.of("(x) -> y")));
        }
    }
}
