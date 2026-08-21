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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Declaration helpers for the higher-order functions used in tests, written the way a function
 * author would write them.
 *
 * <p>Flink does not ship a factory for declaring a lambda argument: {@link LambdaInputTypeStrategy}
 * is the public extension point, and a function author implements it. This class is what an author
 * who writes several such functions would extract for themselves, and it is kept in test scope for
 * exactly that reason — it must not become an API commitment.
 *
 * <p>It deliberately uses <b>only</b> {@code @PublicEvolving} API. That is the property under test:
 * if this class can be written without reaching into internal packages, then so can any
 * user-defined higher-order function.
 */
public final class TestLambdaStrategies {

    // --------------------------------------------------------------------------------------------
    // Signature
    // --------------------------------------------------------------------------------------------

    /**
     * Declares a signature whose arguments are described by the given specs.
     *
     * <p>The returned strategy implements {@link LambdaInputTypeStrategy} if and only if at least
     * one spec {@link ArgumentSpec#isLambda() declares a lambda}. Implementing the interface is
     * what declares a lambda argument, so a signature without one must not implement it.
     */
    public static InputTypeStrategy sequence(ArgumentSpec... specs) {
        final List<ArgumentSpec> specList = Arrays.asList(specs);
        return specList.stream().anyMatch(ArgumentSpec::isLambda)
                ? new LambdaSequenceStrategy(specList)
                : new SequenceStrategy(specList);
    }

    /** One position of a signature. */
    public interface ArgumentSpec {

        /**
         * Validates the argument at the given position and returns the type the function wants it
         * as, or an empty optional if it does not match.
         */
        Optional<DataType> inferArgumentType(
                CallContext callContext, int argumentPos, boolean throwOnFailure);

        /** How this position is rendered in an expected-signature message. */
        String toSignatureString();

        /**
         * The parameter types of the lambda at this position, or an empty optional if this position
         * is not a lambda or the types cannot be derived from the call.
         */
        default Optional<List<DataType>> deriveLambdaParameterTypes(CallContext callContext) {
            return Optional.empty();
        }

        /** Whether this position declares a lambda. */
        default boolean isLambda() {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Non-lambda arguments
    // --------------------------------------------------------------------------------------------

    /** An argument of any type, passed to the function unchanged. */
    public static final ArgumentSpec ANY =
            new ArgumentSpec() {
                @Override
                public Optional<DataType> inferArgumentType(
                        CallContext callContext, int argumentPos, boolean throwOnFailure) {
                    return Optional.of(callContext.getArgumentDataTypes().get(argumentPos));
                }

                @Override
                public String toSignatureString() {
                    return "ANY";
                }
            };

    /** An argument of the given type. The framework casts the call's argument to it if needed. */
    public static ArgumentSpec explicit(DataType expectedDataType) {
        return new ExplicitSpec(expectedDataType);
    }

    /** An argument of the given type root, passed to the function unchanged. */
    public static ArgumentSpec logical(LogicalTypeRoot expectedRoot) {
        return new RootSpec(expectedRoot);
    }

    // --------------------------------------------------------------------------------------------
    // Lambda arguments
    // --------------------------------------------------------------------------------------------

    /** A lambda argument with one parameter type per given derivation. */
    public static ArgumentSpec lambda(ParameterDerivation... derivations) {
        return new LambdaSpec(Arrays.asList(derivations), null);
    }

    /**
     * A lambda argument that the function receives as the given functional interface, i.e. {@link
     * #lambda(ParameterDerivation...)} with an explicit representation of the values the function
     * and the lambda exchange.
     */
    public static ArgumentSpec lambda(
            Class<?> conversionClass, ParameterDerivation... derivations) {
        return new LambdaSpec(Arrays.asList(derivations), conversionClass);
    }

    /** Derives the type of a single lambda parameter from a call. */
    @FunctionalInterface
    public interface ParameterDerivation {

        /** Derives the parameter type, or an empty optional if it cannot be derived. */
        Optional<DataType> derive(CallContext callContext);
    }

    /** The element type of the {@code ARRAY} argument at the given position. */
    public static ParameterDerivation elementOf(int arrayArgumentPos) {
        return callContext -> childOf(callContext, arrayArgumentPos, LogicalTypeRoot.ARRAY, 0);
    }

    /** The type of the argument at the given position, unchanged. */
    public static ParameterDerivation argumentOf(int argumentPos) {
        return callContext -> {
            final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
            if (argumentPos >= argumentDataTypes.size()) {
                return Optional.empty();
            }
            return Optional.of(argumentDataTypes.get(argumentPos));
        };
    }

    /** The key type of the {@code MAP} argument at the given position. */
    public static ParameterDerivation keyOf(int mapArgumentPos) {
        return callContext -> childOf(callContext, mapArgumentPos, LogicalTypeRoot.MAP, 0);
    }

    /** The value type of the {@code MAP} argument at the given position. */
    public static ParameterDerivation valueOf(int mapArgumentPos) {
        return callContext -> childOf(callContext, mapArgumentPos, LogicalTypeRoot.MAP, 1);
    }

    private static Optional<DataType> childOf(
            CallContext callContext, int argumentPos, LogicalTypeRoot expectedRoot, int child) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentPos >= argumentDataTypes.size()) {
            return Optional.empty();
        }
        final DataType argumentDataType = argumentDataTypes.get(argumentPos);
        if (!argumentDataType.getLogicalType().is(expectedRoot)) {
            return Optional.empty();
        }
        return Optional.of(argumentDataType.getChildren().get(child));
    }

    // --------------------------------------------------------------------------------------------

    /**
     * The common behavior of a signature-based strategy. Subclasses differ only in whether they
     * implement {@link LambdaInputTypeStrategy}, so the shared logic cannot drift between them.
     */
    private abstract static class AbstractSequenceStrategy implements InputTypeStrategy {

        final List<ArgumentSpec> specs;

        private AbstractSequenceStrategy(List<ArgumentSpec> specs) {
            this.specs = specs;
        }

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(specs.size());
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            if (callContext.getArgumentDataTypes().size() != specs.size()) {
                return Optional.empty();
            }
            final List<DataType> inferred = new ArrayList<>(specs.size());
            for (int i = 0; i < specs.size(); i++) {
                final Optional<DataType> argumentType =
                        specs.get(i).inferArgumentType(callContext, i, throwOnFailure);
                if (!argumentType.isPresent()) {
                    return Optional.empty();
                }
                inferred.add(argumentType.get());
            }
            return Optional.of(inferred);
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return Collections.singletonList(
                    Signature.of(
                            specs.stream()
                                    .map(spec -> Signature.Argument.of(spec.toSignatureString()))
                                    .collect(Collectors.toList())));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return specs.equals(((AbstractSequenceStrategy) o).specs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(specs);
        }
    }

    /** A signature without a lambda argument, which therefore is not a lambda strategy. */
    private static final class SequenceStrategy extends AbstractSequenceStrategy {

        private SequenceStrategy(List<ArgumentSpec> specs) {
            super(specs);
        }
    }

    /** A signature with at least one lambda argument. */
    private static final class LambdaSequenceStrategy extends AbstractSequenceStrategy
            implements LambdaInputTypeStrategy {

        private LambdaSequenceStrategy(List<ArgumentSpec> specs) {
            super(specs);
        }

        @Override
        public Optional<List<DataType>> getExpectedLambdaParameterTypes(
                CallContext callContext, int argumentPos) {
            if (argumentPos >= specs.size()) {
                return Optional.empty();
            }
            return specs.get(argumentPos).deriveLambdaParameterTypes(callContext);
        }
    }

    private static final class ExplicitSpec implements ArgumentSpec {

        private final DataType expectedDataType;

        private ExplicitSpec(DataType expectedDataType) {
            this.expectedDataType = expectedDataType;
        }

        @Override
        public Optional<DataType> inferArgumentType(
                CallContext callContext, int argumentPos, boolean throwOnFailure) {
            return Optional.of(expectedDataType);
        }

        @Override
        public String toSignatureString() {
            return expectedDataType.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return expectedDataType.equals(((ExplicitSpec) o).expectedDataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expectedDataType);
        }
    }

    private static final class RootSpec implements ArgumentSpec {

        private final LogicalTypeRoot expectedRoot;

        private RootSpec(LogicalTypeRoot expectedRoot) {
            this.expectedRoot = expectedRoot;
        }

        @Override
        public Optional<DataType> inferArgumentType(
                CallContext callContext, int argumentPos, boolean throwOnFailure) {
            final DataType argumentDataType = callContext.getArgumentDataTypes().get(argumentPos);
            if (!argumentDataType.getLogicalType().is(expectedRoot)) {
                return callContext.fail(
                        throwOnFailure,
                        "Unsupported argument type. Expected type root '%s' but actual type was '%s'.",
                        expectedRoot,
                        argumentDataType.getLogicalType().asSummaryString());
            }
            return Optional.of(argumentDataType);
        }

        @Override
        public String toSignatureString() {
            return expectedRoot.name();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return expectedRoot == ((RootSpec) o).expectedRoot;
        }

        @Override
        public int hashCode() {
            return Objects.hash(expectedRoot);
        }
    }

    private static final class LambdaSpec implements ArgumentSpec {

        private final List<ParameterDerivation> derivations;

        private final @Nullable Class<?> conversionClass;

        private LambdaSpec(
                List<ParameterDerivation> derivations, @Nullable Class<?> conversionClass) {
            this.derivations = derivations;
            this.conversionClass = conversionClass;
        }

        @Override
        public boolean isLambda() {
            return true;
        }

        @Override
        public Optional<List<DataType>> deriveLambdaParameterTypes(CallContext callContext) {
            final List<DataType> parameterTypes = new ArrayList<>(derivations.size());
            for (ParameterDerivation derivation : derivations) {
                final Optional<DataType> parameterType = derivation.derive(callContext);
                if (!parameterType.isPresent()) {
                    return Optional.empty();
                }
                parameterTypes.add(parameterType.get());
            }
            return Optional.of(parameterTypes);
        }

        @Override
        public Optional<DataType> inferArgumentType(
                CallContext callContext, int argumentPos, boolean throwOnFailure) {
            final DataType argumentDataType = callContext.getArgumentDataTypes().get(argumentPos);
            if (!argumentDataType.getLogicalType().is(LogicalTypeRoot.FUNCTION)) {
                return callContext.fail(
                        throwOnFailure,
                        "A lambda expression was expected for argument %d.",
                        argumentPos);
            }
            // The lambda itself is never cast; only the representation of the values it exchanges
            // with the function is stated here.
            return Optional.of(
                    conversionClass == null
                            ? argumentDataType
                            : argumentDataType.bridgedTo(conversionClass));
        }

        @Override
        public String toSignatureString() {
            return "FUNCTION";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final LambdaSpec that = (LambdaSpec) o;
            return derivations.equals(that.derivations)
                    && Objects.equals(conversionClass, that.conversionClass);
        }

        @Override
        public int hashCode() {
            return Objects.hash(derivations, conversionClass);
        }
    }

    private TestLambdaStrategies() {}
}
