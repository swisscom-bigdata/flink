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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.RefinableLambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for {@link LambdaInputTypeStrategy}, the public extension point a user-defined
 * higher-order function implements to declare a lambda argument. Implementing the interface is the
 * cross-surface contract, so the same function must behave identically in SQL and in the Table API.
 */
class UserImplementedLambdaInputTypeStrategyITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @Test
    void testUserImplementedLambdaInputTypeStrategy() throws Exception {
        // LambdaInputTypeStrategy is the only public way to declare a lambda argument: a function
        // implements the interface itself. The same strategy is consulted by the SQL operand
        // checker and by the Table API expression resolver, so the very same function must bind the
        // same lambda parameter type and produce the same result on both surfaces.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_widening_transform", PublicStrategyArrayTransformFunction.class);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        // x * 3000000000 does not fit in an INT, so it is only well-typed if the strategy's BIGINT
        // parameter type was honoured; the result type is derived from the lambda's return type.
        final Table sqlResult =
                tEnv.sqlQuery("SELECT my_widening_transform(f0, x -> x * 3000000000) FROM t");
        final Table tableApiResult =
                tEnv.from("t")
                        .select(
                                call(
                                        "my_widening_transform",
                                        $("f0"),
                                        new UnresolvedLambdaExpression(
                                                Collections.singletonList("x"),
                                                $("x").times(lit(3000000000L)))));

        assertThat(sqlResult.getResolvedSchema().getColumnDataTypes().get(0))
                .isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT()));
        assertThat(tableApiResult.getResolvedSchema().getColumnDataTypes().get(0))
                .isEqualTo(sqlResult.getResolvedSchema().getColumnDataTypes().get(0));

        final Object sqlValue = collectFirst(sqlResult);
        final Object tableApiValue = collectFirst(tableApiResult);
        assertThat(sqlValue).isEqualTo(new Long[] {3000000000L, 6000000000L, 9000000000L});
        assertThat(tableApiValue).isEqualTo(sqlValue);
    }

    @Test
    void testUserImplementedLambdaInputTypeStrategyReturningNoType() {
        // The failure path of the same public surface must also exist on both surfaces: a strategy
        // that cannot derive its parameter types leaves the lambda unbound, and the call is
        // rejected with a validation error instead of resolving to an untyped lambda.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_underivable_transform", UnderivablePublicStrategyArrayTransformFunction.class);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        // On SQL the call is reported with its argument types. The arguments look valid here --
        // the strategy itself declines to derive -- but the binding code cannot tell a strategy
        // that returns nothing from a sibling argument that offers nothing, and blaming the lambda
        // would misdiagnose the far more common second case.
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_underivable_transform(f0, x -> x + 1) FROM t"))
                .hasStackTraceContaining(
                        "Cannot apply 'my_underivable_transform' to arguments of type "
                                + "'my_underivable_transform(<INTEGER ARRAY>, "
                                + "<FUNCTION(ANY) -> ANY>)'")
                .hasStackTraceContaining(
                        "Supported form(s): my_underivable_transform(ARRAY, (x) -> y)");

        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                call(
                                                        "my_underivable_transform",
                                                        $("f0"),
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x").plus(1)))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Function 'my_underivable_transform' does not accept a lambda expression "
                                + "at position 1.");
    }

    @Test
    void testUserImplementedLambdaInputTypeStrategyWithUnsupportedParameterCount() {
        // Only lambdas with zero to MAX_CONVERTIBLE_PARAMETER_COUNT parameters have a runtime
        // representation. Nothing validates a strategy's derived arity for it, so both surfaces
        // must reject an unsupported arity themselves - with a validation error, not with a
        // conversion, indexing, or code generation failure further down.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction(
                "my_no_parameter_transform", NoParameterTypesFunction.class);
        tEnv.createTemporarySystemFunction(
                "my_too_many_parameter_transform", TooManyParameterTypesFunction.class);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        // a strategy deriving no parameter types is supported (a zero-parameter lambda), so the
        // one-parameter lambda written here is reported as an arity mismatch instead
        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_no_parameter_transform(f0, x -> x + 1) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "The lambda expression at position 1 expects 0 parameter(s) but 1 were "
                                + "provided.");

        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                call(
                                                        "my_no_parameter_transform",
                                                        $("f0"),
                                                        lambda())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The lambda expression expects 0 parameter(s) but 1 were provided.");

        assertThatThrownBy(
                        () ->
                                tEnv.sqlQuery(
                                        "SELECT my_too_many_parameter_transform(f0, x -> x + 1) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "A lambda argument must have between 0 and 4 parameters, but 5 "
                                + "parameter types were derived.");

        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                call(
                                                        "my_too_many_parameter_transform",
                                                        $("f0"),
                                                        lambda())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "A lambda argument must have between 0 and 4 parameters, but 5 "
                                + "parameter types were derived.");
    }

    @Test
    void testInternalLambdaParameterRefinementWithWrongArity() {
        // The parameter refinement pass is internal (RefinableLambdaInputTypeStrategy) and only
        // reached on the Table API path. A strategy that returns a refined list of the wrong arity
        // must be reported as an invalid strategy, never as an indexing failure while the lambda
        // parameters are rebuilt (too few types) and never by silently dropping the surplus (too
        // many types).
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3}));

        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                TooManyRefinedParametersFunction.class,
                                                $("f0"),
                                                lambda())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid input type strategy of function "
                                + "'TooManyRefinedParametersFunction'. The refined lambda "
                                + "parameter types at position 1 must have 1 parameter(s) but 2 "
                                + "were returned.");

        assertThatThrownBy(
                        () ->
                                input.select(
                                        call(
                                                TooFewRefinedParametersFunction.class,
                                                $("f0"),
                                                lambda())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid input type strategy of function "
                                + "'TooFewRefinedParametersFunction'. The refined lambda "
                                + "parameter types at position 1 must have 1 parameter(s) but 0 "
                                + "were returned.");
    }

    @Test
    void testLambdaArgumentIsRejectedForAggregateFunction() {
        // A lambda argument is only supported for function kinds whose call compiles into a scalar
        // expression that can host the lambda body. Implementing LambdaInputTypeStrategy is the
        // declaration, so declaring one on an aggregate function must be rejected at validation on
        // both surfaces -- SQL reaches the guard through BridgingSqlAggFunction, the Table API
        // through SystemTypeInference#of -- rather than surfacing as an internal error at planning.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_lambda_agg", LambdaDeclaringAggregateFunction.class);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_lambda_agg(f0, x -> x + 1) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Lambda arguments are not supported for functions of kind 'AGGREGATE'. "
                                + "Only scalar and table functions can declare a lambda argument.");

        assertThatThrownBy(() -> tEnv.from("t").select(call("my_lambda_agg", $("f0"), lambda())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Lambda arguments are not supported for functions of kind 'AGGREGATE'. "
                                + "Only scalar and table functions can declare a lambda argument.");
    }

    private static Expression lambda() {
        return new UnresolvedLambdaExpression(Collections.singletonList("x"), $("x").plus(1));
    }

    private static Object collectFirst(Table table) throws Exception {
        try (final CloseableIterator<Row> iterator = table.execute().collect()) {
            assertThat(iterator).hasNext();
            final Object value = iterator.next().getField(0);
            assertThat(iterator).isExhausted();
            return value;
        }
    }

    /**
     * An {@link InputTypeStrategy} for {@code f(ARRAY<INT>, (BIGINT) -> R)} written by a function
     * author against the public {@link LambdaInputTypeStrategy} contract, using nothing but
     * {@code @PublicEvolving} API. The parameter type is widened to {@code BIGINT} rather than
     * taken from the {@code ARRAY<INT>} argument, so honouring this strategy is observable in the
     * result type and value.
     */
    public static class PublicArrayLambdaInputTypeStrategy implements LambdaInputTypeStrategy {

        private final boolean derivable;

        public PublicArrayLambdaInputTypeStrategy(boolean derivable) {
            this.derivable = derivable;
        }

        @Override
        public Optional<List<DataType>> getExpectedLambdaParameterTypes(
                CallContext callContext, int argumentPos) {
            if (argumentPos != 1 || !derivable) {
                return Optional.empty();
            }
            return Optional.of(Collections.singletonList(DataTypes.BIGINT()));
        }

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(2);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
            if (argumentDataTypes.size() != 2
                    || !argumentDataTypes.get(0).getLogicalType().is(LogicalTypeRoot.ARRAY)) {
                if (throwOnFailure) {
                    throw callContext.newValidationError("Expected an ARRAY and a lambda.");
                }
                return Optional.empty();
            }
            // the arguments are returned unchanged so that the lambda argument is never cast
            return Optional.of(argumentDataTypes);
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return Collections.singletonList(
                    Signature.of(
                            Signature.Argument.of("ARRAY"), Signature.Argument.of("(x) -> y")));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return derivable == ((PublicArrayLambdaInputTypeStrategy) o).derivable;
        }

        @Override
        public int hashCode() {
            return Objects.hash(derivable);
        }
    }

    /**
     * A user-defined {@code ARRAY_TRANSFORM} whose signature is declared through a hand-written
     * {@link LambdaInputTypeStrategy}.
     */
    public static class PublicStrategyArrayTransformFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(new PublicArrayLambdaInputTypeStrategy(true))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            if (array == null) {
                return null;
            }
            final Long[] result = new Long[array.length];
            for (int i = 0; i < array.length; i++) {
                result[i] = array[i] == null ? null : lambda.apply(array[i].longValue());
            }
            return result;
        }
    }

    /**
     * The failure counterpart of {@link PublicStrategyArrayTransformFunction}: its hand-written
     * {@link LambdaInputTypeStrategy} never derives parameter types.
     */
    public static class UnderivablePublicStrategyArrayTransformFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(new PublicArrayLambdaInputTypeStrategy(false))
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.BIGINT())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            return null;
        }
    }

    /**
     * An aggregate function that declares a lambda argument. No aggregate call compiles into a
     * scalar expression that could host the lambda body, so the declaration must be rejected.
     */
    public static class LambdaDeclaringAggregateFunction
            extends AggregateFunction<Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        public void accumulate(Integer accumulator, Integer[] array, Function<Long, Long> lambda) {}

        @Override
        public Integer getValue(Integer accumulator) {
            return accumulator;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(new PublicArrayLambdaInputTypeStrategy(true))
                    .accumulatorTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }

    /**
     * A function whose hand-written strategy derives no lambda parameter type at all, although it
     * declares a lambda argument. Such a lambda has no runtime representation.
     */
    public static class NoParameterTypesFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            new FixedParameterTypesLambdaInputTypeStrategy(Collections.emptyList()))
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.BIGINT())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            return null;
        }
    }

    /**
     * A function whose hand-written strategy derives one lambda parameter type more than {@link
     * org.apache.flink.table.types.logical.FunctionType#MAX_CONVERTIBLE_PARAMETER_COUNT}.
     */
    public static class TooManyParameterTypesFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            new FixedParameterTypesLambdaInputTypeStrategy(
                                    Collections.nCopies(5, DataTypes.BIGINT())))
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.BIGINT())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            return null;
        }
    }

    /**
     * A {@link LambdaInputTypeStrategy} that derives a fixed list of lambda parameter types, which
     * may be of an arity that has no runtime representation.
     */
    public static class FixedParameterTypesLambdaInputTypeStrategy
            extends PublicArrayLambdaInputTypeStrategy {

        private final List<DataType> parameterTypes;

        public FixedParameterTypesLambdaInputTypeStrategy(List<DataType> parameterTypes) {
            super(true);
            this.parameterTypes = parameterTypes;
        }

        @Override
        public Optional<List<DataType>> getExpectedLambdaParameterTypes(
                CallContext callContext, int argumentPos) {
            if (argumentPos != 1) {
                return Optional.empty();
            }
            return Optional.of(parameterTypes);
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o)
                    && parameterTypes.equals(
                            ((FixedParameterTypesLambdaInputTypeStrategy) o).parameterTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), parameterTypes);
        }
    }

    /**
     * A function whose (internal) refinement pass returns more parameter types than the lambda
     * declares, for the malformed-strategy path of {@link RefinableLambdaInputTypeStrategy}.
     */
    public static class TooManyRefinedParametersFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            new BadRefinementLambdaInputTypeStrategy(
                                    Arrays.asList(DataTypes.BIGINT(), DataTypes.BIGINT())))
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.BIGINT())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            return null;
        }
    }

    /**
     * A function whose (internal) refinement pass returns fewer parameter types than the lambda
     * declares. Without validation this would surface as an {@link IndexOutOfBoundsException} while
     * the lambda parameters are rebuilt.
     */
    public static class TooFewRefinedParametersFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            new BadRefinementLambdaInputTypeStrategy(Collections.emptyList()))
                    .outputTypeStrategy(
                            TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.BIGINT())))
                    .build();
        }

        public @Nullable Long[] eval(@Nullable Integer[] array, Function<Long, Long> lambda) {
            return null;
        }
    }

    /**
     * A {@link RefinableLambdaInputTypeStrategy} that refines to the wrong number of parameters.
     */
    public static class BadRefinementLambdaInputTypeStrategy
            extends PublicArrayLambdaInputTypeStrategy implements RefinableLambdaInputTypeStrategy {

        private final List<DataType> refinedParameterTypes;

        public BadRefinementLambdaInputTypeStrategy(List<DataType> refinedParameterTypes) {
            super(true);
            this.refinedParameterTypes = refinedParameterTypes;
        }

        @Override
        public Optional<List<DataType>> adjustLambdaParameterTypes(
                CallContext callContext,
                int argumentPos,
                List<DataType> currentParameterTypes,
                DataType lambdaResultType) {
            return Optional.of(refinedParameterTypes);
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o)
                    && refinedParameterTypes.equals(
                            ((BadRefinementLambdaInputTypeStrategy) o).refinedParameterTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), refinedParameterTypes);
        }
    }
}
