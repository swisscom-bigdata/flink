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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a user-defined function cannot declare a lambda argument, as lambda arguments are
 * currently only supported by built-in functions.
 */
class UserDefinedFunctionLambdaArgumentTest {

    private static final String EXPECTED_MESSAGE =
            "Invalid lambda expression at position 1. Lambda arguments are currently only "
                    + "supported by built-in functions, but 'my_function' is a user-defined "
                    + "function.";

    @ParameterizedTest
    @ValueSource(
            classes = {LambdaDeclaringScalarFunction.class, LambdaDeclaringAggregateFunction.class})
    void testLambdaArgumentIsRejected(Class<? extends UserDefinedFunction> functionClass) {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_function", functionClass);
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2, 3})));

        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT my_function(f0, x -> x + 1) FROM t"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(EXPECTED_MESSAGE);

        assertThatThrownBy(
                        () ->
                                tEnv.from("t")
                                        .select(
                                                call(
                                                        "my_function",
                                                        $("f0"),
                                                        new UnresolvedLambdaExpression(
                                                                Collections.singletonList("x"),
                                                                $("x").plus(1)))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(EXPECTED_MESSAGE);
    }

    /** Declares {@code f(ARRAY<INT>, (INT) -> INT)}. */
    private static class ArrayLambdaInputTypeStrategy implements LambdaInputTypeStrategy {

        @Override
        public Optional<List<DataType>> getExpectedLambdaParameterTypes(
                CallContext callContext, int argumentPos) {
            if (argumentPos != 1) {
                return Optional.empty();
            }
            return Optional.of(Collections.singletonList(DataTypes.INT()));
        }

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(2);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            return Optional.of(callContext.getArgumentDataTypes());
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return Collections.singletonList(
                    Signature.of(
                            Signature.Argument.of("ARRAY"), Signature.Argument.of("(x) -> y")));
        }
    }

    /** A scalar function that declares a lambda argument. */
    public static class LambdaDeclaringScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(new ArrayLambdaInputTypeStrategy())
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.INT())))
                    .build();
        }

        public Integer[] eval(Integer[] array, Function<Integer, Integer> lambda) {
            return array;
        }
    }

    /** An aggregate function that declares a lambda argument. */
    public static class LambdaDeclaringAggregateFunction
            extends AggregateFunction<Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        public void accumulate(
                Integer accumulator, Integer[] array, Function<Integer, Integer> lambda) {}

        @Override
        public Integer getValue(Integer accumulator) {
            return accumulator;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(new ArrayLambdaInputTypeStrategy())
                    .accumulatorTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
                    .build();
        }
    }
}
