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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.functions.utils.TestLambdaStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.Function0;
import org.apache.flink.util.function.Function4;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the arities at the edges of the range that a lambda argument of a
 * user-defined function can have: a lambda without parameters and a lambda with {@link
 * org.apache.flink.table.types.logical.FunctionType#MAX_CONVERTIBLE_PARAMETER_COUNT} parameters.
 * The arities in between are covered by {@link UserDefinedHigherOrderFunctionITCase}.
 */
class UserDefinedLambdaArityITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @Test
    void testUserDefinedFunctionWithMaximumArityLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_wide", Wide4ScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of((Object) new Integer[] {1, 2}));
        tEnv.createTemporaryView("t", input);

        // a lambda of the maximum supported arity, received as a first-class Function4 object
        final Table result =
                tEnv.sqlQuery("SELECT my_wide(a, (x1, x2, x3, x4) -> x1 + x2 + x3 + x4) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {4, 8});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithMaximumArityLambdaAndCapture() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_wide", Wide4ScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("c", DataTypes.INT())),
                        Row.of(new Integer[] {1, 2}, 100));
        tEnv.createTemporaryView("t", input);

        // the capture is lifted into an extra parameter, so the compiled lambda has 5
        // parameters: one more than any functional interface covers. That type is only ever
        // constructed internally, and the function object still exposes the user-visible arity.
        final Table result =
                tEnv.sqlQuery(
                        "SELECT my_wide(a, (x1, x2, x3, x4) -> x1 + x2 + x3 + x4 + c) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {4 + 100, 8 + 100});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithZeroArityLambda() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_generate", GenerateScalarFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())), Row.of(3));
        tEnv.createTemporaryView("t", input);

        // a lambda that receives nothing from the function, applied once per generated element
        final Table result = tEnv.sqlQuery("SELECT my_generate(n, () -> 7) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {7, 7, 7});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithZeroArityLambdaAndCapture() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_generate", GenerateScalarFunction.class);

        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("n", DataTypes.INT()),
                                DataTypes.FIELD("c", DataTypes.INT())),
                        Row.of(3, 100));
        tEnv.createTemporaryView("t", input);

        // the capture is lifted into a parameter, so the compiled lambda has one parameter while
        // the user-visible arity stays zero: the function object is still a Function0 whose
        // capture was bound once, before the first application
        final Table result = tEnv.sqlQuery("SELECT my_generate(n, () -> c + 1) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {101, 101, 101});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testUserDefinedFunctionWithZeroArityLambdaDeclaredAsSupplier() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("my_generate", SupplierScalarFunction.class);

        final Table input =
                tEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())), Row.of(3));
        tEnv.createTemporaryView("t", input);

        // Supplier is not the conversion class of FUNCTION(0), so argument enrichment keeps the
        // default Function0. Because Function0 extends Supplier, the generated object still
        // satisfies the declared parameter and get() delegates to apply().
        final Table result = tEnv.sqlQuery("SELECT my_generate(n, () -> 7) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[] {7, 7, 7});
            assertThat(iterator).isExhausted();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    /**
     * A user-defined higher-order function whose lambda takes no parameters: {@code my_generate(n,
     * () -> expr)} applies it once per generated element. Applying it repeatedly is what
     * distinguishes a zero-parameter lambda from a plain scalar argument.
     */
    public static class GenerateScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.explicit(DataTypes.INT()),
                                    TestLambdaStrategies.lambda()))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Integer[] eval(@Nullable Integer count, Function0<Integer> lambda) {
            if (count == null) {
                return null;
            }
            final Integer[] result = new Integer[count];
            for (int i = 0; i < count; i++) {
                result[i] = lambda.apply();
            }
            return result;
        }
    }

    /**
     * A function that declares its zero-parameter lambda as a {@link Supplier} instead of a {@link
     * Function0}, which works because {@code Function0} extends it. Used by {@link
     * #testUserDefinedFunctionWithZeroArityLambdaDeclaredAsSupplier()}.
     */
    public static class SupplierScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.explicit(DataTypes.INT()),
                                    TestLambdaStrategies.lambda()))
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.ARRAY(DataTypes.INT())))
                    .build();
        }

        public @Nullable Integer[] eval(@Nullable Integer count, Supplier<Integer> lambda) {
            if (count == null) {
                return null;
            }
            final Integer[] result = new Integer[count];
            for (int i = 0; i < count; i++) {
                result[i] = lambda.get();
            }
            return result;
        }
    }

    /**
     * A user-defined higher-order function whose lambda has the maximum supported arity, received
     * as a first-class {@link Function4} object. Every parameter is derived from the same array
     * element type, so the function only has to exercise the arity, not a wide signature.
     */
    public static class Wide4ScalarFunction extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            final TestLambdaStrategies.ParameterDerivation[] derivations =
                    new TestLambdaStrategies.ParameterDerivation[4];
            Arrays.fill(derivations, TestLambdaStrategies.elementOf(0));
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.logical(LogicalTypeRoot.ARRAY),
                                    TestLambdaStrategies.lambda(derivations)))
                    .outputTypeStrategy(
                            call ->
                                    Optional.of(
                                            DataTypes.ARRAY(
                                                    call.getLambdaArgument(1)
                                                            .orElseThrow(IllegalStateException::new)
                                                            .getReturnDataType())))
                    .build();
        }

        public @Nullable Integer[] eval(
                @Nullable Integer[] array,
                Function4<Integer, Integer, Integer, Integer, Integer> lambda) {
            if (array == null) {
                return null;
            }
            final Integer[] result = new Integer[array.length];
            for (int i = 0; i < array.length; i++) {
                final Integer e = array[i];
                result[i] = lambda.apply(e, e, e, e);
            }
            return result;
        }
    }
}
