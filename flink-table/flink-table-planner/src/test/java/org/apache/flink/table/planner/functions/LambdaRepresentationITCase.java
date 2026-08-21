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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.FunctionData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.functions.utils.TestLambdaStrategies;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the representation in which a function exchanges values with its lambda argument is
 * the one the function declared, independently of its other arguments.
 */
class LambdaRepresentationITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    private static TableEnvironment tableEnvironment(Class<? extends ScalarFunction> function) {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("f", function);
        final Table t =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.STRING()))),
                        Row.of((Object) new String[] {"a", "b"}));
        tEnv.createTemporaryView("t", t);
        return tEnv;
    }

    private static void assertResult(TableEnvironment tEnv, String sql, String... expected)
            throws Exception {
        try (final CloseableIterator<Row> result = tEnv.sqlQuery(sql).execute().collect()) {
            assertThat(result.next().getField(0)).isEqualTo(expected);
        }
    }

    @Test
    void testInternalLambda() throws Exception {
        assertResult(
                tableEnvironment(InternalLambda.class),
                "SELECT f(f0, x -> UPPER(x)) FROM t",
                "A",
                "B");
    }

    @Test
    void testExternalLambda() throws Exception {
        assertResult(
                tableEnvironment(ExternalLambda.class),
                "SELECT f(f0, x -> UPPER(x)) FROM t",
                "A",
                "B");
    }

    /**
     * A trailing argument in the other representation must not change how the lambda is passed: it
     * has nothing to do with the values the lambda receives.
     */
    @Test
    void testInternalLambdaWithExternalArgument() throws Exception {
        assertResult(
                tableEnvironment(InternalLambdaWithExternalArgument.class),
                "SELECT f(f0, x -> UPPER(x), '!') FROM t",
                "A!",
                "B!");
    }

    @Test
    void testDeclaredRepresentationMustMatchEvalMethod() {
        final TableEnvironment tEnv = tableEnvironment(MismatchedLambda.class);
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT f(f0, x -> UPPER(x)) FROM t").execute())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Could not find an implementation method 'eval' in class "
                                + "'org.apache.flink.table.planner.functions."
                                + "LambdaRepresentationITCase$MismatchedLambda' for function 'f' "
                                + "that matches the following signature:\n"
                                + "java.lang.String[] eval(org.apache.flink.table.data.ArrayData, "
                                + "java.util.function.Function)");
    }

    /** A function that reads its array as {@link ArrayData} and its lambda accordingly. */
    public static class InternalLambda extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return internalArrayInference(
                    TestLambdaStrategies.lambda(
                            FunctionData.class, TestLambdaStrategies.elementOf(0)));
        }

        public @Nullable String[] eval(@Nullable ArrayData array, FunctionData lambda) {
            return applyToEach(array, lambda::apply, "");
        }
    }

    /** The same function on external classes. */
    public static class ExternalLambda extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .inputTypeStrategy(
                            TestLambdaStrategies.sequence(
                                    TestLambdaStrategies.explicit(
                                            DataTypes.ARRAY(DataTypes.STRING())),
                                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0))))
                    .outputTypeStrategy(call -> Optional.of(DataTypes.ARRAY(DataTypes.STRING())))
                    .build();
        }

        public @Nullable String[] eval(@Nullable String[] array, Function<Object, Object> lambda) {
            if (array == null) {
                return null;
            }
            final String[] result = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                final Object applied = lambda.apply(array[i]);
                result[i] = applied == null ? null : applied.toString();
            }
            return result;
        }
    }

    /** {@link InternalLambda} plus an external argument the lambda never sees. */
    public static class InternalLambdaWithExternalArgument extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return internalArrayInference(
                    TestLambdaStrategies.lambda(
                            FunctionData.class, TestLambdaStrategies.elementOf(0)),
                    TestLambdaStrategies.explicit(DataTypes.STRING()));
        }

        public @Nullable String[] eval(
                @Nullable ArrayData array, FunctionData lambda, String suffix) {
            return applyToEach(array, lambda::apply, suffix);
        }
    }

    /** Declares an external lambda but implements {@code eval} for an internal one. */
    public static class MismatchedLambda extends ScalarFunction {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return internalArrayInference(
                    TestLambdaStrategies.lambda(TestLambdaStrategies.elementOf(0)));
        }

        public @Nullable String[] eval(@Nullable ArrayData array, FunctionData lambda) {
            return applyToEach(array, lambda::apply, "");
        }
    }

    private static TypeInference internalArrayInference(
            TestLambdaStrategies.ArgumentSpec... trailing) {
        final DataType internalArray =
                DataTypes.ARRAY(DataTypes.STRING().bridgedTo(StringData.class))
                        .bridgedTo(ArrayData.class);
        final TestLambdaStrategies.ArgumentSpec[] arguments =
                new TestLambdaStrategies.ArgumentSpec[trailing.length + 1];
        arguments[0] = TestLambdaStrategies.explicit(internalArray);
        System.arraycopy(trailing, 0, arguments, 1, trailing.length);
        return TypeInference.newBuilder()
                .inputTypeStrategy(TestLambdaStrategies.sequence(arguments))
                .outputTypeStrategy(call -> Optional.of(DataTypes.ARRAY(DataTypes.STRING())))
                .build();
    }

    private static @Nullable String[] applyToEach(
            @Nullable ArrayData array, Function<Object, Object> lambda, String suffix) {
        if (array == null) {
            return null;
        }
        final String[] result = new String[array.size()];
        for (int i = 0; i < array.size(); i++) {
            final Object element = array.isNullAt(i) ? null : array.getString(i);
            final Object applied = lambda.apply(element);
            result[i] = applied == null ? null : applied.toString() + suffix;
        }
        return result;
    }
}
