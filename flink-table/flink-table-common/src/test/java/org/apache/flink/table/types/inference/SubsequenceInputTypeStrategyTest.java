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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.SubsequenceInputTypeStrategy;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.commonType;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SubsequenceInputTypeStrategy}. */
class SubsequenceInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "A strategy used for IF ELSE with valid arguments",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .finish())
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(), DataTypes.SMALLINT(), DataTypes.DECIMAL(10, 2))
                        .expectSignature("f(<BOOLEAN>, <COMMON>, <COMMON>)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2)),
                TestSpec.forStrategy(
                                "Strategy fails if any of the nested strategies fail",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .finish())
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(), DataTypes.VARCHAR(3), DataTypes.DECIMAL(10, 2))
                        .expectErrorMessage(
                                "Could not find a common type for arguments: [VARCHAR(3), DECIMAL(10, 2)]"),
                TestSpec.forStrategy(
                                "Strategy with a varying argument",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .finishWithVarying(
                                                varyingSequence(logical(LogicalTypeRoot.BIGINT))))
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.SMALLINT(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.TINYINT())
                        .expectSignature("f(<BOOLEAN>, <COMMON>, <COMMON>, <BIGINT>...)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT()),
                TestSpec.forStrategy(
                                "A complex strategy with few sub sequences",
                                InputTypeStrategies.compositeSequence()
                                        .argument(logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(commonType(2))
                                        .argument(explicit(DataTypes.TIME().notNull()))
                                        .subsequence(commonType(2))
                                        .finishWithVarying(
                                                varyingSequence(
                                                        logical(LogicalTypeFamily.TIMESTAMP), ANY)))
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.SMALLINT(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.TIME().notNull(),
                                DataTypes.TINYINT().notNull(),
                                DataTypes.DECIMAL(13, 3).notNull(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT())
                        .expectSignature(
                                "f(<BOOLEAN>, <COMMON>, <COMMON>, TIME(0) NOT NULL, <COMMON>, <COMMON>, <TIMESTAMP>, <ANY>...)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.TIME().notNull(),
                                DataTypes.DECIMAL(13, 3).notNull(),
                                DataTypes.DECIMAL(13, 3).notNull(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT()),
                TestSpec.forStrategy(
                                "A strategy with named argument",
                                InputTypeStrategies.compositeSequence()
                                        .argument("arg1", logical(LogicalTypeRoot.BOOLEAN))
                                        .subsequence(
                                                sequence(
                                                        Arrays.asList("arg2", "arg3"),
                                                        Arrays.asList(
                                                                logical(
                                                                        LogicalTypeFamily
                                                                                .INTEGER_NUMERIC),
                                                                logical(
                                                                        LogicalTypeFamily
                                                                                .INTEGER_NUMERIC))))
                                        .argument(logical(LogicalTypeRoot.INTEGER))
                                        .finish())
                        .calledWithArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.INT())
                        .expectSignature(
                                "f(arg1 <BOOLEAN>, arg2 <INTEGER_NUMERIC>, arg3 <INTEGER_NUMERIC>, <INTEGER>)")
                        .expectArgumentTypes(
                                DataTypes.BOOLEAN(),
                                DataTypes.SMALLINT(),
                                DataTypes.BIGINT(),
                                DataTypes.INT()));
    }

    /**
     * A split shifts every per-argument accessor of the {@link CallContext} it hands to a nested
     * strategy, including {@link CallContext#getLambdaArgument(int)}. No built-in combines {@code
     * compositeSequence()} with a lambda argument yet, so this pins the shift before one does -- an
     * unshifted position would read the lambda of a different argument.
     */
    @Test
    void testLambdaArgumentPositionIsShifted() {
        final LambdaInfo lambdaInfo =
                new LambdaInfo(
                        null,
                        Collections.singletonList(DataTypes.FIELD("x", DataTypes.INT())),
                        DataTypes.BIGINT());

        final CallContextMock callContext = new CallContextMock();
        callContext.name = "f";
        callContext.argumentDataTypes =
                Arrays.asList(
                        DataTypes.BOOLEAN(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.FUNCTION(1));
        callContext.outputDataType = Optional.empty();
        callContext.lambdaArguments = Collections.singletonMap(2, lambdaInfo);

        final CapturingInputTypeStrategy capturing = new CapturingInputTypeStrategy();
        InputTypeStrategies.compositeSequence()
                .argument(logical(LogicalTypeRoot.BOOLEAN))
                .subsequence(capturing)
                .finish()
                .inferInputTypes(callContext, false);

        // the split starts at 1, so the lambda at absolute position 2 is seen at position 1
        assertThat(capturing.lambdaArguments)
                .containsExactly(Optional.empty(), Optional.of(lambdaInfo));
    }

    /**
     * An {@link InputTypeStrategy} for two arguments that records the {@link
     * CallContext#getLambdaArgument(int)} of every argument it is given.
     */
    private static class CapturingInputTypeStrategy implements InputTypeStrategy {

        private static final int ARGUMENT_COUNT = 2;

        private final List<Optional<LambdaInfo>> lambdaArguments = new ArrayList<>();

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.of(ARGUMENT_COUNT);
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            for (int pos = 0; pos < ARGUMENT_COUNT; pos++) {
                lambdaArguments.add(callContext.getLambdaArgument(pos));
            }
            return Optional.of(callContext.getArgumentDataTypes());
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return Collections.singletonList(
                    Signature.of(Signature.Argument.of("ANY"), Signature.Argument.of("ANY")));
        }
    }
}
