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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.inference.LambdaInfo;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TypeInferenceReturnInference}. */
class TypeInferenceReturnInferenceTest {

    /** Stands in for the bridged operator, which is not available without a planner context. */
    private static final SqlFunction HIGHER_ORDER_FUNCTION =
            new SqlFunction(
                    "HIGHER_ORDER_FUNCTION",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.ANY,
                    SqlFunctionCategory.SYSTEM);

    private final FlinkTypeFactory typeFactory =
            new FlinkTypeFactory(
                    TypeInferenceReturnInferenceTest.class.getClassLoader(),
                    FlinkTypeSystem.INSTANCE);
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    @Test
    void testReturnTypeDerivationDoesNotExposeLambdaBody() {
        // HIGHER_ORDER_FUNCTION(array#0, x -> x + 1). The lambda is closed, as it is after capture
        // lifting, when the planner re-derives the call's return type through this class.
        final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        final RelDataType arrayType = typeFactory.createArrayType(intType, -1);
        final RexLambdaRef element = new RexLambdaRef(0, "x", intType);
        final RexNode body =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        element,
                        rexBuilder.makeExactLiteral(BigDecimal.ONE, intType));
        final RexCall call =
                (RexCall)
                        rexBuilder.makeCall(
                                arrayType,
                                HIGHER_ORDER_FUNCTION,
                                Arrays.asList(
                                        rexBuilder.makeInputRef(arrayType, 0),
                                        rexBuilder.makeLambdaCall(
                                                body, Collections.singletonList(element))));

        final List<LambdaInfo> observed = new ArrayList<>();
        final TypeInference typeInference =
                TypeInference.newBuilder()
                        .outputTypeStrategy(
                                callContext -> {
                                    final LambdaInfo lambda =
                                            callContext
                                                    .getLambdaArgument(1)
                                                    .orElseThrow(IllegalStateException::new);
                                    observed.add(lambda);
                                    return Optional.of(DataTypes.ARRAY(lambda.getReturnDataType()));
                                })
                        .build();

        new TypeInferenceReturnInference(
                        new DataTypeFactoryMock(),
                        BuiltInFunctionDefinitions.ARRAY_TRANSFORM,
                        typeInference)
                .inferReturnType(RexCallBinding.create(typeFactory, call, Collections.emptyList()));

        // Type inference sees the parameters and the result type, but never the body (see
        // LambdaInfo#getBody), just as it does during SQL validation and in the Table API.
        assertThat(observed)
                .singleElement()
                .satisfies(
                        lambda -> {
                            assertThat(lambda.hasBody()).isFalse();
                            assertThat(lambda.getParameterFields())
                                    .singleElement()
                                    .satisfies(
                                            field -> {
                                                assertThat(field.getName()).isEqualTo("x");
                                                assertThat(field.getDataType().getLogicalType())
                                                        .isEqualTo(new IntType(false));
                                            });
                            assertThat(lambda.getReturnDataType().getLogicalType())
                                    .isEqualTo(new IntType(false));
                        });
    }
}
