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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkSqlCallBinding;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;

/**
 * A {@link SqlOperandTypeInference} backed by {@link TypeInference}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
@Internal
public final class TypeInferenceOperandInference implements SqlOperandTypeInference {

    private final DataTypeFactory dataTypeFactory;

    private final FunctionDefinition definition;

    private final TypeInference typeInference;

    public TypeInferenceOperandInference(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            TypeInference typeInference) {
        this.dataTypeFactory = dataTypeFactory;
        this.definition = definition;
        this.typeInference = typeInference;
    }

    @Override
    public void inferOperandTypes(
            SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        final SqlCallBinding binding = bindPermutedCall(callBinding);
        final CallContext callContext =
                new CallBindingCallContext(
                        dataTypeFactory,
                        definition,
                        binding,
                        returnType,
                        typeInference.getStaticArguments().orElse(null));
        try {
            // Operand type inference runs top-down during Calcite's inferUnknownTypes pass, which
            // may reach this call while an enclosing lambda parameter is still unbound (a
            // parameter reference operand is transiently ANY). In that case defer: the operand
            // types are inferred in a later pass once the enclosing parameter is bound (mirrors the
            // operand checker and the built-in array higher-order function operators).
            if (TypeInferenceOperandChecker.hasUnresolvedLambdaParameterOperand(binding)) {
                return;
            }
            // Bind lambda parameter types (if any) so that the lambda operands carry a concrete
            // FUNCTION type before the input type strategy inspects them.
            TypeInferenceOperandChecker.bindLambdaArguments(binding, callContext, typeInference);
            if (TypeInferenceUtil.validateArgumentCount(
                    typeInference.getInputTypeStrategy().getArgumentCount(),
                    callContext.getArgumentDataTypes().size(),
                    false)) {
                inferOperandTypesOrError(unwrapTypeFactory(callBinding), callContext, operandTypes);
            }
        } catch (ValidationException e) {
            // let operand checker fail
        } catch (CalciteContextException e) {
            throw e;
        } catch (Throwable t) {
            throw createUnexpectedException(callContext, t);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Calcite infers operand types on a binding of the call as written. With named arguments, the
     * binding's {@link SqlCallBinding#operands()} are permuted into the declared order but its
     * {@link SqlCallBinding#getOperandType(int)} is not, so the same position would denote two
     * different arguments — and a lambda's strategy would derive its parameter types from the wrong
     * sibling. Binds the permuted call instead, which is what the operand checker sees.
     */
    private static SqlCallBinding bindPermutedCall(SqlCallBinding callBinding) {
        final boolean hasNamedArguments =
                callBinding.getCall().getOperandList().stream()
                        .anyMatch(
                                operand ->
                                        operand != null
                                                && operand.getKind()
                                                        == SqlKind.ARGUMENT_ASSIGNMENT);
        if (!hasNamedArguments) {
            return callBinding;
        }
        return new FlinkSqlCallBinding(
                callBinding.getValidator(), callBinding.getScope(), callBinding.permutedCall());
    }

    private void inferOperandTypesOrError(
            FlinkTypeFactory typeFactory, CallContext callContext, RelDataType[] operandTypes) {
        final List<DataType> expectedDataTypes;

        // Static arguments have the highest priority
        final List<StaticArgument> staticArgs = typeInference.getStaticArguments().orElse(null);
        if (staticArgs != null) {
            expectedDataTypes =
                    staticArgs.stream()
                            .map(staticArg -> staticArg.getDataType().orElse(null))
                            .collect(Collectors.toList());
        } else {
            expectedDataTypes =
                    typeInference
                            .getInputTypeStrategy()
                            .inferInputTypes(callContext, false)
                            .orElse(null);
        }

        // Early out for invalid input
        if (expectedDataTypes == null || expectedDataTypes.size() != operandTypes.length) {
            return;
        }

        for (int i = 0; i < operandTypes.length; i++) {
            final DataType expectedDataType = expectedDataTypes.get(i);
            if (expectedDataType != null) {
                final LogicalType inferredType = expectedDataType.getLogicalType();
                operandTypes[i] = typeFactory.createFieldTypeFromLogicalType(inferredType);
            }
        }
    }
}
