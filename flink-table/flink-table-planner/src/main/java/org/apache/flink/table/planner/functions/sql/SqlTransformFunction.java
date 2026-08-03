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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The higher-order function {@code TRANSFORM(collection, lambda)} that applies a lambda (a value of
 * the {@code FUNCTION} type) to every element of an array or every entry of a map.
 *
 * <ul>
 *   <li>{@code TRANSFORM(array, element -> expr)} returns an {@code ARRAY} where every element is
 *       the result of applying the lambda to the corresponding input element.
 *   <li>{@code TRANSFORM(map, (key, value) -> expr)} returns a {@code MAP} with the same keys where
 *       every value is the result of applying the lambda to the corresponding key/value pair.
 * </ul>
 *
 * <p>The lambda parameter types are inferred from the collection: the array element type, or the
 * map key and value types. The return type is derived from the lambda body.
 */
public class SqlTransformFunction extends SqlFunction {

    public SqlTransformFunction() {
        super(
                "TRANSFORM",
                SqlKind.OTHER_FUNCTION,
                SqlTransformFunction::deriveReturnType,
                null,
                OPERAND_TYPE_CHECKER,
                SqlFunctionCategory.SYSTEM);
    }

    /**
     * Binds the lambda parameter types to the collection element/entry types before the enclosing
     * call (and therefore the lambda body) is validated. Without this, Calcite validates the lambda
     * body with the parameter defaulting to {@code ANY}, which fails for bodies that require a
     * concrete type (e.g. arithmetic).
     */
    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        if (call.operandCount() == 2 && call.operand(1) instanceof SqlLambda) {
            final RelDataType collectionType = validator.deriveType(scope, call.operand(0));
            final List<RelDataType> parameterTypes = deriveParameterTypes(collectionType);
            final SqlLambda lambda = call.operand(1);
            if (parameterTypes != null && lambda.getParameters().size() == parameterTypes.size()) {
                final SqlLambdaScope lambdaScope =
                        (SqlLambdaScope) validator.getLambdaScope(lambda);
                for (int i = 0; i < parameterTypes.size(); i++) {
                    final RelDataType parameterType = parameterTypes.get(i);
                    if (parameterType != null) {
                        lambdaScope
                                .getParameterTypes()
                                .put(lambda.getParameters().get(i).toString(), parameterType);
                    }
                }
            }
        }
        super.validateCall(call, validator, scope, operandScope);
    }

    /**
     * Returns the lambda parameter types for the given collection type: the element type for an
     * array, or the key and value types for a map. Returns {@code null} for unsupported types.
     */
    private static List<RelDataType> deriveParameterTypes(RelDataType collectionType) {
        if (collectionType.getSqlTypeName() == SqlTypeName.ARRAY) {
            return Collections.singletonList(collectionType.getComponentType());
        } else if (collectionType.getSqlTypeName() == SqlTypeName.MAP) {
            return Arrays.asList(collectionType.getKeyType(), collectionType.getValueType());
        }
        return null;
    }

    private static RelDataType deriveReturnType(SqlOperatorBinding opBinding) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType collectionType = opBinding.getOperandType(0);
        final RelDataType lambdaType = opBinding.getOperandType(1);
        final RelDataType lambdaReturnType = ((FunctionSqlType) lambdaType).getReturnType();

        final RelDataType result;
        if (collectionType.getSqlTypeName() == SqlTypeName.MAP) {
            result = typeFactory.createMapType(collectionType.getKeyType(), lambdaReturnType);
        } else {
            result = typeFactory.createArrayType(lambdaReturnType, -1);
        }
        return typeFactory.createTypeWithNullability(result, collectionType.isNullable());
    }

    private static final SqlOperandTypeChecker OPERAND_TYPE_CHECKER =
            new SqlOperandTypeChecker() {

                @Override
                public boolean checkOperandTypes(
                        SqlCallBinding callBinding, boolean throwOnFailure) {
                    final RelDataType collectionType =
                            SqlTypeUtil.deriveType(callBinding, callBinding.operand(0));

                    final List<RelDataType> parameterTypes = deriveParameterTypes(collectionType);
                    if (parameterTypes == null) {
                        return fail(callBinding, throwOnFailure);
                    }

                    final SqlNode lambdaNode = callBinding.operand(1);
                    if (!(lambdaNode instanceof SqlLambda)
                            || ((SqlLambda) lambdaNode).getParameters().size()
                                    != parameterTypes.size()) {
                        return fail(callBinding, throwOnFailure);
                    }

                    // Bind the lambda parameter types to the collection element/entry types and
                    // re-validate the lambda body accordingly (mirrors Calcite's lambda checkers).
                    final SqlLambda lambda = (SqlLambda) lambdaNode;
                    final SqlValidator validator = callBinding.getValidator();
                    final SqlLambdaScope scope = (SqlLambdaScope) validator.getLambdaScope(lambda);
                    for (int i = 0; i < parameterTypes.size(); i++) {
                        final RelDataType parameterType = parameterTypes.get(i);
                        if (parameterType != null) {
                            scope.getParameterTypes()
                                    .put(lambda.getParameters().get(i).toString(), parameterType);
                        }
                    }
                    lambda.accept(new TypeRemover(validator));
                    validator.validateLambda(lambda);
                    return true;
                }

                private boolean fail(SqlCallBinding callBinding, boolean throwOnFailure) {
                    if (throwOnFailure) {
                        throw callBinding.newValidationSignatureError();
                    }
                    return false;
                }

                @Override
                public SqlOperandCountRange getOperandCountRange() {
                    return SqlOperandCountRanges.of(2);
                }

                @Override
                public String getAllowedSignatures(SqlOperator op, String opName) {
                    return opName + "(<ARRAY|MAP>, <FUNCTION>)";
                }
            };

    /**
     * Visitor that clears the previously validated types of a lambda body so that it can be
     * re-validated after its parameter types have been bound. Mirrors the equivalent helper used by
     * Calcite's lambda operand type checkers.
     */
    private static class TypeRemover extends SqlBasicVisitor<Void> {
        private final SqlValidator validator;

        private TypeRemover(SqlValidator validator) {
            this.validator = validator;
        }

        @Override
        public Void visit(SqlIdentifier id) {
            validator.removeValidatedNodeType(id);
            return super.visit(id);
        }

        @Override
        public Void visit(SqlCall call) {
            validator.removeValidatedNodeType(call);
            return super.visit(call);
        }
    }
}
