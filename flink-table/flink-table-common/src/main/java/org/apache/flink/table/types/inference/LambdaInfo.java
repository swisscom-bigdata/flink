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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Describes a lambda argument of a higher-order function call, as returned by {@link
 * CallContext#getLambdaArgument(int)}.
 *
 * <p>It exposes the lambda body as an {@link Expression} together with the typed fields for the
 * lambda parameters and the result type. A higher-order function can evaluate the body per element
 * by passing these to {@link
 * org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluatorFactory#createEvaluator(Expression,
 * DataType, DataTypes.Field...)}.
 *
 * <p>This is a planning-time description only. At runtime, a function receives the lambda as a
 * function object whose contract is documented on {@link LambdaInputTypeStrategy}.
 */
@PublicEvolving
public final class LambdaInfo {

    private final @Nullable Expression body;

    private final List<Field> parameterFields;

    private final DataType returnDataType;

    public LambdaInfo(
            @Nullable Expression body, List<Field> parameterFields, DataType returnDataType) {
        this.body = body;
        this.parameterFields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        parameterFields, "Parameter fields must not be null.")));
        this.returnDataType =
                Preconditions.checkNotNull(returnDataType, "Return data type must not be null.");
    }

    /**
     * Returns the lambda body as an {@link Expression}, or an empty optional if the body is not
     * available.
     *
     * <p>The body is only available while a function is being specialized (i.e. from {@link
     * org.apache.flink.table.functions.SpecializedFunction#specialize}), not during type inference.
     * This holds for both the SQL and the Table API surface: type inference must derive from {@link
     * #getParameterFields()} and {@link #getReturnDataType()} alone, so that a function infers the
     * same types no matter which surface the call was written on.
     */
    public Optional<Expression> getBody() {
        return Optional.ofNullable(body);
    }

    /** Returns the typed fields describing the lambda parameters (name and type). */
    public List<Field> getParameterFields() {
        return parameterFields;
    }

    /** Returns the result type of the lambda body. */
    public DataType getReturnDataType() {
        return returnDataType;
    }
}
