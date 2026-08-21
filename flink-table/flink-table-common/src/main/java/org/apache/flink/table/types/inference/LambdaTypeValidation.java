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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.FunctionType;

import java.util.List;

/**
 * Validation of the arity of a lambda argument, shared by the SQL and the Table API binding paths.
 *
 * <p>The arity that a caller requests when constructing a {@code FUNCTION} type is validated by
 * {@link FunctionType} itself.
 *
 * <p>Only parameter counts between zero and {@link FunctionType#MAX_CONVERTIBLE_PARAMETER_COUNT}
 * have a runtime representation. Nothing validates the arity a {@link LambdaInputTypeStrategy}
 * derives on its behalf, so both surfaces must apply this check right after {@link
 * LambdaInputTypeStrategy#getExpectedLambdaParameterTypes} returned.
 */
@Internal
public final class LambdaTypeValidation {

    private static final String RUNTIME_REPRESENTATION_HINT =
            "A lambda is passed to a user-defined function as an "
                    + "org.apache.flink.util.function.FunctionN, where N is its number of "
                    + "parameters and ranges from Function0 to Function"
                    + FunctionType.MAX_CONVERTIBLE_PARAMETER_COUNT
                    + ", so no other number of parameters can be represented at runtime.";

    /** Whether a lambda with the given number of parameters has a runtime representation. */
    public static boolean isSupportedParameterCount(int parameterCount) {
        return parameterCount >= 0
                && parameterCount <= FunctionType.MAX_CONVERTIBLE_PARAMETER_COUNT;
    }

    /**
     * Validates the parameter types that a {@link LambdaInputTypeStrategy} derived for a call.
     *
     * @throws ValidationException if the strategy derived more than {@link
     *     FunctionType#MAX_CONVERTIBLE_PARAMETER_COUNT} parameter types
     */
    public static void checkDerivedParameterTypes(List<DataType> parameterTypes) {
        if (!isSupportedParameterCount(parameterTypes.size())) {
            throw new ValidationException(derivedParameterCountError(parameterTypes.size()));
        }
    }

    /**
     * The message of {@link #checkDerivedParameterTypes(List)} for call sites that need to wrap it
     * into their own exception type.
     */
    public static String derivedParameterCountError(int parameterCount) {
        return String.format(
                "A lambda argument must have between 0 and %d parameters, but %d parameter "
                        + "types were derived. %s",
                FunctionType.MAX_CONVERTIBLE_PARAMETER_COUNT,
                parameterCount,
                RUNTIME_REPRESENTATION_HINT);
    }

    private LambdaTypeValidation() {
        // no instantiation
    }
}
