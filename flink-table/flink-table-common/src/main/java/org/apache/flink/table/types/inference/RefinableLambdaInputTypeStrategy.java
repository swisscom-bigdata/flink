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
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * A {@link LambdaInputTypeStrategy} that takes part in the framework-internal feedback between a
 * higher-order function and the lambda it receives: it may refine the lambda parameter types once
 * the body has been resolved with the types returned by {@link
 * LambdaInputTypeStrategy#getExpectedLambdaParameterTypes}, and it may require the body to be
 * coerced to a type of its choosing.
 *
 * <p>This is not part of the user-facing extension point. Both hooks let a function shape a lambda
 * beyond what its own signature declares, which is only sound for the built-in functions whose
 * runtime behaviour is defined together with the strategy. Exposing them publicly would let a
 * user-defined strategy declare a lambda that a user cannot reason about from the signature alone,
 * so only built-in strategies implement this interface.
 *
 * <p>Both surfaces apply both hooks: the expression resolver on the Table API path and {@code
 * TypeInferenceOperandChecker} on the SQL path, so a call reaches the same lambda types no matter
 * where it was written.
 */
@Internal
public interface RefinableLambdaInputTypeStrategy extends LambdaInputTypeStrategy {

    /**
     * The type the lambda body is coerced to before it is compiled, or an empty optional to compile
     * the body as written.
     *
     * <p>{@code ARRAY_REDUCE} requires this: its reducer body only has to be <em>assignable</em> to
     * the accumulator, so a body of a narrower type (an {@code INT} body for a {@code BIGINT}
     * accumulator) would otherwise hand the next iteration a value its accumulator parameter cannot
     * hold. Coercing the body makes every iteration produce the accumulator's own type.
     *
     * <p>The coercion follows {@code CAST} semantics and is only applied when the required type
     * differs from the body type; the strategy is responsible for having rejected a body that
     * cannot be coerced in {@link #inferInputTypes}.
     *
     * @param callContext the call context (with the resolved sibling arguments)
     * @param argumentPos the position of the lambda argument
     */
    default Optional<DataType> getRequiredLambdaResultType(
            CallContext callContext, int argumentPos) {
        return Optional.empty();
    }

    /**
     * Adjusts the lambda parameter types after the lambda body has been resolved once. This
     * provides a single, monotonic feedback pass: the strategy may inspect the resolved body type
     * and return refined parameter types, after which the resolver re-resolves the body with them.
     *
     * <p>{@code ARRAY_REDUCE} implements this to widen the accumulator parameter to nullable once
     * the reducer body is found to be nullable, so a later iteration can observe an accumulator
     * that an earlier iteration set to {@code NULL}.
     *
     * @param callContext the call context (with the resolved sibling arguments)
     * @param argumentPos the position of the lambda argument
     * @param currentParameterTypes the parameter types the body was first resolved with
     * @param lambdaResultType the resolved type of the lambda body
     * @return refined parameter types of the same arity as {@code currentParameterTypes}, or an
     *     empty optional to keep the current ones
     */
    Optional<List<DataType>> adjustLambdaParameterTypes(
            CallContext callContext,
            int argumentPos,
            List<DataType> currentParameterTypes,
            DataType lambdaResultType);
}
