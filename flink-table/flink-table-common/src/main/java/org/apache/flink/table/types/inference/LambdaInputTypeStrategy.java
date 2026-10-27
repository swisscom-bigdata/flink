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
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * An {@link InputTypeStrategy} for a function signature that takes one or more lambda arguments.
 *
 * <p>Implementing this interface is the only way to declare a lambda argument. A call that passes a
 * lambda to a function whose input type strategy does not implement it is rejected. {@link
 * #inferInputTypes} handles a lambda argument like any other argument, and {@link
 * #getExpectedLambdaParameterTypes} returns the types of the lambda's parameters, which are needed
 * before the lambda body can be resolved. SQL and the Table API call the same method, so both bind
 * the same types.
 *
 * <p>Lambda arguments are currently only supported by built-in functions. A call that passes a
 * lambda to a user-defined function is rejected during validation, in SQL and in the Table API.
 *
 * <p><b>The lambda object at runtime</b>
 *
 * <p>A lambda with {@code n} parameters reaches the evaluation method as an {@code
 * org.apache.flink.util.function.FunctionN}, or as an {@code
 * org.apache.flink.table.data.FunctionDataN} if the type inference declares that conversion class.
 * The object is not a value: applying it runs a lambda body compiled by Flink. The function that
 * receives it must follow these rules:
 *
 * <ul>
 *   <li><b>Lifetime</b>: the object is valid only during the evaluation call that receives it. Do
 *       not store it in a field or in state, return it, pass it to anything that outlives the call,
 *       or serialize it. Using it after the call has returned is undefined behavior and is not
 *       detected.
 *   <li><b>Identity</b>: every evaluation of the call creates a new object (per row, or per element
 *       if the call is inside another lambda body). Do not use its identity as a cache key.
 *   <li><b>Captures</b>: values that the body reads from outside the lambda are evaluated once,
 *       before the evaluation method is called. Every application within one call sees the same
 *       values.
 *   <li><b>Invocation</b>: the function decides how often and in which order it applies the object:
 *       zero, one or many times. Flink never applies it on the function's behalf, so the body runs
 *       only when the function applies it.
 *   <li><b>Nulls</b>: pass {@code null} only for a parameter that {@link
 *       LambdaInfo#getParameterFields()} declares nullable. The compiled body trusts the declared
 *       types, so {@code null} for a {@code NOT NULL} parameter is undefined behavior. A function
 *       that may pass {@code null}, for example to pad the shorter of two arrays, must declare that
 *       parameter nullable. {@code apply} returns {@code null} only if {@link
 *       LambdaInfo#getReturnDataType()} is nullable.
 *   <li><b>Reused results</b>: a {@code FunctionN} accepts and returns external values. A {@code
 *       FunctionDataN} may return an internal value whose memory is reused by the next application.
 *       A function that keeps such a result beyond the next application must deep-copy it first.
 *   <li><b>Nesting</b>: a lambda body may call another higher-order function, which receives its
 *       own object, so applications may nest to any depth. An object is never applied again from
 *       within its own body, because SQL has no recursion, and a function must not make this
 *       happen.
 *   <li><b>Threads</b>: apply the object only from the thread that called the evaluation method,
 *       and only while that call is running. The object is not thread-safe: applications must not
 *       overlap, and the object must not be passed to another thread, an executor, a parallel
 *       stream or a {@link java.util.concurrent.CompletableFuture} callback. This is not checked.
 *       All objects from one call site share one compiled body, so a simple arithmetic body may
 *       appear to work concurrently, while a body that builds a {@code ROW} or {@code ARRAY} or
 *       calls another function corrupts the buffers it reuses.
 *   <li><b>Exceptions</b>: a {@link RuntimeException} or {@link Error} thrown by the body
 *       propagates out of {@code apply} unchanged. A checked exception is wrapped in an {@link
 *       org.apache.flink.util.FlinkRuntimeException}. Letting it propagate is recommended, so that
 *       it fails the task like any other function error. If the function catches it, that
 *       application has no result, anything the body touched is in an unspecified state, and the
 *       call is not retried.
 *   <li><b>Lifecycle</b>: no lambda object exists during {@link
 *       org.apache.flink.table.functions.UserDefinedFunction#open(org.apache.flink.table.functions.FunctionContext)}
 *       or {@link org.apache.flink.table.functions.UserDefinedFunction#close()}, so none may be
 *       used there. Flink manages the compiled body. A function called inside a body is created,
 *       opened and closed as anywhere else.
 *   <li><b>Determinism</b>: the object is only as deterministic as the lambda body, so applying it
 *       twice to the same arguments may give different results. A non-deterministic body makes the
 *       whole call non-deterministic, and the planner treats it like any other non-deterministic
 *       call: for example, two identical calls are evaluated separately instead of sharing one
 *       result.
 * </ul>
 */
@PublicEvolving
public interface LambdaInputTypeStrategy extends InputTypeStrategy {

    /**
     * Returns the parameter types of the lambda argument at the given position, or an empty
     * optional if that argument is not a lambda or its parameter types cannot be derived.
     *
     * <p>Derive the types only from the arguments before the lambda, at positions {@code 0} to
     * {@code argumentPos - 1} of the signature (named arguments are already mapped to their
     * positions). Only those arguments are guaranteed to be resolved: the Table API resolves
     * arguments from left to right, so the lambda and the arguments after it are not resolved yet
     * and must not be read. If the parameter types depend on another argument, place the lambda
     * after that argument in the signature.
     *
     * <p>A present list contains between zero and {@link
     * org.apache.flink.table.types.logical.FunctionType#MAX_CONVERTIBLE_PARAMETER_COUNT} types,
     * because {@code FunctionN} and {@code FunctionDataN} exist only for up to four parameters. SQL
     * and the Table API reject a list of any other size with a {@link
     * org.apache.flink.table.api.ValidationException}.
     */
    Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos);
}
