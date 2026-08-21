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
 * An {@link InputTypeStrategy} that declares one or more lambda arguments.
 *
 * <p>The expression resolver and the planner use this to bind the types of a lambda's parameters
 * from the sibling arguments before resolving the lambda body. Implementing this interface is what
 * declares that the signature contains a lambda: a strategy without a lambda argument must not
 * implement it, because the interface alone rejects a lambda argument for a function kind that
 * cannot host one. The same method is consulted on both surfaces, so a function that implements
 * this interface behaves identically in SQL and in the Table API.
 *
 * <p>This is the only way to declare a lambda argument. A function that wants one implements this
 * interface as its {@link
 * org.apache.flink.table.types.inference.TypeInference#getInputTypeStrategy() input type strategy}:
 * {@link #inferInputTypes} accepts the lambda argument like any other argument, and {@link
 * #getExpectedLambdaParameterTypes} states what its parameters are bound to. The built-in
 * higher-order functions declare their signatures the same way.
 *
 * <p><b>Contract of the function object received at runtime</b>
 *
 * <p>A lambda argument reaches the evaluation method as a {@link java.util.function.Function},
 * {@link java.util.function.BiFunction}, or {@link org.apache.flink.util.function.TriFunction} —
 * or, if the type inference declares the argument's conversion class accordingly, as the
 * corresponding {@link org.apache.flink.table.data.FunctionData}, {@link
 * org.apache.flink.table.data.BiFunctionData}, or {@link
 * org.apache.flink.table.data.TriFunctionData}. That object is not an ordinary value but a handle
 * on a framework-compiled expression. The following rules are binding for the function:
 *
 * <ul>
 *   <li><b>Lifetime</b>: the object is valid only for the duration of the evaluation method call it
 *       is passed to. It must not be stored in a field, put into state, returned, handed to
 *       anything that outlives the call, or serialized. Behavior after the evaluation method has
 *       returned is undefined and is not checked for.
 *   <li><b>Identity</b>: a fresh object is created for every evaluation of the enclosing call (per
 *       row, or per element if the call itself is located in a lambda body). Its identity must not
 *       be used as a cache key.
 *   <li><b>Captures</b>: all captured values are evaluated before the evaluation method is entered
 *       and bound behind the object, so every application within one call observes the same
 *       snapshot.
 *   <li><b>Invocation</b>: the function owns its loop and may apply the object zero, one, or
 *       arbitrarily many times, in any order. The framework never applies the lambda on the
 *       function's behalf, thus the body is only evaluated where the function applies it.
 *   <li><b>Arguments and result</b>: {@code apply} may be called with {@code null} arguments and
 *       may return {@code null} (i.e. SQL {@code NULL}). A lambda exposed through {@link
 *       java.util.function.Function}, {@link java.util.function.BiFunction}, or {@link
 *       org.apache.flink.util.function.TriFunction} exchanges external values. A lambda exposed
 *       through the corresponding internal-data interface may return a mutable value backed by
 *       evaluator-owned memory that the next application reuses. A function that retains or buffers
 *       such an internal result across another application must deep-copy it first.
 *   <li><b>Nesting</b>: applications of different lambda objects may nest to any depth, as a body
 *       may contain another higher-order call whose function receives its own object. An object is
 *       never re-entered from within its own body — SQL has no recursion — and a function must not
 *       construct such a call itself.
 *   <li><b>Thread affinity</b>: the object must be applied only from the thread that called the
 *       evaluation method, and only while that call is on the stack. It is not thread-safe:
 *       applications must not overlap, and the object must not be handed to another thread, an
 *       executor, a parallel stream, or a {@link java.util.concurrent.CompletableFuture} callback.
 *       This is not enforced and cannot be probed: the compiled body is a single instance shared by
 *       every object built at that call site, so a body that folds to plain arithmetic may appear
 *       to tolerate concurrent applications while one that constructs a {@code ROW}/{@code ARRAY}
 *       or calls another function corrupts the reusable buffers behind it. For the same reason a
 *       retained object stays technically callable after the evaluation method returned; applying
 *       it then is undefined behavior, not a supported mode. Consequently, {@link
 *       org.apache.flink.table.functions.AsyncScalarFunction} and {@link
 *       org.apache.flink.table.functions.AsyncTableFunction} cannot declare a lambda argument; only
 *       {@link org.apache.flink.table.functions.ScalarFunction} and {@link
 *       org.apache.flink.table.functions.TableFunction} can.
 *   <li><b>Exceptions</b>: an unchecked exception ({@link RuntimeException} or {@link Error})
 *       raised while the body is evaluated propagates out of {@code apply} unchanged, exactly as it
 *       does for a built-in higher-order function, which evaluates its body the same way. Only a
 *       throwable that {@code apply} cannot declare leaves it as an {@link
 *       org.apache.flink.util.FlinkRuntimeException} with the original error as its cause. Letting
 *       it propagate is recommended; it then fails the task like any other function failure.
 *       Catching it is allowed, but the application then yields no result, anything the body
 *       touched is left in an unspecified state, and the enclosing call is not retried.
 *   <li><b>Lifecycle</b>: no lambda object exists in {@link
 *       org.apache.flink.table.functions.UserDefinedFunction#open(org.apache.flink.table.functions.FunctionContext)}
 *       or {@link org.apache.flink.table.functions.UserDefinedFunction#close()}, and none must be
 *       obtained or applied there. The framework owns the lifecycle of the compiled body; a
 *       function called from a body is instantiated, opened, and closed like one called anywhere
 *       else.
 *   <li><b>Determinism</b>: the object is only as deterministic as the body of the caller's lambda
 *       expression, so applying the same arguments twice may yield different results.
 * </ul>
 */
@PublicEvolving
public interface LambdaInputTypeStrategy extends InputTypeStrategy {

    /**
     * Returns the expected parameter types of the lambda argument at the given position, or an
     * empty optional if the argument at that position is not a lambda (or the types cannot be
     * derived).
     *
     * <p>The types are derived from the (already resolved) sibling arguments available in the given
     * {@link CallContext}.
     *
     * <p>A present list must contain one, two, or three types, because a lambda is passed to a
     * user-defined function as a {@link java.util.function.Function}, {@link
     * java.util.function.BiFunction}, or {@link org.apache.flink.util.function.TriFunction}, and no
     * other number of parameters can be represented at runtime. Both SQL and the Table API reject a
     * list of any other size with a {@link org.apache.flink.table.api.ValidationException}.
     */
    Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos);
}
