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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;

/**
 * Implemented by the generated class that hosts a compiled lambda body, so that the caller can
 * obtain the function object for a lambda argument without going through a {@link
 * java.lang.invoke.MethodHandle}.
 *
 * <p>The generated class compiles the body once, into an {@code eval} method typed with the lambda
 * parameters' external classes. {@link #bindLambda(Object[])} returns a small object that calls
 * that method directly, so applying the lambda costs one interface call per element rather than the
 * {@code Object[]} allocation and {@code asType} adaptation that {@link
 * java.lang.invoke.MethodHandle#invokeWithArguments} performs on every call.
 */
@Internal
public interface LambdaFunctionFactory {

    /**
     * Returns the function object handed to a user-defined higher-order function: a {@link
     * java.util.function.Function}, {@link java.util.function.BiFunction}, or {@link
     * org.apache.flink.util.function.TriFunction} for a lambda with one, two, or three user-visible
     * parameters respectively.
     *
     * <p>The lifted captures are bound here rather than per application, so they are converted once
     * per function object while the returned object is applied once per element. The array holds
     * one value per capture, in the order the captures were appended to the compiled parameter
     * list, and is not retained by the returned object.
     */
    Object bindLambda(Object[] captures);
}
