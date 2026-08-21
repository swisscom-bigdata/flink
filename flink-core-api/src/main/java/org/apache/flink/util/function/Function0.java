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

package org.apache.flink.util.function;

import org.apache.flink.annotation.PublicEvolving;

import java.util.function.Supplier;

/**
 * Function which takes no arguments.
 *
 * <p>This is the arity-0 member of the {@code FunctionN} family. It extends {@link Supplier} so
 * that the received object can be handed to APIs expecting one, mirroring how {@link Function1},
 * {@link Function2} and {@link Function3} relate to their {@code java.util.function} counterparts.
 *
 * <p>{@link #apply()} is the single abstract method, so that a lambda argument of any arity is
 * generated the same way; {@link #get()} is a default that delegates to it. A user-defined function
 * may therefore declare its zero-parameter lambda argument as either this type or {@link Supplier}.
 *
 * @param <R> type of the return value
 */
@PublicEvolving
@FunctionalInterface
public interface Function0<R> extends Supplier<R> {
    /**
     * Applies this function.
     *
     * @return the function result
     */
    R apply();

    @Override
    default R get() {
        return apply();
    }
}
