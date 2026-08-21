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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.LambdaInfo;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Optional;

/**
 * Utilities shared by the input and result type strategies of the built-in higher-order functions.
 *
 * <p>A lambda argument carries a {@code FUNCTION} type that only states how many parameters the
 * lambda accepts. The parameter and result types are reached through {@link
 * CallContext#getLambdaArgument(int)} instead, which every {@link CallContext} implementation
 * backing a higher-order call provides.
 */
@Internal
final class LambdaStrategyUtils {

    /** Returns the result type of the lambda body at the given position, if available. */
    static Optional<LogicalType> lambdaResultType(CallContext callContext, int pos) {
        return callContext
                .getLambdaArgument(pos)
                .map(LambdaInfo::getReturnDataType)
                .map(DataType::getLogicalType);
    }

    /**
     * Returns the result type of the lambda body at the given position. The argument type has
     * already been checked to be a {@code FUNCTION}, so the lambda information must be present.
     */
    static LogicalType requireLambdaResultType(CallContext callContext, int pos) {
        return lambdaResultType(callContext, pos)
                .orElseThrow(
                        () ->
                                new TableException(
                                        String.format(
                                                "Missing lambda information for the argument at "
                                                        + "position %d. This is a bug, please file "
                                                        + "an issue.",
                                                pos)));
    }

    private LambdaStrategyUtils() {
        // no instantiation
    }
}
