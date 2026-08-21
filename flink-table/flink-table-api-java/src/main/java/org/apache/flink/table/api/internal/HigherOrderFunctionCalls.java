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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.util.function.TriFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.table.expressions.ApiExpressionUtils.objectToExpression;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;

/** Utilities for building calls to higher-order functions in {@link BaseExpressions}. */
@Internal
final class HigherOrderFunctionCalls {

    private static final AtomicLong LAMBDA_PARAMETER_COUNTER = new AtomicLong();

    /** Builds a call to a higher-order function taking a lambda with one parameter. */
    static <InType, OutType> OutType lambdaCall(
            BaseExpressions<InType, OutType> input,
            BuiltInFunctionDefinition definition,
            Function<OutType, OutType> lambdaFunction,
            Expression... leadingOperands) {
        return lambdaCall(
                input, definition, 1, refs -> lambdaFunction.apply(refs.get(0)), leadingOperands);
    }

    /** Builds a call to a higher-order function taking a lambda with two parameters. */
    static <InType, OutType> OutType lambdaCall(
            BaseExpressions<InType, OutType> input,
            BuiltInFunctionDefinition definition,
            BiFunction<OutType, OutType, OutType> lambdaFunction,
            Expression... leadingOperands) {
        return lambdaCall(
                input,
                definition,
                2,
                refs -> lambdaFunction.apply(refs.get(0), refs.get(1)),
                leadingOperands);
    }

    /** Builds a call to a higher-order function taking a lambda with three parameters. */
    static <InType, OutType> OutType lambdaCall(
            BaseExpressions<InType, OutType> input,
            BuiltInFunctionDefinition definition,
            TriFunction<OutType, OutType, OutType, OutType> lambdaFunction,
            Expression... leadingOperands) {
        return lambdaCall(
                input,
                definition,
                3,
                refs -> lambdaFunction.apply(refs.get(0), refs.get(1), refs.get(2)),
                leadingOperands);
    }

    /**
     * Builds a call of the shape {@code definition(input, leadingOperands..., lambda)} where the
     * lambda declares {@code arity} parameters with freshly generated names.
     *
     * <p>The {@code bodyBuilder} receives references to those parameters, in declaration order, and
     * returns the lambda body. References are passed in the API-specific expression representation
     * such that the body can be built with the fluent API of the caller.
     */
    private static <InType, OutType> OutType lambdaCall(
            BaseExpressions<InType, OutType> input,
            BuiltInFunctionDefinition definition,
            int arity,
            Function<List<OutType>, OutType> bodyBuilder,
            Expression... leadingOperands) {
        final List<String> parameters = new ArrayList<>(arity);
        final List<OutType> parameterRefs = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            final String name = "$lambdaParam$" + LAMBDA_PARAMETER_COUNTER.getAndIncrement();
            parameters.add(name);
            parameterRefs.add(input.toApiSpecificExpression(unresolvedRef(name)));
        }
        final Expression body = objectToExpression(bodyBuilder.apply(parameterRefs));
        final Expression[] operands = new Expression[leadingOperands.length + 2];
        operands[0] = input.toExpr();
        System.arraycopy(leadingOperands, 0, operands, 1, leadingOperands.length);
        operands[operands.length - 1] = new UnresolvedLambdaExpression(parameters, body);
        return input.toApiSpecificExpression(unresolvedCall(definition, operands));
    }

    private HigherOrderFunctionCalls() {
        // no instantiation
    }
}
