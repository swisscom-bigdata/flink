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
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Preconditions;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/** Default runtime implementation for {@link ExpressionEvaluator}. */
@Internal
public class DefaultExpressionEvaluator implements ExpressionEvaluator {
    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<RichFunction> generatedClass;
    private final MethodType methodType;
    private final String expressionSummary;
    private transient RichFunction instance;

    public DefaultExpressionEvaluator(
            GeneratedFunction<RichFunction> generatedClass,
            Class<?> returnClass,
            Class<?>[] argClasses,
            String expressionSummary) {
        this.generatedClass = generatedClass;
        this.methodType = MethodType.methodType(returnClass, argClasses);
        this.expressionSummary = expressionSummary;
    }

    @Override
    public MethodHandle open(FunctionContext context) {
        final RichFunction openedInstance = openInstance(context);
        try {
            final MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
            return publicLookup
                    .findVirtual(openedInstance.getClass(), "eval", methodType)
                    .bindTo(openedInstance);
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Expression evaluator for '%s' could not be opened.",
                            expressionSummary),
                    e);
        }
    }

    /**
     * Creates and initializes the runtime implementation for a compiled lambda body, and returns it
     * as the factory for the function objects handed to a higher-order function.
     *
     * <p>This is the counterpart of {@link #open(FunctionContext)} for lambda arguments. It hands
     * out the generated instance itself instead of a {@link MethodHandle} onto its {@code eval}
     * method, so that applying the lambda does not pay {@link
     * MethodHandle#invokeWithArguments(Object...)}'s per-application boxing and adaptation. It is
     * only callable when the evaluator was generated for a lambda body.
     */
    public LambdaFunctionFactory openLambdaFactory(FunctionContext context) {
        final RichFunction openedInstance = openInstance(context);
        if (!(openedInstance instanceof LambdaFunctionFactory)) {
            throw new TableException(
                    String.format(
                            "Expression evaluator for '%s' was not generated for a lambda body.",
                            expressionSummary));
        }
        return (LambdaFunctionFactory) openedInstance;
    }

    private RichFunction openInstance(FunctionContext context) {
        Preconditions.checkState(
                instance == null,
                "Expression evaluator for '%s' has already been opened.",
                expressionSummary);
        try {
            instance = generatedClass.newInstance(context.getUserCodeClassLoader());
            instance.open(DefaultOpenContext.INSTANCE);
            return instance;
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Expression evaluator for '%s' could not be opened.",
                            expressionSummary),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            if (instance != null) {
                instance.close();
            }
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Expression evaluator for '%s' could not be closed.",
                            expressionSummary),
                    e);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "ExpressionEvaluator{handleSignature=%s, expression=%s}",
                methodType, expressionSummary);
    }
}
