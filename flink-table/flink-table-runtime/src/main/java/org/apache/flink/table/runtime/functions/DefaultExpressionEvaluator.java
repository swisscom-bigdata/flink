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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.metrics.UdfMetrics;
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
        final RichFunction openedInstance = openInstance(context, new UdfMetrics[0]);
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
     *
     * @param context the context of the enclosing generated class, with which the functions called
     *     from the body are opened as well
     * @param udfMetrics the enclosing generated class's metrics handles for the user-defined
     *     functions called from the body, in the order the body requested them
     */
    public LambdaFunctionFactory openLambdaFactory(
            FunctionContext context, UdfMetrics[] udfMetrics) {
        final RichFunction openedInstance = openInstance(context, udfMetrics);
        if (!(openedInstance instanceof LambdaFunctionFactory)) {
            throw new TableException(
                    String.format(
                            "Expression evaluator for '%s' was not generated for a lambda body.",
                            expressionSummary));
        }
        return (LambdaFunctionFactory) openedInstance;
    }

    private RichFunction openInstance(FunctionContext context, UdfMetrics[] udfMetrics) {
        Preconditions.checkState(
                instance == null,
                "Expression evaluator for '%s' has already been opened.",
                expressionSummary);
        try {
            instance = generatedClass.newInstance(context.getUserCodeClassLoader());
            instance.open(new EvaluatorOpenContext(context, udfMetrics));
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

    /**
     * Returns the {@link FunctionContext} for opening the functions that a generated evaluator
     * class calls.
     *
     * <p>An evaluator is not an operator and has no runtime context of its own. It passes on the
     * context it was opened with, so that a function called from a lambda body sees the same job
     * parameters, task info, metric group and distributed cache as one called directly in the
     * enclosing operator.
     */
    public static FunctionContext functionContextOf(
            OpenContext openContext, ClassLoader classLoader) {
        if (openContext instanceof EvaluatorOpenContext) {
            return ((EvaluatorOpenContext) openContext).functionContext;
        }
        return new FunctionContext(null, classLoader, openContext);
    }

    /**
     * Returns the metrics handle of a user-defined function that a generated evaluator class calls.
     *
     * <p>The handle is the one of the enclosing generated class, so that every call site of a
     * function in an operator feeds a single histogram and counter, whether it is inside a lambda
     * body or not. Registering a second handle under the same name would collide in the metric
     * group and go unreported. A handle that the enclosing class did not pass on, which only
     * happens for an evaluator opened through {@link #open(FunctionContext)}, is registered in the
     * context's metric group instead.
     */
    public static UdfMetrics udfMetricsOf(
            OpenContext openContext, int index, String udfName, int sampleInterval) {
        if (openContext instanceof EvaluatorOpenContext) {
            final UdfMetrics[] udfMetrics = ((EvaluatorOpenContext) openContext).udfMetrics;
            if (index < udfMetrics.length) {
                return udfMetrics[index];
            }
        }
        return UdfMetrics.register(
                functionContextOf(openContext, null).getMetricGroup(), udfName, sampleInterval);
    }

    /** The {@link OpenContext} of a generated evaluator class. */
    private static final class EvaluatorOpenContext implements OpenContext {
        private final FunctionContext functionContext;
        private final UdfMetrics[] udfMetrics;

        private EvaluatorOpenContext(FunctionContext functionContext, UdfMetrics[] udfMetrics) {
            this.functionContext = Preconditions.checkNotNull(functionContext);
            this.udfMetrics = Preconditions.checkNotNull(udfMetrics);
        }
    }
}
