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

import org.apache.flink.table.functions.LambdaBuiltInFunctions;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the Table API exposes every built-in function that takes a lambda.
 *
 * <p>PyFlink parity is covered separately by {@code test_expression_completeness.py}, which derives
 * the expected Python methods from the public {@code ApiExpression} surface. This test closes the
 * step before that one: a built-in that SQL can call but {@link BaseExpressions} does not expose is
 * invisible to both.
 */
class BaseExpressionsLambdaCompletenessTest {

    /**
     * A built-in function that declares a lambda argument must be callable from the Table API.
     *
     * <p>The expected set is derived from whether the input type strategy is a {@link
     * LambdaInputTypeStrategy} rather than listed, so a new higher-order function is covered
     * without extending this test.
     */
    @Test
    void testLambdaFunctionsAreExposedOnBaseExpressions() {
        final Set<String> methodNames =
                Arrays.stream(BaseExpressions.class.getMethods())
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        final List<String> missing =
                LambdaBuiltInFunctions.names().stream()
                        .filter(name -> !methodNames.contains(toMethodName(name)))
                        .collect(Collectors.toList());

        assertThat(missing)
                .as(
                        "Every built-in function that declares a lambda argument needs a "
                                + "BaseExpressions method so that it can be called from the Table API.")
                .isEmpty();
    }

    /** Turns a definition name such as {@code ARRAY_TRANSFORM} into {@code arrayTransform}. */
    private static String toMethodName(String definitionName) {
        final String[] parts = definitionName.toLowerCase().split("_");
        final StringBuilder builder = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            builder.append(Character.toUpperCase(parts[i].charAt(0))).append(parts[i].substring(1));
        }
        return builder.toString();
    }
}
