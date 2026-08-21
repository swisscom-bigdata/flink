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

package org.apache.flink.table.functions;

import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for invariants that hold across all {@link BuiltInFunctionDefinitions}. */
class BuiltInFunctionDefinitionsTest {

    /**
     * A function that declares a lambda argument must be routed through the bridged stack, i.e.
     * {@link BuiltInFunctionDefinition#hasRuntimeImplementation()} must be true so that its
     * evaluation is a runtime class rather than a planner-side implementation.
     *
     * <p>A planner-side implementation costs a raw Calcite operator in {@code
     * FlinkSqlOperatorTable}, a Calcite operand type checker duplicating the function's {@link
     * InputTypeStrategy}, a hand-written return-type inference method, an {@code
     * ExpressionConverter} mapping and a bespoke code generator, all of which have to be kept in
     * sync with the definition by hand and none of which a definition with a runtime implementation
     * needs. No built-in higher-order function pays that cost any more, and none may start to.
     *
     * <p>The check derives the set it applies to from whether the input type strategy is a {@link
     * LambdaInputTypeStrategy}, so it covers a new higher-order function without being extended.
     */
    @Test
    void testLambdaFunctionsHaveRuntimeImplementation() {
        final List<String> unbridged =
                LambdaBuiltInFunctions.all().stream()
                        .filter(definition -> !definition.hasRuntimeImplementation())
                        .map(BuiltInFunctionDefinition::getName)
                        .collect(Collectors.toList());

        assertThat(unbridged)
                .as(
                        "A built-in function that declares a lambda argument must provide a runtime "
                                + "implementation via runtimeClass(...) or runtimeProvided().")
                .isEmpty();
    }
}
