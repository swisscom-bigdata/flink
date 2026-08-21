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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.LambdaBuiltInFunctions;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Requires every built-in function that takes a lambda to be tested for what it computes.
 *
 * <p>The expected set is derived from {@link LambdaBuiltInFunctions}, so a newly added higher-order
 * function is covered without extending this test — it simply starts failing until its cases exist.
 *
 * <p>Restore and serde coverage is deliberately <b>not</b> checked per function. Those fixtures pin
 * the serialized lambda representation, which is identical for every higher-order function and for
 * user-defined ones, so they are tracked generically by {@code RestoreTestCompletenessTest} per
 * ExecNode instead. A new built-in adds no restore artifact.
 */
class HigherOrderFunctionCoverageTest {

    @Test
    void testEveryLambdaFunctionHasSemanticCoverage() {
        final Set<String> tested =
                new HigherOrderFunctionsITCase()
                        .getTestSetSpecs()
                        .map(BuiltInFunctionTestBase.TestSetSpec::getDefinition)
                        .filter(Objects::nonNull)
                        .map(BuiltInFunctionDefinition::getName)
                        .collect(Collectors.toSet());

        assertThat(missingFrom(tested))
                .as(
                        "Every built-in function that declares a lambda argument needs execution "
                                + "cases in %s, declared with TestSetSpec.forFunction(...) so that "
                                + "the function under test is recorded.",
                        HigherOrderFunctionsITCase.class.getSimpleName())
                .isEmpty();
    }

    private static List<String> missingFrom(Set<String> tested) {
        return LambdaBuiltInFunctions.names().stream()
                .filter(name -> !tested.contains(name))
                .collect(Collectors.toList());
    }
}
