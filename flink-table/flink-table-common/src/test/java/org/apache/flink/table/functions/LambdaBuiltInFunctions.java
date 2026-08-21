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

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The built-in functions that take a lambda argument, derived from the definitions rather than
 * listed.
 *
 * <p>Several completeness checks across the table modules have to agree on what "a higher-order
 * built-in" means: each of them asserts that some surface — a runtime implementation, a {@code
 * BaseExpressions} method, a documentation entry, a test case — exists for every one of them. That
 * only holds together if they all derive the set the same way, and deriving it means the checks
 * cover a newly added function without being extended.
 */
public class LambdaBuiltInFunctions {

    /** All built-in function definitions that declare a lambda argument. */
    public static List<BuiltInFunctionDefinition> all() {
        return BuiltInFunctionDefinitions.getDefinitions().stream()
                .filter(LambdaBuiltInFunctions::declaresLambdaArgument)
                .collect(Collectors.toList());
    }

    /** All names of {@link #all()}. */
    public static List<String> names() {
        return all().stream().map(BuiltInFunctionDefinition::getName).collect(Collectors.toList());
    }

    /**
     * Whether the given definition takes a lambda argument, i.e. whether its input type strategy is
     * a {@link LambdaInputTypeStrategy}.
     */
    public static boolean declaresLambdaArgument(BuiltInFunctionDefinition definition) {
        final DataTypeFactory typeFactory = new DataTypeFactoryMock();
        final InputTypeStrategy inputTypeStrategy =
                definition.getTypeInference(typeFactory).getInputTypeStrategy();
        return inputTypeStrategy instanceof LambdaInputTypeStrategy;
    }

    private LambdaBuiltInFunctions() {}
}
