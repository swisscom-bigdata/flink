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

package org.apache.flink.docs.configuration;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.docs.util.Utils;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.LambdaInputTypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the SQL functions documentation covers the built-in functions that take a lambda.
 *
 * <p>Unlike configuration options, the SQL function documentation is written by hand and nothing
 * generates it, so an added function is silently undocumented. The check is restricted to
 * lambda-declaring built-ins because the documentable set cannot otherwise be derived: {@link
 * BuiltInFunctionDefinitions} also holds operators, aliases and internal helpers that legitimately
 * have no entry. For lambda-declaring functions the expected set is exact.
 */
class SqlFunctionsDocsCompletenessITCase {

    private static final List<String> SQL_FUNCTION_FILES =
            List.of("sql_functions.yml", "sql_functions_zh.yml");

    @Test
    void testLambdaFunctionsAreDocumented() throws IOException {
        final List<String> lambdaFunctions =
                BuiltInFunctionDefinitions.getDefinitions().stream()
                        .filter(SqlFunctionsDocsCompletenessITCase::declaresLambdaArgument)
                        .map(BuiltInFunctionDefinition::getName)
                        .collect(Collectors.toList());

        assertThat(lambdaFunctions)
                .as("Expected the higher-order built-in functions to be discoverable.")
                .isNotEmpty();

        for (String file : SQL_FUNCTION_FILES) {
            final String content = readDocsData(file);
            final List<String> undocumented =
                    lambdaFunctions.stream()
                            .filter(name -> !content.contains("- sql: " + name + "("))
                            .collect(Collectors.toList());

            assertThat(undocumented)
                    .as(
                            "Every built-in function that declares a lambda argument needs an entry "
                                    + "in docs/data/%s.",
                            file)
                    .isEmpty();
        }
    }

    private static String readDocsData(String fileName) throws IOException {
        final Path path =
                Paths.get(Utils.getProjectRootDir(), "docs", "data", fileName).toAbsolutePath();
        assertThat(Files.exists(path)).as("Expected %s to exist.", path).isTrue();
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static boolean declaresLambdaArgument(BuiltInFunctionDefinition definition) {
        final InputTypeStrategy inputTypeStrategy =
                definition.getTypeInference(UnusedDataTypeFactory.INSTANCE).getInputTypeStrategy();
        return inputTypeStrategy instanceof LambdaInputTypeStrategy;
    }

    /**
     * {@link BuiltInFunctionDefinition#getTypeInference(DataTypeFactory)} takes a factory to build
     * the strategies, but declaring a lambda argument is a property of the strategy itself and does
     * not resolve any data type.
     */
    private enum UnusedDataTypeFactory implements DataTypeFactory {
        INSTANCE;

        @Override
        public DataType createDataType(AbstractDataType<?> abstractDataType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataType createDataType(String typeString) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataType createDataType(UnresolvedIdentifier identifier) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> DataType createDataType(Class<T> clazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> DataType createDataType(TypeInformation<T> typeInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> DataType createRawDataType(Class<T> clazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> DataType createRawDataType(TypeInformation<T> typeInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogicalType createLogicalType(String typeString) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LogicalType createLogicalType(UnresolvedIdentifier identifier) {
            throw new UnsupportedOperationException();
        }
    }
}
