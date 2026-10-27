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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExprCodeGenerator}. */
class ExprCodeGeneratorTest {

    @Test
    void testValueOfTheOperatorWithoutOperatorContext() {
        // A lambda body is compiled without an operator context (see BridgingFunctionGenUtil).
        // Validation and capture lifting keep values of the operator out of a body; if one still
        // reaches code generation, it fails with a message instead of uncompilable code.
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final RexBuilder rexBuilder =
                new FlinkRexBuilder(new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE));
        final RexNode proctime =
                rexBuilder.makeCall(
                        FlinkSqlOperatorTable.PROCTIME_MATERIALIZE,
                        rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME));
        final ExprCodeGenerator generator =
                new ExprCodeGenerator(
                        new CodeGeneratorContext(new Configuration(), classLoader), false);
        generator.contextTerm_$eq(null);

        assertThatThrownBy(() -> generator.generateExpression(proctime))
                .isInstanceOf(CodeGenException.class)
                .hasMessage(
                        "PROCTIME cannot be evaluated here, for example in the body of a lambda,"
                                + " because it needs the context of the operator.");
    }
}
