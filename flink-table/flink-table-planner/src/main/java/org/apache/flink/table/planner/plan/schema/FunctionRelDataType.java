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

package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.FunctionType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/** The {@link RelDataType} representation of a {@link FunctionType}. */
@Internal
public final class FunctionRelDataType extends AbstractSqlType {

    private final FunctionType functionType;

    public FunctionRelDataType(FunctionType functionType) {
        super(SqlTypeName.OTHER, functionType.isNullable(), null);
        this.functionType = functionType;
        computeDigest();
    }

    public FunctionType getFunctionType() {
        return functionType;
    }

    public FunctionRelDataType createWithNullability(boolean nullable) {
        if (nullable == isNullable()) {
            return this;
        }
        return new FunctionRelDataType((FunctionType) functionType.copy(nullable));
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(functionType.asSummaryString());
    }

    @Override
    protected void computeDigest() {
        final StringBuilder sb = new StringBuilder();
        generateTypeString(sb, true);
        digest = sb.toString();
    }
}
