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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Resolved lambda expression {@code (p0, p1, ...) -> body} used as an argument of a higher-order
 * function such as {@code ARRAY_TRANSFORM(array, x -> x + 1)}.
 *
 * <p>The parameters are represented as {@link LocalReferenceExpression}s whose types were bound
 * from the sibling arguments of the enclosing higher-order call. The output data type is a {@code
 * FUNCTION} type carrying the number of parameters; the parameter and body types are reached
 * through {@link #getParameters()} and {@link #getBody()} (and exposed to type inference as a
 * {@link org.apache.flink.table.types.inference.LambdaInfo}).
 *
 * <p>The number of parameters must be one, two, or three, because that is what a lambda can be
 * represented as at runtime (see {@link DataTypes#FUNCTION(int)}).
 */
@PublicEvolving
public final class LambdaExpression implements ResolvedExpression {

    private final List<LocalReferenceExpression> parameters;

    private final ResolvedExpression body;

    private final DataType outputDataType;

    public LambdaExpression(List<LocalReferenceExpression> parameters, ResolvedExpression body) {
        this.parameters =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        parameters, "Parameters must not be null.")));
        this.body = Preconditions.checkNotNull(body, "Body must not be null.");
        this.outputDataType = DataTypes.FUNCTION(this.parameters.size());
    }

    public List<LocalReferenceExpression> getParameters() {
        return parameters;
    }

    public ResolvedExpression getBody() {
        return body;
    }

    @Override
    public DataType getOutputDataType() {
        return outputDataType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        final List<ResolvedExpression> children = new ArrayList<>(parameters);
        children.add(body);
        return Collections.unmodifiableList(children);
    }

    @Override
    public String asSummaryString() {
        final String params =
                parameters.size() == 1
                        ? parameters.get(0).asSummaryString()
                        : parameters.stream()
                                .map(LocalReferenceExpression::asSummaryString)
                                .collect(Collectors.joining(", ", "(", ")"));
        return params + " -> " + body.asSummaryString();
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        final String params =
                parameters.size() == 1
                        ? parameters.get(0).asSerializableString(sqlFactory)
                        : parameters.stream()
                                .map(p -> p.asSerializableString(sqlFactory))
                                .collect(Collectors.joining(", ", "(", ")"));
        return params + " -> " + body.asSerializableString(sqlFactory);
    }

    @Override
    public List<Expression> getChildren() {
        final List<Expression> children = new ArrayList<>(parameters);
        children.add(body);
        return Collections.unmodifiableList(children);
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final LambdaExpression that = (LambdaExpression) o;
        return parameters.equals(that.parameters) && body.equals(that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters, body);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
