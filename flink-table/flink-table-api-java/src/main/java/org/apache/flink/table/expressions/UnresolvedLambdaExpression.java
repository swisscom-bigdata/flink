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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unresolved lambda expression {@code (p0, p1, ...) -> body} used as an argument of a higher-order
 * function such as {@code ARRAY_TRANSFORM(array, x -> x + 1)}.
 *
 * <p>A lambda is not a value that flows at runtime; it is only valid as an argument of a
 * higher-order function. During resolution the parameter types are bound from the sibling arguments
 * of the enclosing higher-order call and the body is resolved in a scope containing the parameters,
 * which turns this into a resolved {@link LambdaExpression}.
 *
 * <p>The parameter names must be unique, because the body binds them by name.
 */
@PublicEvolving
public final class UnresolvedLambdaExpression implements Expression {

    private final List<String> parameterNames;

    private final Expression body;

    public UnresolvedLambdaExpression(List<String> parameterNames, Expression body) {
        this.parameterNames =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        parameterNames, "Parameter names must not be null.")));
        validateUniqueParameterNames(this.parameterNames);
        this.body = Preconditions.checkNotNull(body, "Body must not be null.");
    }

    /**
     * Rejects duplicate parameter names. The parameters of a lambda are bound by name, so a
     * duplicate would silently shadow the earlier parameter of the same name and leave it
     * unreferenceable. Validated here rather than during resolution so that no traversal of the
     * unresolved expression (which also binds parameters by name) can observe an ambiguous lambda.
     */
    private static void validateUniqueParameterNames(List<String> parameterNames) {
        final Set<String> seen = new HashSet<>();
        final List<String> duplicates =
                parameterNames.stream()
                        .filter(name -> !seen.add(name))
                        .distinct()
                        .collect(Collectors.toList());
        if (!duplicates.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "The parameters of a lambda expression must have unique names. "
                                    + "Found duplicates: %s",
                            duplicates));
        }
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public Expression getBody() {
        return body;
    }

    @Override
    public String asSummaryString() {
        final String params =
                parameterNames.size() == 1
                        ? parameterNames.get(0)
                        : "(" + String.join(", ", parameterNames) + ")";
        return params + " -> " + body.asSummaryString();
    }

    @Override
    public List<Expression> getChildren() {
        // The body is intentionally hidden from generic expression traversals: it must be resolved
        // in the dedicated lambda scope (with bound parameter types), not as a standalone
        // expression.
        return Collections.emptyList();
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
        final UnresolvedLambdaExpression that = (UnresolvedLambdaExpression) o;
        return parameterNames.equals(that.parameterNames) && body.equals(that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameterNames, body);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
