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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Logical type of a function (i.e. a lambda expression) that maps a fixed list of argument types to
 * a single return type.
 *
 * <p>This type is used to represent higher-order function arguments such as the lambda passed to
 * {@code TRANSFORM(array, x -> x + 1)}. It describes the argument types the lambda accepts and the
 * type it produces.
 *
 * <p>Note: The runtime does not materialize a value of this type. It is a pure helper type during
 * translation and planning. Table columns cannot be declared with this type. Functions cannot
 * declare persisted return types of this type.
 *
 * <p>The serialized string representation is {@code FUNCTION<(t0, t1, ...) -> tr>} where {@code t0,
 * t1, ...} are the argument types and {@code tr} is the result type.
 */
@PublicEvolving
public final class FunctionType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "FUNCTION<(%s) -> %s>";

    private static final Class<?> INPUT_OUTPUT_CONVERSION = Function.class;

    private static final Class<?> DEFAULT_CONVERSION = Function.class;

    private final List<LogicalType> argumentTypes;

    private final LogicalType resultType;

    public FunctionType(
            boolean isNullable, List<LogicalType> argumentTypes, LogicalType resultType) {
        super(isNullable, LogicalTypeRoot.FUNCTION);
        this.argumentTypes =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        argumentTypes, "Argument types must not be null.")));
        this.resultType = Preconditions.checkNotNull(resultType, "Result type must not be null.");
    }

    public FunctionType(List<LogicalType> argumentTypes, LogicalType resultType) {
        this(true, argumentTypes, resultType);
    }

    public List<LogicalType> getArgumentTypes() {
        return argumentTypes;
    }

    public LogicalType getResultType() {
        return resultType;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new FunctionType(
                isNullable,
                argumentTypes.stream().map(LogicalType::copy).collect(Collectors.toList()),
                resultType.copy());
    }

    @Override
    public String asSummaryString() {
        return withNullability(
                FORMAT,
                argumentsToString(LogicalType::asSummaryString),
                resultType.asSummaryString());
    }

    @Override
    public String asSerializableString() {
        return withNullability(
                FORMAT,
                argumentsToString(LogicalType::asSerializableString),
                resultType.asSerializableString());
    }

    private String argumentsToString(Function<LogicalType, String> mapper) {
        return argumentTypes.stream().map(mapper).collect(Collectors.joining(", "));
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.isAssignableFrom(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.isAssignableFrom(clazz);
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        final List<LogicalType> children = new ArrayList<>(argumentTypes);
        children.add(resultType);
        return Collections.unmodifiableList(children);
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
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
        if (!super.equals(o)) {
            return false;
        }
        final FunctionType that = (FunctionType) o;
        return argumentTypes.equals(that.argumentTypes) && resultType.equals(that.resultType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), argumentTypes, resultType);
    }
}
