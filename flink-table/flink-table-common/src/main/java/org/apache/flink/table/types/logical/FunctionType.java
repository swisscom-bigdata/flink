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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.FunctionData0;
import org.apache.flink.table.data.FunctionData1;
import org.apache.flink.table.data.FunctionData2;
import org.apache.flink.table.data.FunctionData3;
import org.apache.flink.table.data.FunctionData4;
import org.apache.flink.util.function.Function0;
import org.apache.flink.util.function.Function1;
import org.apache.flink.util.function.Function2;
import org.apache.flink.util.function.Function3;
import org.apache.flink.util.function.Function4;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Logical type of a function (i.e. a lambda expression) that is passed as an argument to a
 * higher-order function such as {@code ARRAY_TRANSFORM(array, x -> x + 1)}.
 *
 * <p>The type carries the number of parameters the lambda accepts, because that is what determines
 * the runtime representation: a lambda with {@code N} parameters is a {@code FunctionN}, from
 * {@link Function0} to {@link Function4}. At arities one to three the equivalent {@link Function},
 * {@link BiFunction} and {@link TriFunction} are accepted as well, since {@link Function1}, {@link
 * Function2} and {@link Function3} merely extend them. The parameter types and the result type are
 * deliberately <i>not</i> part of the type. They are only needed for validation and type inference
 * and are exposed there through {@link
 * org.apache.flink.table.types.inference.CallContext#getLambdaArgument(int)}, in the same way that
 * {@link DescriptorType} leaves the described columns to {@code CallContext#getArgumentValue} and
 * {@link SymbolType} leaves the symbol to the value.
 *
 * <p>Like every other type, a {@code FUNCTION} type has an external and an internal representation,
 * carried by its conversion class: {@link FunctionData0}, {@link FunctionData1}, {@link
 * FunctionData2}, {@link FunctionData3} and {@link FunctionData4} are the counterparts for a
 * function that works on internal data structures. The representation belongs to the argument
 * rather than to the lambda, because the values a function feeds its lambda are its own: a function
 * that reads an {@code ARRAY} argument as {@code ArrayData} hands the lambda internal elements.
 * Declaring it on the argument is what lets the regular argument enrichment settle it, instead of
 * the call having to guess from sibling arguments.
 *
 * <p>Note: The runtime does not materialize a value of this type. It is a helper type during
 * translation, planning, and (de)serialization of plans. Table columns cannot be declared with this
 * type, functions cannot declare persisted return types of this type, and it cannot be used as a
 * state type.
 *
 * <p>Because a lambda is never materialized as a runtime value, it can never be {@code NULL} and
 * the nullability of a {@code FUNCTION} type is not meaningful. It is therefore canonicalized to a
 * single value ({@link #CANONICAL_NULLABILITY}): {@link #copy(boolean)} ignores its argument, so
 * that two lambdas of the same arity do not split into distinct types merely because a caller
 * requested a different nullability. This mirrors {@code FunctionRelDataType} on the Calcite side,
 * which is always nullable and whose {@code createWithNullability} is a no-op.
 *
 * <p>The serialized string representation is {@code FUNCTION(n)} where {@code n} is the number of
 * parameters. It round-trips through {@link
 * org.apache.flink.table.types.logical.utils.LogicalTypeParser} like every other logical type. An
 * explicit {@code NOT NULL} is accepted and ignored, consistent with the canonicalization above.
 */
@PublicEvolving
public final class FunctionType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "FUNCTION(%d)";

    /**
     * The canonical nullability of every {@code FUNCTION} type. A lambda is never materialized as a
     * runtime value, so it can never be {@code NULL} and its wrapper nullability carries no
     * meaning. Fixing it to a single value keeps lambdas of the same arity from splitting into
     * distinct types. Nullable is chosen because it matches the default construction path.
     */
    private static final boolean CANONICAL_NULLABILITY = true;

    /**
     * The maximum number of parameters for which a runtime conversion class exists. Function types
     * with more (or zero) parameters can still be constructed and planned, but their value cannot
     * be handed to a user-defined function.
     *
     * @see #getDefaultConversion()
     */
    public static final int MAX_CONVERTIBLE_PARAMETER_COUNT = 4;

    // The conversion class of a lambda is its arity's functional interface, indexed by arity. The
    // FunctionN family is the conversion class at every arity, so that one naming scheme covers the
    // whole range: java.util.function has no zero- or four-argument type, so any scheme built on
    // the JDK types alone would have to break uniformity exactly where a user is least able to
    // guess the name. Raising the bound is a matter of adding the next FunctionN and FunctionDataN
    // and extending these tables.
    private static final Class<?>[] CONVERSIONS = {
        Function0.class, Function1.class, Function2.class, Function3.class, Function4.class,
    };

    // Arities one to three additionally accept the equivalent JDK/Flink interface, which Function1,
    // Function2 and Function3 extend and which a Java developer is more likely to reach for. Both
    // spellings are supported conversions, so either may be declared on an eval() parameter and
    // either may be named in a bridgedTo(...) call; the declared one is what the code generator
    // implements. Arity zero has no alias: the generated object's method is always apply(), while
    // the single abstract method of java.util.function.Supplier is get(), so Supplier cannot be the
    // implemented interface. Function0 extends Supplier instead, which lets a function still
    // declare a Supplier parameter and receive the generated Function0 for it -- but Supplier is
    // not itself a supported conversion and cannot be bridged to.
    private static final @Nullable Class<?>[] ALIAS_CONVERSIONS = {
        null, Function.class, BiFunction.class, TriFunction.class, null,
    };

    // A function that works on internal data structures receives its lambda argument over internal
    // data as well, because the values it feeds the lambda are its own. The representation is
    // therefore carried by the conversion class, exactly as it is for an ARRAY argument that is
    // received either as ArrayData or as an array of external elements.
    private static final Class<?>[] INTERNAL_CONVERSIONS = {
        FunctionData0.class,
        FunctionData1.class,
        FunctionData2.class,
        FunctionData3.class,
        FunctionData4.class,
    };

    private final int parameterCount;

    /**
     * Creates a {@code FUNCTION} type for a lambda that accepts {@code parameterCount} parameters.
     *
     * <p>Only parameter counts between zero and {@link #MAX_CONVERTIBLE_PARAMETER_COUNT} have a
     * runtime representation and are therefore accepted here. Other arities occur internally only,
     * for a lambda that carries lifted captures in addition to its user-visible parameters: the
     * planner's type factory derives such a type from a Calcite {@code FunctionSqlType} whose field
     * count includes the lifted parameters, and {@link
     * org.apache.flink.table.types.logical.utils.LogicalTypeParser} reads one back from {@link
     * #asSerializableString()}. {@link #ofUncheckedArity(int)} is the construction path for those.
     *
     * @throws ValidationException if the count is negative or greater than {@link
     *     #MAX_CONVERTIBLE_PARAMETER_COUNT}
     */
    public FunctionType(int parameterCount) {
        this(parameterCount, true);
    }

    /**
     * Creates a {@code FUNCTION} type without checking that the arity has a runtime representation.
     *
     * <p>A lambda that carries lifted captures accepts more parameters than the user-visible arity
     * that {@link #MAX_CONVERTIBLE_PARAMETER_COUNT} bounds. Two internal paths construct such a
     * type: the planner's type factory, converting a Calcite {@code FunctionSqlType} whose field
     * count includes the lifted parameters, and {@link
     * org.apache.flink.table.types.logical.utils.LogicalTypeParser}, reading back the {@link
     * #asSerializableString()} form that every logical type must round-trip through. A compiled
     * plan is not one of them: it stores the lambda's parameters and body, never this type. No
     * value of the resulting type can be handed to a user-defined function, so this is not a public
     * construction path. A negative count remains rejected.
     */
    @Internal
    public static FunctionType ofUncheckedArity(int parameterCount) {
        return new FunctionType(parameterCount, false);
    }

    private FunctionType(int parameterCount, boolean checkConvertibleArity) {
        super(CANONICAL_NULLABILITY, LogicalTypeRoot.FUNCTION);
        if (checkConvertibleArity
                && (parameterCount < 0 || parameterCount > MAX_CONVERTIBLE_PARAMETER_COUNT)) {
            throw new ValidationException(requestedParameterCountError(parameterCount));
        }
        if (parameterCount < 0) {
            throw new IllegalArgumentException("Parameter count must not be negative.");
        }
        this.parameterCount = parameterCount;
    }

    /**
     * The error for an arity that has no runtime representation.
     *
     * <p>The check lives here rather than next to the other lambda arity validation in {@code
     * org.apache.flink.table.types.inference.LambdaTypeValidation} because this type owns {@link
     * #MAX_CONVERTIBLE_PARAMETER_COUNT} and because the reverse dependency would make the logical
     * and the inference package mutually dependent.
     */
    private static String requestedParameterCountError(int parameterCount) {
        return String.format(
                "A lambda argument must have between 0 and %d parameters, but a FUNCTION type "
                        + "with %d parameters was requested. A lambda is passed to a user-defined "
                        + "function as an org.apache.flink.util.function.FunctionN, where N is its "
                        + "number of parameters and ranges from Function0 to Function%d, so no other "
                        + "number of parameters can be represented at runtime.",
                MAX_CONVERTIBLE_PARAMETER_COUNT, parameterCount, MAX_CONVERTIBLE_PARAMETER_COUNT);
    }

    /**
     * The internal-data conversion class for a lambda of the given arity, falling back to the
     * one-parameter handle for an arity that has no functional interface (a lambda carrying lifted
     * captures).
     */
    @Internal
    public static Class<?> internalConversionClass(int parameterCount) {
        if (parameterCount < 0 || parameterCount >= INTERNAL_CONVERSIONS.length) {
            return FunctionData1.class;
        }
        return INTERNAL_CONVERSIONS[parameterCount];
    }

    /** Returns the number of parameters the lambda accepts. */
    public int getParameterCount() {
        return parameterCount;
    }

    /**
     * Returns a copy of this type. The {@code ignoredNullability} argument has no effect because a
     * {@code FUNCTION} type always has {@link #CANONICAL_NULLABILITY}.
     */
    @Override
    public LogicalType copy(boolean ignoredNullability) {
        return ofUncheckedArity(parameterCount);
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, parameterCount);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, parameterCount);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return supportsConversion(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return supportsConversion(clazz);
    }

    private boolean supportsConversion(Class<?> clazz) {
        return isAssignableFrom(conversionClass(false), clazz)
                || isAssignableFrom(aliasConversionClass(), clazz)
                || isAssignableFrom(conversionClass(true), clazz);
    }

    private static boolean isAssignableFrom(@Nullable Class<?> conversion, Class<?> clazz) {
        return conversion != null && conversion.isAssignableFrom(clazz);
    }

    /**
     * Returns the conversion class of the corresponding functional interface, or {@link Function}
     * for arities that have none. Every logical type must name a default conversion class; for an
     * unsupported arity that class is intentionally one that {@link
     * #supportsInputConversion(Class)} and {@link #supportsOutputConversion(Class)} reject, because
     * no value of such a type can be handed to a user-defined function. A caller never reaches that
     * state through public API: {@link org.apache.flink.table.api.DataTypes#FUNCTION(int)} and the
     * {@link #FunctionType(int) constructor} both reject an unsupported arity up front, and so do
     * the SQL and Table API binding paths, on the parameter types that a {@code
     * LambdaInputTypeStrategy} derives for a call. Only the {@link #ofUncheckedArity(int) internal
     * factory} admits the arities that carry lifted captures, as the planner's type factory and the
     * serializable-string parser produce them.
     */
    @Override
    public Class<?> getDefaultConversion() {
        final Class<?> conversion = conversionClass(false);
        return conversion != null ? conversion : Function.class;
    }

    /**
     * The runtime conversion class for this function type in the requested representation, or
     * {@code null} for unsupported arities.
     */
    private @Nullable Class<?> conversionClass(boolean internal) {
        final Class<?>[] conversions = internal ? INTERNAL_CONVERSIONS : CONVERSIONS;
        if (parameterCount < 0 || parameterCount >= conversions.length) {
            return null;
        }
        return conversions[parameterCount];
    }

    /**
     * The equivalent JDK or Flink interface accepted alongside this arity's {@code FunctionN}
     * conversion class, or {@code null} where none exists.
     */
    private @Nullable Class<?> aliasConversionClass() {
        if (parameterCount < 0 || parameterCount >= ALIAS_CONVERSIONS.length) {
            return null;
        }
        return ALIAS_CONVERSIONS[parameterCount];
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
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
        return parameterCount == that.parameterCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parameterCount);
    }
}
