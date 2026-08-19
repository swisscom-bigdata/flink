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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.planner.plan.schema.GenericRelDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BitmapType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DescriptorType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkTypeFactory}. */
@Execution(ExecutionMode.CONCURRENT)
class FlinkTypeFactoryTest {

    static Stream<LogicalType> testInternalToRelType() {
        return Stream.of(
                new BooleanType(),
                new TinyIntType(),
                VarCharType.STRING_TYPE,
                new DoubleType(),
                new FloatType(),
                new IntType(),
                new BigIntType(),
                new SmallIntType(),
                new VarBinaryType(VarBinaryType.MAX_LENGTH),
                new DateType(),
                new TimeType(),
                new TimestampType(3),
                new LocalZonedTimestampType(3),
                new ArrayType(new DoubleType()),
                new MapType(new DoubleType(), VarCharType.STRING_TYPE),
                RowType.of(new DoubleType(), VarCharType.STRING_TYPE),
                new RawType<>(
                        DayOfWeek.class,
                        new KryoSerializer<>(DayOfWeek.class, new SerializerConfigImpl())));
    }

    @MethodSource("testInternalToRelType")
    @ParameterizedTest
    void testInternalToRelType(LogicalType logicalType) {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(
                                        logicalType.copy(false))))
                .isEqualTo(logicalType.copy(false));
        // twice for cache.
        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(
                                        logicalType.copy(false))))
                .isEqualTo(logicalType.copy(false));
    }

    @Test
    void testInternalToRelTypeNull() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        LogicalType logicalType = new NullType();

        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));

        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
    }

    @Test
    void testDayTimeIntervalLeadingPrecisionUpToMaxIsSupported() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType intervalType =
                typeFactory.createSqlIntervalType(
                        new SqlIntervalQualifier(
                                TimeUnit.DAY,
                                DayTimeIntervalType.MAX_DAY_PRECISION,
                                TimeUnit.SECOND,
                                RelDataType.PRECISION_NOT_SPECIFIED,
                                SqlParserPos.ZERO));

        assertThat(FlinkTypeFactory.toLogicalType(intervalType))
                .isEqualTo(DataTypes.INTERVAL(DataTypes.SECOND(3)).notNull().getLogicalType());
    }

    @Test
    void testDayTimeIntervalLeadingPrecisionAboveMaxIsRejected() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType intervalType =
                typeFactory.createSqlIntervalType(
                        new SqlIntervalQualifier(
                                TimeUnit.DAY,
                                DayTimeIntervalType.MAX_DAY_PRECISION + 1,
                                TimeUnit.SECOND,
                                RelDataType.PRECISION_NOT_SPECIFIED,
                                SqlParserPos.ZERO));

        assertThatThrownBy(() -> FlinkTypeFactory.toLogicalType(intervalType))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "DAY_INTERVAL_TYPES precision is not supported: "
                                + (DayTimeIntervalType.MAX_DAY_PRECISION + 1));
    }

    @Test
    void testDecimalInferType() {
        assertThat(LogicalTypeMerging.findSumAggType(new DecimalType(10, 5)))
                .isEqualTo(new DecimalType(38, 5));
        assertThat(LogicalTypeMerging.findAvgAggType(new DecimalType(10, 5)))
                .isEqualTo(new DecimalType(38, 6));
    }

    @Test
    void testCanonizeType() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        TypeInformation<?> genericTypeInfo = Types.GENERIC(TestClass.class);
        TypeInformation<?> genericTypeInfo2 = Types.GENERIC(TestClass2.class);
        RelDataType genericRelType =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(genericTypeInfo));
        RelDataType genericRelType2 =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(genericTypeInfo));
        RelDataType genericRelType3 =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(genericTypeInfo2));

        assertThat(genericRelType).as("The type expect to be canonized").isEqualTo(genericRelType2);
        assertThat(genericRelType)
                .as("The type expect to be not canonized")
                .isNotEqualTo(genericRelType3);
        assertThat(typeFactory.builder().add("f0", genericRelType).build())
                .as("The type expect to be not canonized")
                .isNotEqualTo(typeFactory.builder().add("f0", genericRelType3).build());
    }

    @Test
    void testLeastRestrictiveFailsForDifferentRawTypes() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType rawType1 =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(Types.GENERIC(TestClass.class)));
        RelDataType rawType2 =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(Types.GENERIC(TestClass2.class)));

        // Two different generic RAW types have no common type: this must fail fast instead of
        // silently resolving to an untyped ANY column. Regression guard for narrowing the guard to
        // GenericRelDataType: behaviour for generic RAW types must be unchanged by that narrowing.
        assertThatThrownBy(() -> typeFactory.leastRestrictive(Arrays.asList(rawType1, rawType2)))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Generic RAW types must have a common type information");
    }

    @Test
    void testLeastRestrictiveResolvesIdenticalRawTypes() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType rawType =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(Types.GENERIC(TestClass.class)));

        // Two identical RAW types do have a common type (themselves): the relaxed leastRestrictive
        // must still resolve them via the all-identical branch rather than failing. Only
        // *differing* RAW types have no common type.
        assertThat(typeFactory.leastRestrictive(Arrays.asList(rawType, rawType)))
                .isEqualTo(rawType);
    }

    @Test
    void testLeastRestrictiveReturnsNullForIncompatibleNonRawTypes() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType intType = typeFactory.createFieldTypeFromLogicalType(new IntType());
        RelDataType booleanType = typeFactory.createFieldTypeFromLogicalType(new BooleanType());

        // Incompatible non-RAW types have no common type: the wrapper defers to Calcite, which
        // returns null (the caller then reports a validation error). It must NOT throw the RAW-only
        // "Generic RAW types must have a common type information" exception, which is now narrowed
        // to real RAW types.
        assertThat(typeFactory.leastRestrictive(Arrays.asList(intType, booleanType))).isNull();
    }

    @Test
    void testLeastRestrictiveDefersForCalciteNativeAny() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        // A Calcite-native basic ANY -- as created by SqlFirstLastValueAggFunction for the
        // FIRST_VALUE/LAST_VALUE operand inference, and by the window and ML table functions --
        // is SqlTypeName.ANY but is NOT a GenericRelDataType. Previously the guard keyed off the
        // SQL type name alone and threw the RAW-only "Generic RAW types must have a common type
        // information" for it, i.e. a RAW-specific error for a query containing no RAW type.
        // The guard now keys off GenericRelDataType, so this defers to Calcite's default
        // least-restrictive logic, which treats ANY as the top type. This is the one behaviour that
        // the narrowing actually changed, so it is pinned here explicitly.
        // Note that an unresolved dynamic parameter "?" is NOT affected: it is typed with
        // createUnknownType(), which Flink maps to SqlTypeName.NULL (see
        // testUnknownTypeIsNotSqlTypeNameAny).
        RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
        RelDataType intType = typeFactory.createFieldTypeFromLogicalType(new IntType());

        assertThatCode(() -> typeFactory.leastRestrictive(Arrays.asList(anyType, intType)))
                .doesNotThrowAnyException();
        assertThat(typeFactory.leastRestrictive(Arrays.asList(anyType, intType)))
                .isNotNull()
                .extracting(RelDataType::getSqlTypeName)
                .isEqualTo(SqlTypeName.ANY);
    }

    @Test
    void testLeastRestrictiveStillFailsWhenNativeAnyMeetsRawType() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        // The narrowing must not weaken the RAW guard: as soon as one input really is a generic RAW
        // type, the fail-fast behaviour applies even though the other input is a native ANY that
        // shares its SQL type name.
        RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
        RelDataType rawType =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(Types.GENERIC(TestClass.class)));

        assertThatThrownBy(() -> typeFactory.leastRestrictive(Arrays.asList(anyType, rawType)))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Generic RAW types must have a common type information");
    }

    static Stream<Arguments> testLeastRestrictive() {
        return Stream.of(
                // Since the problem is actual for collection
                // then tests are for array, map, multiset
                // Also as https://issues.apache.org/jira/browse/CALCITE-4603 says
                // before Calcite 1.27.0  it derived the type of nested collection based on the last
                // element, for that reason the type of the last element is narrower
                // than the type of element in the middle
                Arguments.of(
                        Arrays.asList(
                                new ArrayType(new VarCharType(6)),
                                new ArrayType(VarCharType.STRING_TYPE),
                                new ArrayType(new CharType(1))),
                        new ArrayType(VarCharType.STRING_TYPE)),
                Arguments.of(
                        Arrays.asList(
                                new MultisetType(new VarCharType(6)),
                                new MultisetType(VarCharType.STRING_TYPE),
                                new MultisetType(new CharType(1))),
                        new MultisetType(VarCharType.STRING_TYPE)),
                Arguments.of(
                        Arrays.asList(
                                new MapType(new CharType(1), new CharType(1)),
                                new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE),
                                new MapType(new CharType(1), new CharType(1))),
                        new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)),
                Arguments.of(
                        Arrays.asList(
                                new MapType(new CharType(1), new VarCharType(6)),
                                new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE),
                                new MapType(new CharType(1), new CharType(1))),
                        new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)),
                // Scalar widening still works after the leastRestrictive change (e.g. used by
                // CASE/COALESCE/UNION over numeric columns).
                Arguments.of(
                        Arrays.asList(new IntType(false), new BigIntType(false)),
                        new BigIntType(false)),
                // A nullable operand makes the common type nullable.
                Arguments.of(
                        Arrays.asList(new IntType(false), new BigIntType(true)),
                        new BigIntType(true)));
    }

    @MethodSource("testLeastRestrictive")
    @ParameterizedTest
    void testLeastRestrictive(List<LogicalType> input, LogicalType expected) {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        assertThat(
                        typeFactory.leastRestrictive(
                                input.stream()
                                        .map(typeFactory::createFieldTypeFromLogicalType)
                                        .collect(Collectors.toList())))
                .isEqualTo(typeFactory.createFieldTypeFromLogicalType(expected));
    }

    /**
     * A representative instance for every {@link LogicalTypeRoot}. Kept exhaustive by {@link
     * #testEveryLogicalTypeRootIsAudited()} so that a newly added root cannot silently escape the
     * {@code SqlTypeName.ANY} audit below.
     */
    private static Map<LogicalTypeRoot, LogicalType> logicalTypeRootRepresentatives() {
        final Map<LogicalTypeRoot, LogicalType> types = new EnumMap<>(LogicalTypeRoot.class);
        types.put(LogicalTypeRoot.CHAR, new CharType(5));
        types.put(LogicalTypeRoot.VARCHAR, VarCharType.STRING_TYPE);
        types.put(LogicalTypeRoot.BOOLEAN, new BooleanType());
        types.put(LogicalTypeRoot.BINARY, new BinaryType(5));
        types.put(LogicalTypeRoot.VARBINARY, new VarBinaryType(5));
        types.put(LogicalTypeRoot.DECIMAL, new DecimalType(10, 2));
        types.put(LogicalTypeRoot.TINYINT, new TinyIntType());
        types.put(LogicalTypeRoot.SMALLINT, new SmallIntType());
        types.put(LogicalTypeRoot.INTEGER, new IntType());
        types.put(LogicalTypeRoot.BIGINT, new BigIntType());
        types.put(LogicalTypeRoot.FLOAT, new FloatType());
        types.put(LogicalTypeRoot.DOUBLE, new DoubleType());
        types.put(LogicalTypeRoot.DATE, new DateType());
        types.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, new TimeType());
        types.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, new TimestampType(3));
        types.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, new ZonedTimestampType(3));
        types.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, new LocalZonedTimestampType(3));
        types.put(
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR));
        types.put(
                LogicalTypeRoot.INTERVAL_DAY_TIME,
                new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND));
        types.put(LogicalTypeRoot.ARRAY, new ArrayType(new IntType()));
        types.put(LogicalTypeRoot.MULTISET, new MultisetType(new IntType()));
        types.put(LogicalTypeRoot.MAP, new MapType(new IntType(), new IntType()));
        types.put(LogicalTypeRoot.ROW, RowType.of(new IntType()));
        types.put(
                LogicalTypeRoot.DISTINCT_TYPE,
                DistinctType.newBuilder(
                                ObjectIdentifier.of("cat", "db", "distinctType"), new IntType())
                        .build());
        types.put(
                LogicalTypeRoot.STRUCTURED_TYPE,
                StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "structuredType"))
                        .build());
        types.put(LogicalTypeRoot.NULL, new NullType());
        types.put(
                LogicalTypeRoot.RAW,
                new RawType<>(
                        DayOfWeek.class,
                        new KryoSerializer<>(DayOfWeek.class, new SerializerConfigImpl())));
        types.put(LogicalTypeRoot.SYMBOL, new SymbolType<>());
        types.put(
                LogicalTypeRoot.UNRESOLVED,
                new UnresolvedUserDefinedType(UnresolvedIdentifier.of("unresolvedType")));
        types.put(LogicalTypeRoot.DESCRIPTOR, new DescriptorType());
        types.put(LogicalTypeRoot.VARIANT, new VariantType());
        types.put(LogicalTypeRoot.BITMAP, new BitmapType());
        return types;
    }

    @Test
    void testEveryLogicalTypeRootIsAudited() {
        assertThat(logicalTypeRootRepresentatives().keySet())
                .as(
                        "A new LogicalTypeRoot was added. Add a representative instance so that "
                                + "testOnlyGenericRawTypesMapToSqlTypeNameAny keeps auditing the "
                                + "complete type surface.")
                .containsExactlyInAnyOrder(LogicalTypeRoot.values());
    }

    @Test
    void testOnlyGenericRawTypesMapToSqlTypeNameAny() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        // The RAW guard in leastRestrictive keys off GenericRelDataType rather than off
        // SqlTypeName.ANY. That is only equivalent for Flink-produced types as long as
        // GenericRelDataType stays the sole Flink type that maps to ANY, so pin it here: every
        // logical type that Flink can hand to Calcite is converted and checked. If a new type ever
        // maps to ANY, this test fails and the guard must be revisited.
        final List<LogicalTypeRoot> rootsMappingToAny = new ArrayList<>();
        logicalTypeRootRepresentatives()
                .forEach(
                        (root, logicalType) -> {
                            final RelDataType relDataType;
                            try {
                                relDataType =
                                        typeFactory.createFieldTypeFromLogicalType(logicalType);
                            } catch (Throwable t) {
                                // The type cannot enter Calcite at all, so it cannot contribute an
                                // ANY to leastRestrictive either.
                                return;
                            }
                            if (relDataType.getSqlTypeName() == SqlTypeName.ANY) {
                                rootsMappingToAny.add(root);
                                assertThat(relDataType)
                                        .as("Unexpected ANY producer for root %s", root)
                                        .isInstanceOf(GenericRelDataType.class);
                            }
                        });

        // No non-legacy type maps to ANY today: the modern RawType maps to RawRelDataType, which
        // uses SqlTypeName.OTHER.
        assertThat(rootsMappingToAny).isEmpty();

        // The single Flink producer of ANY is the legacy TypeInformationRawType, and it is always a
        // GenericRelDataType.
        final RelDataType legacyRaw =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType<>(Types.GENERIC(TestClass.class)));
        assertThat(legacyRaw.getSqlTypeName()).isEqualTo(SqlTypeName.ANY);
        assertThat(legacyRaw).isInstanceOf(GenericRelDataType.class);
    }

    @Test
    void testUnknownTypeIsNotSqlTypeNameAny() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        // The validator types a not-yet-inferred node -- in particular a dynamic parameter "?" --
        // with createUnknownType(). Flink overrides it to SqlTypeName.NULL, so such nodes never
        // reached the RAW guard before and are unaffected by narrowing it.
        assertThat(typeFactory.createUnknownType().getSqlTypeName()).isEqualTo(SqlTypeName.NULL);
    }

    public static class TestClass {
        public int f0;
        public String f1;
    }

    public static class TestClass2 {
        public int f0;
        public String f1;
    }
}
