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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Batch execution coverage for the built-in array higher-order functions, plus the cross-cutting
 * SQL semantics that are asserted once rather than per runtime mode. The {@link
 * HigherOrderFunctionsITCase} suite runs the same built-ins in streaming mode; the duplication
 * across modes is deliberate, since batch/stream equivalence is a requirement of FLINK-31207.
 *
 * <p>The class name states the runtime mode, but the file also hosts the areas below. They live
 * here because they share the fixtures at the bottom of the file ({@code higherOrderFunctionInput},
 * {@code groupedHigherOrderFunctionInput}, {@code collectSingle*}, {@code assertInvalidCall} and
 * friends). Grep for the method prefix to find an area:
 *
 * <ul>
 *   <li>{@code testArrayHigherOrderFunctions*} — the built-ins end to end, in batch.
 *   <li>{@code testInvalidArguments*}, {@code testLambdaOutsideOfHigherOrderFunctionIsRejected},
 *       {@code testDuplicateLambdaParameterNamesAreRejected} — validation and signature error
 *       messages.
 *   <li>{@code testArrayFilterOverEnclosingLambdaParameter}, {@code
 *       testCollectionConstructorsOverLambdaParameter}, {@code
 *       testMutableLambdaResultsAreNotAliased} — lambda parameter scoping and the no-aliasing rule
 *       for mutable results.
 *   <li>{@code testFunctionTypeRejected*} — the {@code FUNCTION} type cannot surface as a table
 *       column, in a resolved schema, or across a catalog round-trip.
 *   <li>{@code testAggregate*}, {@code testCaptureOfGroupKeyInLambdaBody} — aggregates in and
 *       around lambda bodies, and capture of a group key.
 *   <li>{@code testOverWindowInLambdaBody} — {@code OVER} windows in a lambda body.
 *   <li>{@code testSubQueryInLambdaBody}, {@code testOuterColumnCaptureFromLambdaInScalarSubquery}
 *       — sub-queries in a lambda body and capture across a sub-query boundary.
 *   <li>{@code testViewOver*} — the view / SQL-expansion round-trip.
 *   <li>{@code testNonDeterministicLambdaBody*} — per-call re-evaluation of a non-deterministic
 *       body; see also {@code HigherOrderFunctionDeterminismTest} for the planner-level analysis.
 *   <li>{@code testLargeLambdaBodyIsSplitIntoSeparateMethods} — generated-code splitting for a body
 *       that exceeds the method size limit.
 * </ul>
 *
 * <p>Only the first bullet duplicates {@link HigherOrderFunctionsITCase}; every other area is
 * mode-independent and has no streaming counterpart. Should {@link BuiltInFunctionTestBase} ever be
 * parameterized over the runtime mode, that method is what it would absorb — the rest of this file
 * would stay as it is.
 */
class HigherOrderFunctionsBatchITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @Test
    void testArrayHigherOrderFunctionsBatch() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("f1", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(new Integer[] {1, 2, 3}, new Integer[] {10, 20}));
        tEnv.createTemporaryView("t", input);

        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(f0, x -> x + 1),"
                                + " ARRAY_FILTER(f0, x -> x > 1),"
                                + " ARRAY_REDUCE(f0, 0, (acc, x) -> acc + x),"
                                + " ARRAY_ZIP_WITH(f0, f1, (x, y) -> x + COALESCE(y, 0)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {2, 3});
            assertThat(row.getField(2)).isEqualTo(6);
            assertThat(row.getField(3)).isEqualTo(new Integer[] {11, 22, 3});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testInvalidArgumentsAreRejectedWithTheFunctionSignature() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // An argument of the wrong type, a lambda with the wrong number of parameters, or a regular
        // expression where a lambda is expected are all reported with the call's argument types and
        // the function's supported signature. Bridging a function does not change which of the two
        // reporting styles applies. When no lambda parameter type can be derived, the lambda is not
        // necessarily at fault -- here the first argument is not an ARRAY -- so the call is
        // reported
        // by Calcite with its signature, exactly as for a hand-written operator; only the supported
        // form is now generated from the type inference rather than hand-written. When the lambda
        // itself is rejected, the input type strategy reports it, which names the actual cause.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(base, x -> x + 1) FROM t",
                "Cannot apply 'ARRAY_TRANSFORM' to arguments of type "
                        + "'ARRAY_TRANSFORM(<INTEGER>, <FUNCTION(ANY) -> ANY>)'. "
                        + "Supported form(s): "
                        + "ARRAY_TRANSFORM(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->ANY)");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, base) FROM t",
                "Invalid input arguments. Expected signatures are:",
                "ARRAY_TRANSFORM(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->ANY)");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, (x, y) -> x) FROM t",
                "The lambda expression at position 1 expects 1 parameter(s) but 2 were provided.");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_FILTER(base, x -> x > 1) FROM t",
                "Cannot apply 'ARRAY_FILTER' to arguments of type "
                        + "'ARRAY_FILTER(<INTEGER>, <FUNCTION(ANY) -> BOOLEAN>)'. "
                        + "Supported form(s): "
                        + "ARRAY_FILTER(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->BOOLEAN)");
        // the predicate of ARRAY_FILTER must be BOOLEAN
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_FILTER(a, x -> x + 1) FROM t",
                "Invalid function call:\nARRAY_FILTER(ARRAY<INT>, FUNCTION(1))",
                "ARRAY_FILTER(array ARRAY, lambda FUNCTION(ARRAY_ELEMENT_TYPE)->BOOLEAN)",
                "The lambda expression at position 1 must return BOOLEAN, "
                        + "but its body returns INT.");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_REDUCE(a, 0, 5) FROM t",
                "Invalid function call:\nARRAY_REDUCE(ARRAY<INT>, INT NOT NULL, INT NOT NULL)",
                "ARRAY_REDUCE(array ARRAY, initial INIT, "
                        + "lambda FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE)");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_ZIP_WITH(a, base, (x, y) -> x) FROM t",
                "Cannot apply 'ARRAY_ZIP_WITH' to arguments of type "
                        + "'ARRAY_ZIP_WITH(<INTEGER ARRAY>, <INTEGER>, <FUNCTION(ANY, ANY) -> ANY>)'. "
                        + "Supported form(s): ARRAY_ZIP_WITH(array1 ARRAY, array2 ARRAY, "
                        + "lambda FUNCTION(ARRAY1_ELEMENT_TYPE, ARRAY2_ELEMENT_TYPE)->ANY)");
        // a reducer whose body is not assignable to the accumulator type is rejected during
        // validation (rather than failing later during code generation)
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * 0.5) FROM t",
                "Invalid function call:\nARRAY_REDUCE(ARRAY<INT>, INT NOT NULL, FUNCTION(2))",
                "ARRAY_REDUCE(array ARRAY, initial INIT, "
                        + "lambda FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE)",
                "The reducer of ARRAY_REDUCE must return a type assignable to the accumulator "
                        + "type INT NOT NULL, but its body returns DECIMAL(14, 1).");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> CAST(acc AS STRING)) FROM t",
                "Invalid function call:\nARRAY_REDUCE(ARRAY<INT>, INT NOT NULL, FUNCTION(2))",
                "ARRAY_REDUCE(array ARRAY, initial INIT, "
                        + "lambda FUNCTION(INIT_TYPE, ARRAY_ELEMENT_TYPE)->INIT_TYPE)",
                "The reducer of ARRAY_REDUCE must return a type assignable to the accumulator "
                        + "type INT NOT NULL, but its body returns STRING NOT NULL.");
    }

    @Test
    void testArrayFilterOverEnclosingLambdaParameter() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Table input =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))),
                        Row.of((Object) new Integer[][] {{1, 2, 3}, {4}}));
        tEnv.createTemporaryView("t", input);

        // The inner array is an unbound lambda parameter while ARRAY_FILTER is first checked, so
        // its operand check must be deferred to the pass in which the parameter is bound.
        final Table result =
                tEnv.sqlQuery("SELECT ARRAY_TRANSFORM(a, x -> ARRAY_FILTER(x, y -> y > 2)) FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            assertThat(iterator.next().getField(0)).isEqualTo(new Integer[][] {{3}, {4}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testCollectionConstructorsOverLambdaParameter() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda body is type-derived before the enclosing call binds the parameter, so an
        // ARRAY[...]/MAP[...] over the parameter cannot know its component type yet and must defer
        // instead of failing with "Type is not supported: ANY".
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(a, x -> ARRAY[x][1]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[x, base][2]),"
                                + " ARRAY_TRANSFORM(a, x -> CARDINALITY(ARRAY[x])),"
                                + " ARRAY_TRANSFORM(a, x -> MAP['k', x]['k']),"
                                + " ARRAY_TRANSFORM(a, x -> MAP[x, base][1]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[ARRAY[x]][1][1]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(ARRAY[x], y -> y + 1)[1]),"
                                + " ARRAY_REDUCE(a, 0, (acc, x) -> ARRAY[acc, x][2]),"
                                + " ARRAY_TRANSFORM(a, x -> CAST(ROW(x, base) AS ROW<c1 INT, c2 INT>).c1)"
                                + " FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {10, 10, 10});
            assertThat(row.getField(2)).isEqualTo(new Integer[] {1, 1, 1});
            assertThat(row.getField(3)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(row.getField(4)).isEqualTo(new Integer[] {10, null, null});
            assertThat(row.getField(5)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(row.getField(6)).isEqualTo(new Integer[] {2, 3, 4});
            assertThat(row.getField(7)).isEqualTo(3);
            assertThat(row.getField(8)).isEqualTo(new Integer[] {1, 2, 3});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testMutableLambdaResultsAreNotAliased() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A body result of a mutable type may be backed by an object that the generated code reuses
        // across iterations (e.g. the shared BinaryArrayData behind ARRAY[...]), so it must be
        // copied before it is buffered -- otherwise every entry ends up holding the last value.
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_TRANSFORM(a, x -> ARRAY[x, base]),"
                                + " ARRAY_TRANSFORM(a, x -> ROW(x, base)),"
                                + " ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y])"
                                + " FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 10}, {2, 10}, {3, 10}});
            assertThat(row.getField(1))
                    .isEqualTo(new Row[] {Row.of(1, 10), Row.of(2, 10), Row.of(3, 10)});
            assertThat(row.getField(2)).isEqualTo(new Integer[][] {{1, 1}, {2, 2}, {3, 3}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdasThatDifferOnlyInTheirCapturesAreNotMerged() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // Calcite compares two lambdas by their parameter indices and types only -- a parameter
        // name is not part of RexLambdaRef#equals -- so RexProgramBuilder merges lambdas that agree
        // on those. A lifted capture parameter is therefore numbered apart from a lambda's own
        // parameters: without that, the two-parameter lambda below and the one-parameter lambda
        // that captures `base` would both read (#0, #1) and be merged into one node, after which
        // the number of trailing capture operands no longer matches the call that carries it.
        final Table result =
                tEnv.sqlQuery(
                        "SELECT ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[x, base]),"
                                + " ARRAY_TRANSFORM(a, x -> ARRAY[x, x]),"
                                + " ARRAY_ZIP_WITH(a, a, (x, y) -> ARRAY[x, y + base])"
                                + " FROM t");

        try (final CloseableIterator<Row> iterator = result.execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 1}, {2, 2}, {3, 3}});
            assertThat(row.getField(1)).isEqualTo(new Integer[][] {{1, 10}, {2, 10}, {3, 10}});
            assertThat(row.getField(2)).isEqualTo(new Integer[][] {{1, 1}, {2, 2}, {3, 3}});
            assertThat(row.getField(3)).isEqualTo(new Integer[][] {{1, 11}, {2, 12}, {3, 13}});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testLambdaOutsideOfHigherOrderFunctionIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // SQL rejects a lambda outside of a function call already at the parser
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT x -> x + 1 FROM t"))
                .hasMessageContaining("SQL parse failed. Encountered \"->\"");
    }

    @Test
    void testFunctionTypeRejectedAsColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        // A FUNCTION type is a lambda helper type and must not be usable as a materialized column.
        assertThatThrownBy(
                        () ->
                                tEnv.fromValues(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "f", DataTypes.FUNCTION(1))),
                                                Row.of((Object) null))
                                        .execute()
                                        .collect())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("FUNCTION data type is a helper type");
    }

    @Test
    void testFunctionTypeRejectedAsSchemaColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A schema is what a catalog persists, so the type has to be rejected while the schema is
        // resolved -- otherwise a FUNCTION column enters the catalog and only fails much later,
        // when the table is read or written.
        assertThatThrownBy(
                        () ->
                                tEnv.createTable(
                                        "t_function_column",
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column("f", DataTypes.FUNCTION(1))
                                                                .build())
                                                .build()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for column 'f'. The FUNCTION data type "
                                + "is a helper type for lambda arguments of higher-order "
                                + "functions.");

        // nested in a constructed type as well
        assertThatThrownBy(
                        () ->
                                tEnv.createTable(
                                        "t_nested_function_column",
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column(
                                                                        "f",
                                                                        DataTypes.ARRAY(
                                                                                DataTypes.ROW(
                                                                                        DataTypes
                                                                                                .FIELD(
                                                                                                        "g",
                                                                                                        DataTypes
                                                                                                                .FUNCTION(
                                                                                                                        2)))))
                                                                .build())
                                                .build()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'ARRAY<ROW<`g` FUNCTION(2)>>' for column 'f'.");

        // DDL has no syntax for the type at all
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE t_ddl (f FUNCTION(1))"
                                                + " WITH ('connector' = 'datagen')"))
                .hasMessageContaining("Incorrect syntax near the keyword 'FUNCTION'");
    }

    @Test
    void testFunctionTypeRejectedAsMetadataColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A metadata column is a separate column kind in the schema resolver, so the
        // physical-column
        // case above does not cover it. It reaches the same rejection, but only because the
        // resolver
        // checks every column kind; pin it so that a future resolver refactor cannot let a FUNCTION
        // metadata column through.
        assertThatThrownBy(
                        () ->
                                tEnv.createTable(
                                        "t_function_metadata_column",
                                        TableDescriptor.forConnector("datagen")
                                                .schema(
                                                        Schema.newBuilder()
                                                                .columnByMetadata(
                                                                        "f", DataTypes.FUNCTION(1))
                                                                .build())
                                                .build()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid data type 'FUNCTION(1)' for column 'f'. The FUNCTION data type "
                                + "is a helper type for lambda arguments of higher-order "
                                + "functions.");
    }

    @Test
    void testFunctionTypeRejectedAsComputedColumn() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A computed column derives its type from an expression rather than from a declaration, so
        // it is the one column kind where a FUNCTION type could appear without anyone writing it
        // down. This pins that the computed-column path is not a back door: the grammar only
        // admits a lambda in the argument position of a call, so a bare lambda is rejected by the
        // parser long before it could become a column type.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE t_function_computed_column ("
                                                + " i INT,"
                                                + " f AS (x -> x + 1)"
                                                + ") WITH ('connector' = 'datagen')"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageContaining("Encountered \"->\"");
    }

    @Test
    void testFunctionTypeRejectedOnCatalogRoundTrip() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        final Catalog catalog =
                tEnv.getCatalog(tEnv.getCurrentCatalog()).orElseThrow(IllegalStateException::new);
        final ObjectPath path = new ObjectPath(tEnv.getCurrentDatabase(), "t_planted");

        // A catalog that already holds a FUNCTION column -- planted here past the catalog manager,
        // as an external catalog or an older Flink version could -- must not hand it out either:
        // the type is rejected again when the stored schema is resolved on read.
        catalog.createTable(
                path,
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("f", DataTypes.FUNCTION(1)).build())
                        .options(Collections.singletonMap("connector", "datagen"))
                        .build(),
                false);

        assertThatThrownBy(() -> tEnv.from("t_planted"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid data type 'FUNCTION(1)' for column 'f'.");

        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM t_planted"))
                .hasMessageContaining("Invalid data type 'FUNCTION(1)' for column 'f'.");

        // and neither can it be copied into a new table
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "CREATE TABLE t_ctas WITH ('connector' = 'blackhole')"
                                                + " AS SELECT * FROM t_planted"))
                .hasMessageContaining("Invalid data type 'FUNCTION(1)' for column 'f'.");
    }

    @Test
    void testDuplicateLambdaParameterNamesAreRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda binds its parameters by name, so two parameters of the same name would leave
        // the first one unreferenceable. Rejected rather than silently collapsed.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_ZIP_WITH(a, a, (x, x) -> x) FROM t",
                "Duplicate lambda parameter name 'x'. The parameters of a lambda expression "
                        + "must have unique names.");
        // a nested lambda may shadow an enclosing parameter, but not repeat its own
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> ARRAY_ZIP_WITH(a, a, (x, x) -> x)[1]) FROM t",
                "Duplicate lambda parameter name 'x'.");
    }

    @Test
    void testAggregateOverLambdaParameterIsRejected() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda parameter exists per element, so there is no group to aggregate it over --
        // ARRAY_REDUCE is the fold for that. Rejected during validation, because deriving the type
        // of the aggregate over the not-yet-bound parameter would otherwise fail with an internal
        // error. An aggregate over a column of the enclosing query is a different matter and is
        // supported; see testAggregateOverOuterColumnInLambdaBody.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> SUM(x)) FROM t",
                "Aggregate functions over a lambda parameter are not supported in the body of a "
                        + "lambda expression. 'x' is a lambda parameter");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(x + base)) FROM t GROUP BY a",
                "Aggregate functions over a lambda parameter are not supported");
        // the parameter of an *enclosing* lambda counts as well
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, e -> ARRAY_REDUCE(a, 0, (acc, x) -> acc + SUM(e)))"
                        + " FROM t GROUP BY a",
                "Aggregate functions over a lambda parameter are not supported in the body of a "
                        + "lambda expression. 'e' is a lambda parameter");

        // but the parameter of a lambda nested *inside* the aggregate is bound within it, so this
        // aggregates over the enclosing query only and is accepted
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(ARRAY_REDUCE(a, 0,"
                                        + " (acc, y) -> acc + y))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {7, 8, 9});
    }

    @Test
    void testAggregateInLambdaBodyWithoutGroupingIsRejected() {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // Hoisting the aggregate out of the lambda body does not exempt the column the body is
        // applied to from the grouping rules: without a GROUP BY the aggregate is global, so `a` is
        // not available to the projection. A lambda changes nothing here -- the same query without
        // one is rejected identically.
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t",
                "Expression 'a' is not being grouped");
        assertInvalidCall(
                tEnv, "SELECT a, SUM(base) FROM t", "Expression 'a' is not being grouped");
    }

    @Test
    void testAggregateOverOuterColumnInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // An aggregate over a column of the enclosing query is evaluated once per group; the body
        // sees its result, exactly as if it had been computed in a sub-query. The conversion turns
        // it into a reference into the Aggregate output, which capture lifting then hoists out of
        // the lambda (see SqlToRelConverter#convertLambda and HigherOrderFunctionUtil).
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {31, 32, 33});
        // grouping by another column too, in both orders -- the group key order must not matter
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t"
                                        + " GROUP BY a, g"))
                .isEqualTo(new Integer[] {31, 32, 33});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t"
                                        + " GROUP BY g, a"))
                .isEqualTo(new Integer[] {31, 32, 33});
        // COUNT(*), which has no argument to resolve at all
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + CAST(COUNT(*) AS INT)) FROM t"
                                        + " GROUP BY a"))
                .isEqualTo(new Integer[] {3, 4, 5});
        // an aggregate and an ordinary column capture in the same body
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) + CHAR_LENGTH(g))"
                                        + " FROM t GROUP BY a, g"))
                .isEqualTo(new Integer[] {34, 35, 36});
        // the other array higher-order functions host it just as well
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_FILTER(a, x -> x > SUM(base) - 30) FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {1, 2, 3});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x + SUM(base)) FROM t"
                                        + " GROUP BY a"))
                .isEqualTo(96);
    }

    @Test
    void testAggregateOverOuterColumnInNestedLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "a", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
                                DataTypes.FIELD("base", DataTypes.INT())),
                        Row.of(new Integer[][] {{1, 2}, {3}}, 10)));

        // the aggregate is threaded out of the inner lambda and then out of the outer one
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, b -> ARRAY_TRANSFORM(b,"
                                        + " x -> x + SUM(base))) FROM t GROUP BY a"))
                .isEqualTo(new Integer[][] {{11, 12}, {13}});
    }

    @Test
    void testCaptureOfGroupKeyInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // A captured group key must be resolved against the Aggregate output, not against the
        // pre-aggregation input: the two agree only when the GROUP BY order happens to match the
        // input order, so both orders are pinned here.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + CHAR_LENGTH(g)) FROM t"
                                        + " GROUP BY a, g"))
                .isEqualTo(new Integer[] {4, 5, 6});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + CHAR_LENGTH(g)) FROM t"
                                        + " GROUP BY g, a"))
                .isEqualTo(new Integer[] {4, 5, 6});
        // a nested lambda that captures both a group key and the enclosing lambda's parameter
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, e -> ARRAY_REDUCE(a, 0,"
                                        + " (acc, x) -> acc + x + e + CHAR_LENGTH(g))) FROM t"
                                        + " GROUP BY g, a"))
                .isEqualTo(new Integer[] {18, 21, 24});
    }

    @Test
    void testOverWindowInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // An OVER window yields one value per row, so it is lifted out of the lambda and evaluated
        // in a projection below it -- the same plan as writing the window in a sub-query. Both
        // input rows see the same unbounded window, so both produce the same array.
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) OVER ()) FROM t"))
                .containsExactly(new Integer[] {31, 32, 33}, new Integer[] {31, 32, 33});
        assertThat(
                        collectColumn(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) OVER"
                                        + " (PARTITION BY g)) FROM t"))
                .containsExactly(new Integer[] {31, 32, 33}, new Integer[] {31, 32, 33});

        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base) OVER (PARTITION BY x)) FROM t",
                "OVER windows over a lambda parameter are not supported in the body of a lambda "
                        + "expression. 'x' is a lambda parameter");
    }

    @Test
    void testSubQueryInLambdaBody() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", groupedHigherOrderFunctionInput(tEnv));

        // a scalar sub-query is a value, so the body may use it -- plain or correlated
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(base) FROM t))"
                                        + " FROM t GROUP BY a"))
                .isEqualTo(new Integer[] {21, 22, 23});
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(base) FROM t t2"
                                        + " WHERE t2.g = t1.g)) FROM t t1 GROUP BY a, g"))
                .isEqualTo(new Integer[] {21, 22, 23});

        // but not one that correlates on a lambda parameter
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x + (SELECT MAX(base) FROM t t2"
                        + " WHERE t2.base = x)) FROM t",
                "Subqueries over a lambda parameter are not supported in the body of a lambda "
                        + "expression. 'x' is a lambda parameter");
        assertInvalidCall(
                tEnv,
                "SELECT ARRAY_TRANSFORM(a, x -> x IN (SELECT base FROM t)) FROM t",
                "Subqueries over a lambda parameter are not supported");
    }

    @Test
    void testOuterColumnCaptureFromLambdaInScalarSubquery() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        // A lambda body inside a scalar sub-query captures a column of the *outer* query. This
        // crosses a query boundary, so the reference keeps Calcite's correlated-reference path (a
        // RexCorrelVariable field access placed *inside* the lambda) rather than being flattened
        // into a direct input reference of the sub-query's row (see
        // SqlToRelConverter#isSameQueryLambdaCapture). Decorrelation descends into the lambda body,
        // rewrites the correlated reference to an input reference of the decorrelated row, and
        // capture lifting hoists it out of the lambda, so the query executes. x = 1, t.a = 2 -> 3.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x + t.a)[1])"
                                        + " FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(3);

        // Nested lambda inside the correlated scalar sub-query: the outer correlation is captured
        // by the inner lambda body. x = 1, y = 1, t.a = 2 -> 1 + 1 + 2 = 4.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1],"
                                        + " x -> ARRAY_TRANSFORM(ARRAY[1], y -> y + x + t.a)[1])[1])"
                                        + " FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(4);

        // A lambda body that mixes a same-query capture (the sub-query's own column s.b, lifted as
        // an ordinary capture) with an outer correlated value (t.a, decorrelated). x = 1, s.b = 10,
        // t.a = 2 -> 1 + 10 + 2 = 13.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x + s.b + t.a)[1]"
                                        + " FROM (VALUES (10)) AS s(b))"
                                        + " FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(13);

        // Two distinct correlated outer fields, both discovered and rewritten inside the lambda.
        // x = 1, t.a = 2, t.b = 5 -> 1 + 2 + 5 = 8.
        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT (SELECT ARRAY_TRANSFORM(ARRAY[1], x -> x + t.a + t.b)[1])"
                                        + " FROM (VALUES (2, 5)) AS t(a, b)"))
                .isEqualTo(8);

        // Control: a same-query outer-column capture (no query boundary crossed) is flattened to a
        // plain input reference, hoisted out of the lambda by capture lifting, and executes -- it
        // is
        // never turned into a correlation, so it does not exercise the decorrelation path above.
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));
        assertThat(collectSingleRow(tEnv, "SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM t"))
                .isEqualTo(new Integer[] {11, 12, 13});

        // Control: an equivalent correlated scalar sub-query *without* a lambda, guarding that the
        // decorrelator's behaviour is unchanged for the non-lambda case. t.a = 2 -> 3.
        assertThat(collectSingleRow(tEnv, "SELECT (SELECT t.a + 1) FROM (VALUES (2)) AS t(a)"))
                .isEqualTo(3);
    }

    @Test
    void testViewOverBuiltInHigherOrderFunctions() throws Exception {
        // A view does not persist its query as written: it persists the *expanded* query, i.e. the
        // validated statement unparsed back to SQL -- tables fully qualified, built-in function
        // names merely quoted -- and re-parses that string on every read. A lambda parameter is
        // bound by the enclosing higher-order call rather than by the query's row type, so the
        // expansion must leave it alone -- qualifying it would either fail to re-parse or silently
        // resolve to a column.
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        tEnv.executeSql(
                "CREATE VIEW v AS SELECT"
                        + " ARRAY_TRANSFORM(a, x -> x + base) AS c0,"
                        + " ARRAY_FILTER(a, x -> x > 1) AS c1,"
                        + " ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) AS c2,"
                        + " ARRAY_ZIP_WITH(a, a, (x, y) -> x + y) AS c3"
                        + " FROM t");

        assertThat(expandedQueryOf(tEnv, "v"))
                .isEqualTo(
                        "SELECT `ARRAY_TRANSFORM`(`t`.`a`, `x` -> `x` + `t`.`base`) AS `c0`,"
                                + " `ARRAY_FILTER`(`t`.`a`, `x` -> `x` > 1) AS `c1`,"
                                + " `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`) AS `c2`,"
                                + " `ARRAY_ZIP_WITH`(`t`.`a`, `t`.`a`, (`x`, `y`) -> `x` + `y`) AS `c3`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery("SELECT * FROM v").execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(row.getField(0)).isEqualTo(new Integer[] {11, 12, 13});
            assertThat(row.getField(1)).isEqualTo(new Integer[] {2, 3});
            assertThat(row.getField(2)).isEqualTo(6);
            assertThat(row.getField(3)).isEqualTo(new Integer[] {2, 4, 6});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testViewOverShadowingAndNestedLambdas() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A lambda parameter named like a column of the source table. The expansion must not
        // qualify it, otherwise the re-parsed view would read the column instead of the parameter
        // and silently return {1000, 1000, 1000} (a = {1, 2, 3}, base = 10).
        tEnv.executeSql(
                "CREATE VIEW v_shadow AS SELECT base, ARRAY_TRANSFORM(a, base -> base * 100) AS c"
                        + " FROM t");
        assertThat(expandedQueryOf(tEnv, "v_shadow"))
                .isEqualTo(
                        "SELECT `t`.`base`, `ARRAY_TRANSFORM`(`t`.`a`, `base` -> `base` * 100) AS `c`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");
        assertThat(collectColumn(tEnv, "SELECT c FROM v_shadow"))
                .containsExactly((Object) new Integer[] {100, 200, 300});

        // Nested lambdas whose parameters shadow each other, mixed with a capture of an enclosing
        // parameter and of an outer column. Only the outer column may be qualified.
        tEnv.executeSql(
                "CREATE VIEW v_nested AS SELECT"
                        + " ARRAY_TRANSFORM(a, x -> ARRAY_REDUCE(a, x, (acc, x) -> acc + x)) AS c0,"
                        + " ARRAY_TRANSFORM(a, x -> ARRAY_REDUCE(a, 0, (acc, y) -> acc + y + x + base)) AS c1"
                        + " FROM t");
        assertThat(expandedQueryOf(tEnv, "v_nested"))
                .isEqualTo(
                        "SELECT `ARRAY_TRANSFORM`(`t`.`a`, `x` -> `ARRAY_REDUCE`(`t`.`a`, `x`,"
                                + " (`acc`, `x`) -> `acc` + `x`)) AS `c0`,"
                                + " `ARRAY_TRANSFORM`(`t`.`a`, `x` -> `ARRAY_REDUCE`(`t`.`a`, 0,"
                                + " (`acc`, `y`) -> `acc` + `y` + `x` + `t`.`base`)) AS `c1`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`");

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery("SELECT * FROM v_nested").execute().collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            // the inner reduce starts at x and folds {1, 2, 3} over the shadowing parameter
            assertThat(row.getField(0)).isEqualTo(new Integer[] {7, 8, 9});
            // sum(a) + 3 * (x + base) = 6 + 3 * (x + 10)
            assertThat(row.getField(1)).isEqualTo(new Integer[] {39, 42, 45});
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void testViewOverHigherOrderFunctionsOutsideOfAProjection() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        // A higher-order call in a filter and as a grouping key, plus a star expansion next to one.
        tEnv.executeSql(
                "CREATE VIEW v AS SELECT *, ARRAY_FILTER(a, x -> x > base - 9) AS c FROM t"
                        + " WHERE ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) > 1");
        assertThat(expandedQueryOf(tEnv, "v"))
                .isEqualTo(
                        "SELECT `t`.`a`, `t`.`m`, `t`.`base`,"
                                + " `ARRAY_FILTER`(`t`.`a`, `x` -> `x` > `t`.`base` - 9) AS `c`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`"
                                + " WHERE `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`) > 1");
        assertThat(collectColumn(tEnv, "SELECT c FROM v"))
                .containsExactly((Object) new Integer[] {2, 3});

        // A view stacked on a view: the outer lambda works on a column that is itself the result of
        // a lambda, so both expansions have to hold.
        tEnv.executeSql("CREATE VIEW v2 AS SELECT ARRAY_TRANSFORM(c, x -> x * 10) AS c FROM v");
        assertThat(expandedQueryOf(tEnv, "v2"))
                .isEqualTo(
                        "SELECT `ARRAY_TRANSFORM`(`v`.`c`, `x` -> `x` * 10) AS `c`"
                                + " FROM `default_catalog`.`default_database`.`v` AS `v`");
        assertThat(collectColumn(tEnv, "SELECT c FROM v2"))
                .containsExactly((Object) new Integer[] {20, 30});

        tEnv.executeSql(
                "CREATE VIEW v_group AS SELECT ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) AS s,"
                        + " COUNT(*) AS n FROM t GROUP BY ARRAY_REDUCE(a, 0, (acc, x) -> acc + x)");
        assertThat(expandedQueryOf(tEnv, "v_group"))
                .isEqualTo(
                        "SELECT `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`) AS `s`,"
                                + " COUNT(*) AS `n`"
                                + " FROM `default_catalog`.`default_database`.`t` AS `t`"
                                + " GROUP BY `ARRAY_REDUCE`(`t`.`a`, 0, (`acc`, `x`) -> `acc` + `x`)");
        assertThat(collectColumn(tEnv, "SELECT s FROM v_group")).containsExactly(6);
    }

    /**
     * Two textually identical higher-order calls whose lambda body is non-deterministic collapse
     * into a single {@code RexProgram} expression referenced twice, so code generation must not
     * reuse the generated value for the second reference. The counting function returns a fresh
     * value per invocation, hence six distinct values iff both calls are evaluated independently.
     */
    @Test
    void testNonDeterministicLambdaBodyIsEvaluatedPerCall() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        CountingScalarFunction.COUNTER.set(0);
        tEnv.createTemporarySystemFunction("cnt", CountingScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        try (final CloseableIterator<Row> iterator =
                tEnv.sqlQuery(
                                "SELECT ARRAY_TRANSFORM(a, x -> cnt(x)),"
                                        + " ARRAY_TRANSFORM(a, x -> cnt(x)),"
                                        + " ARRAY_TRANSFORM(a, y -> cnt(y)) FROM t")
                        .execute()
                        .collect()) {
            assertThat(iterator).hasNext();
            final Row row = iterator.next();
            assertThat(iterator).isExhausted();
            assertThat(
                            Stream.of(row.getField(0), row.getField(1), row.getField(2))
                                    .flatMap(array -> Arrays.stream((Integer[]) array)))
                    .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9);
        }
    }

    /**
     * A non-deterministic call in a lambda body must not be pushed below the aggregation that a
     * filter on the higher-order call sits above, and must be re-evaluated for the projection: the
     * function is called once per element per surviving evaluation.
     */
    @Test
    void testNonDeterministicLambdaBodyInFilterAndProjection() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        CountingScalarFunction.COUNTER.set(0);
        tEnv.createTemporarySystemFunction("cnt", CountingScalarFunction.class);
        tEnv.createTemporaryView("t", higherOrderFunctionInput(tEnv));

        final List<Object> values =
                collectColumn(
                        tEnv,
                        "SELECT ARRAY_TRANSFORM(a, x -> cnt(x)) FROM t"
                                + " WHERE ARRAY_TRANSFORM(a, x -> cnt(x))[1] > 0");
        assertThat(values).hasSize(1);
        assertThat((Integer[]) values.get(0)).doesNotContain(1, 2, 3);
        assertThat(CountingScalarFunction.COUNTER).hasValue(6);
    }

    /**
     * A lambda body is compiled into a generated class of its own, but that class is subject to the
     * JVM's 64 KB per-method limit like any other, and machine-generated SQL can drive a body past
     * it. Such a body must therefore be split into separate methods by {@code
     * flink-table-code-splitter} exactly like the same expression written outside a lambda,
     * otherwise it fails to compile with a misleading "this is a bug" error. The bodies below are
     * far above {@link TableConfigOptions#MAX_LENGTH_GENERATED_CODE}.
     */
    @Test
    void testLargeLambdaBodyIsSplitIntoSeparateMethods() throws Exception {
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("a1", DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.FIELD("a2", DataTypes.ARRAY(DataTypes.INT()))),
                        Row.of(new Integer[] {1, 2}, new Integer[] {10, 20})));

        // The same expression outside of a lambda compiles, so the lambda must not regress.
        assertThat(collectSingleRow(tEnv, "SELECT " + longExpression("1") + " FROM t"))
                .isEqualTo(longExpressionResult(1));

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_TRANSFORM(a1, x -> "
                                        + longExpression("x")
                                        + ") FROM t"))
                .isEqualTo(new Integer[] {longExpressionResult(1), longExpressionResult(2)});

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_FILTER(a1, x -> "
                                        + longExpression("x")
                                        + " > 0) FROM t"))
                .isEqualTo(new Integer[] {1, 2});

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_REDUCE(a1, 0, (acc, x) -> acc + "
                                        + longExpression("x")
                                        + ") FROM t"))
                .isEqualTo(longExpressionResult(1) + longExpressionResult(2));

        assertThat(
                        collectSingleRow(
                                tEnv,
                                "SELECT ARRAY_ZIP_WITH(a1, a2, (x, y) -> y + "
                                        + longExpression("x")
                                        + ") FROM t"))
                .isEqualTo(
                        new Integer[] {10 + longExpressionResult(1), 20 + longExpressionResult(2)});
    }

    /** The number of terms in {@link #longExpression(String)}. */
    private static final int LONG_EXPRESSION_TERMS = 500;

    /**
     * An {@code INT} expression over {@code term} that generates well beyond 64 KB of Java code:
     * the casts are neither constant-folded nor shared, so every term contributes its own code.
     *
     * <p>The summands are combined into a balanced tree rather than the left-deep chain that {@code
     * a + b + c + ...} parses into, because code generation recurses once per nesting level of the
     * expression: a chain this long exhausts the JVM's default 1 MB thread stack before the
     * generated code is ever split. The nesting depth is not what this test is about, and testing
     * it would only pin down the recursion limit of the code generator.
     */
    private static String longExpression(String term) {
        List<String> summands = new ArrayList<>();
        summands.add(term);
        for (int i = 0; i < LONG_EXPRESSION_TERMS; i++) {
            summands.add("CAST(CAST(" + term + " + " + i + " AS VARCHAR) AS INT)");
        }
        while (summands.size() > 1) {
            final List<String> pairs = new ArrayList<>();
            for (int i = 0; i < summands.size(); i += 2) {
                if (i + 1 < summands.size()) {
                    pairs.add("(" + summands.get(i) + " + " + summands.get(i + 1) + ")");
                } else {
                    pairs.add(summands.get(i));
                }
            }
            summands = pairs;
        }
        return summands.get(0);
    }

    /** The value of {@link #longExpression(String)} for the given term value. */
    private static int longExpressionResult(int term) {
        int result = term;
        for (int i = 0; i < LONG_EXPRESSION_TERMS; i++) {
            result += term + i;
        }
        return result;
    }

    // --------------------------------------------------------------------------------------------

    /** A non-deterministic function returning a fresh value on every invocation. */
    public static class CountingScalarFunction extends ScalarFunction {

        static final AtomicInteger COUNTER = new AtomicInteger();

        public Integer eval(Integer i) {
            return COUNTER.incrementAndGet();
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }

    /**
     * The query a view was persisted with, i.e. the original statement with all identifiers
     * expanded, as a single line.
     */
    private static String expandedQueryOf(TableEnvironment tEnv, String viewName) throws Exception {
        final CatalogView view =
                (CatalogView)
                        tEnv.getCatalog(tEnv.getCurrentCatalog())
                                .orElseThrow(IllegalStateException::new)
                                .getTable(new ObjectPath(tEnv.getCurrentDatabase(), viewName));
        return view.getExpandedQuery().replace('\n', ' ');
    }

    /** A single row with an array, a map, and a scalar column for negative tests. */
    private static Table higherOrderFunctionInput(TableEnvironment tEnv) {
        final java.util.Map<String, Integer> m = new java.util.LinkedHashMap<>();
        m.put("a", 1);
        return tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD("base", DataTypes.INT())),
                Row.of(new Integer[] {1, 2, 3}, m, 10));
    }

    /**
     * Two rows sharing an array and a group column, with a scalar column that sums to 30 -- so that
     * an aggregate over the enclosing query is distinguishable from the value of a single row.
     */
    private static Table groupedHigherOrderFunctionInput(TableEnvironment tEnv) {
        return tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("g", DataTypes.STRING()),
                        DataTypes.FIELD("base", DataTypes.INT())),
                Row.of(new Integer[] {1, 2, 3}, "abc", 10),
                Row.of(new Integer[] {1, 2, 3}, "abc", 20));
    }

    private static Object collectSingleRow(TableEnvironment tEnv, String sql) throws Exception {
        try (final CloseableIterator<Row> iterator = tEnv.sqlQuery(sql).execute().collect()) {
            assertThat(iterator).hasNext();
            final Object value = iterator.next().getField(0);
            assertThat(iterator).isExhausted();
            return value;
        }
    }

    private static Object collectSingle(Table table) throws Exception {
        try (final CloseableIterator<Row> iterator = table.execute().collect()) {
            assertThat(iterator).hasNext();
            final Object value = iterator.next().getField(0);
            assertThat(iterator).isExhausted();
            return value;
        }
    }

    private static List<Object> collectColumn(TableEnvironment tEnv, String sql) throws Exception {
        return collectColumn(tEnv.sqlQuery(sql));
    }

    private static List<Object> collectColumn(Table table) throws Exception {
        try (final CloseableIterator<Row> iterator = table.execute().collect()) {
            final List<Object> values = new ArrayList<>();
            iterator.forEachRemaining(row -> values.add(row.getField(0)));
            return values;
        }
    }

    private static void assertInvalidCall(
            TableEnvironment tEnv, String sql, String... expectedMessageParts) {
        assertThatThrownBy(() -> tEnv.sqlQuery(sql))
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        e -> {
                            final String message = ExceptionUtils.stringifyException(e);
                            for (String part : expectedMessageParts) {
                                assertThat(message).contains(part);
                            }
                        });
    }
}
