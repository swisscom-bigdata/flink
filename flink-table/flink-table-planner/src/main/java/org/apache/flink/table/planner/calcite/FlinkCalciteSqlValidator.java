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

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.FlinkSqlParsingValidator;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.planner.catalog.CatalogSchemaModel;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.HigherOrderFunctionUtil;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.SystemTypeInference;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplicitModelCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlModelCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.ExtraCalciteResource;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.IdentifierSnapshotNamespace;
import org.apache.calcite.sql.validate.ParameterScope;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup.includeExpandedColumn;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Extends Calcite's {@link SqlValidator} by Flink-specific behavior. */
@Internal
public final class FlinkCalciteSqlValidator extends FlinkSqlParsingValidator {

    private static final ExtraCalciteResource EXTRA_RESOURCE =
            Resources.create(ExtraCalciteResource.class);

    /**
     * Kinds that make a call a sub-query construct in a lambda body although the call itself is not
     * of a {@link SqlKind#QUERY} kind (see {@code validateLambdaBody}).
     */
    private static final Set<SqlKind> QUERY_CONSTRUCTS_IN_LAMBDA_BODY =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            SqlKind.SCALAR_QUERY,
                            SqlKind.IN,
                            SqlKind.NOT_IN,
                            SqlKind.SOME,
                            SqlKind.ALL,
                            SqlKind.EXISTS,
                            SqlKind.UNIQUE));

    // Enables CallContext#getOutputDataType() when validating SQL expressions.
    private SqlNode sqlNodeForExpectedOutputType;
    private RelDataType expectedOutputType;

    private final RelOptCluster relOptCluster;

    private final RelOptTable.ToRelContext toRelContext;

    private final FrameworkConfig frameworkConfig;

    private final List<ColumnExpansionStrategy> columnExpansionStrategies;

    public FlinkCalciteSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlValidator.Config config,
            RelOptTable.ToRelContext toRelcontext,
            RelOptCluster relOptCluster,
            FrameworkConfig frameworkConfig) {
        super(
                opTab,
                catalogReader,
                typeFactory,
                config,
                ShortcutUtils.unwrapTableConfig(relOptCluster)
                        .get(TableConfigOptions.LEGACY_NESTED_ROW_NULLABILITY));
        this.relOptCluster = relOptCluster;
        this.toRelContext = toRelcontext;
        this.frameworkConfig = frameworkConfig;
        this.columnExpansionStrategies =
                ShortcutUtils.unwrapTableConfig(relOptCluster)
                        .get(TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY);
    }

    public RelOptCluster getRelOptCluster() {
        return relOptCluster;
    }

    public void setExpectedOutputType(SqlNode sqlNode, RelDataType expectedOutputType) {
        this.sqlNodeForExpectedOutputType = sqlNode;
        this.expectedOutputType = expectedOutputType;
    }

    public Optional<RelDataType> getExpectedOutputType(SqlNode sqlNode) {
        if (sqlNode == sqlNodeForExpectedOutputType) {
            return Optional.of(expectedOutputType);
        }
        return Optional.empty();
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() == DECIMAL) {
            final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
            if (decimal.precision() > DecimalType.MAX_PRECISION) {
                throw newValidationError(
                        literal, Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
            }
        }
        super.validateLiteral(literal);
    }

    @Override
    public void validateLambda(SqlLambda lambdaExpr) {
        // Reject lambda parameter names that begin with the prefix reserved for lifted capture
        // parameters (see HigherOrderFunctionUtil#CAPTURE_PARAM_PREFIX). The planner recognizes a
        // capture by its index, not its name, so this only keeps plans readable: a user-chosen
        // name such as "cap$x" would look like a lifted capture there. Duplicate names are
        // rejected for a different reason: the body binds parameters by name, and the lambda scope
        // keys them by name, so a duplicate would collapse into a single parameter and the derived
        // FUNCTION type would disagree with the declared arity (an internal error later on).
        final Set<String> declared = new HashSet<>();
        for (SqlNode parameter : lambdaExpr.getParameters()) {
            final String name = ((SqlIdentifier) parameter).getSimple();
            if (HigherOrderFunctionUtil.isCaptureName(name)) {
                throw newValidationError(
                        parameter,
                        EXTRA_RESOURCE.reservedLambdaParameterName(
                                name, HigherOrderFunctionUtil.CAPTURE_PARAM_PREFIX));
            }
            if (!declared.add(name)) {
                throw newValidationError(
                        parameter, EXTRA_RESOURCE.duplicateLambdaParameterName(name));
            }
        }

        // A lambda body is compiled into an expression that is evaluated per element. Aggregates,
        // OVER windows and sub-queries over the columns of the enclosing query are fine there --
        // they are evaluated once per row and the body sees their result -- but the same constructs
        // over a lambda *parameter* have no meaning: the parameter only exists per element, and
        // there is no group to aggregate it over (ARRAY_REDUCE is the fold for that). The checks
        // that reject them in an ordinary projection (validateExpr, the aggregate finders) do not
        // descend into a lambda body, so without this they would surface as an internal error much
        // later -- deriving the type of an aggregate over an unbound lambda parameter fails with
        // "Type is not supported: ANY". Table functions produce rows rather than a value and are
        // rejected outright; otherwise they reach code generation and fail with an unset collector.
        // Asynchronous scalar functions are rejected outright for the same reason: their result
        // only arrives after the per-element expression consuming it has been evaluated, and the
        // rule that extracts them out of a projection does not descend into lambda bodies either.
        // Python scalar functions are rejected outright too: they are evaluated by a separate
        // Python operator that a projection's Python calls are split out into, which cannot happen
        // per element inside a lambda body.
        // Both checks must run before super, which derives the body's type.
        final SqlLambdaScope scope = (SqlLambdaScope) getLambdaScope(lambdaExpr);
        validateLambdaBody(lambdaExpr.getExpression(), visibleLambdaParameters(scope));

        super.validateLambda(lambdaExpr);
    }

    /**
     * Registers the scopes of the lambdas in a stand-alone SQL expression -- a computed column, a
     * watermark, or {@code callSql} of the Table API -- before validating it. Calcite registers
     * scopes only when the expression is a query, so such a lambda would otherwise have no {@link
     * SqlLambdaScope} and its parameters would not resolve. The scope registration is private to
     * Calcite's {@code validateScopedExpression}, hence the scopes' parent is an equivalent {@link
     * ParameterScope}, which resolves a captured column from the same name-to-type map as the one
     * created by super. See {@link #validateCall} for the other half.
     */
    @Override
    public SqlNode validateParameterizedExpression(
            SqlNode topNode, Map<String, RelDataType> nameToTypeMap) {
        if (!topNode.isA(SqlKind.TOP_LEVEL)) {
            registerOutermostLambdas(new ParameterScope(this, nameToTypeMap), topNode);
        }
        return super.validateParameterizedExpression(topNode, nameToTypeMap);
    }

    private void registerOutermostLambdas(SqlValidatorScope scope, @Nullable SqlNode node) {
        if (node instanceof SqlLambda) {
            // Also registers the lambdas nested in the body, under this lambda's scope.
            registerQuery(scope, null, node, node, null, false);
        } else if (node instanceof SqlCall && !node.getKind().belongsTo(SqlKind.QUERY)) {
            ((SqlCall) node)
                    .getOperandList()
                    .forEach(operand -> registerOutermostLambdas(scope, operand));
        } else if (node instanceof SqlNodeList) {
            ((SqlNodeList) node).forEach(child -> registerOutermostLambdas(scope, child));
        }
    }

    /**
     * Validates the operands of an outermost lambda of a stand-alone SQL expression (see {@link
     * #validateParameterizedExpression}) in the lambda's own scope. The scope of such an expression
     * is a {@link ParameterScope}, whose {@code getOperandScope} returns itself for every call,
     * whereas the scopes of a query hand out the lambda's scope for a lambda (see {@code
     * DelegatingScope#getOperandScope}).
     */
    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        if (call instanceof SqlLambda && scope instanceof ParameterScope) {
            call.getOperator().validateCall(call, this, scope, getLambdaScope((SqlLambda) call));
            return;
        }
        super.validateCall(call, scope);
    }

    /**
     * Defers a field access in a lambda body whose record is derived from a lambda parameter that
     * is not bound yet, such as {@code o.items[1].price} in {@code ARRAY_TRANSFORM(a, x ->
     * ARRAY_TRANSFORM(orders, o -> o.items[1].price + x))}.
     *
     * <p>The body of a nested lambda, and of a lambda in a stand-alone SQL expression, is
     * type-derived once before the operand checker of its call binds the parameters (see {@link
     * SqlValidatorUtils#isUnresolvedLambdaParameter}). The record is then the unbound placeholder,
     * which Calcite's {@code DOT} operator rejects with "Incompatible types" because it has no
     * fields. The field access is the placeholder as well until the body is re-validated against
     * the bound parameter types. A field of a parameter itself, {@code o.items}, is an identifier
     * rather than a {@code DOT} call and is deferred by {@link FlinkSqlNameMatcher#field}.
     */
    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        if (expr.getKind() == SqlKind.DOT && isInLambdaBody(scope)) {
            final RelDataType recordType = super.deriveType(scope, ((SqlCall) expr).operand(0));
            if (SqlValidatorUtils.isUnresolvedLambdaParameter(recordType)) {
                setValidatedNodeType(expr, recordType);
                return recordType;
            }
        }
        return super.deriveType(scope, expr);
    }

    private static boolean isInLambdaBody(SqlValidatorScope scope) {
        for (SqlValidatorScope current = scope;
                current instanceof DelegatingScope;
                current = ((DelegatingScope) current).getParent()) {
            if (current instanceof SqlLambdaScope) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the names of every lambda parameter visible in the given lambda scope: its own
     * parameters plus those of the lambdas it is nested in, since a body may close over them.
     */
    private static Set<String> visibleLambdaParameters(SqlLambdaScope scope) {
        final Set<String> parameters = new HashSet<>();
        for (SqlValidatorScope current = scope;
                current instanceof DelegatingScope;
                current = ((DelegatingScope) current).getParent()) {
            if (current instanceof SqlLambdaScope) {
                parameters.addAll(((SqlLambdaScope) current).getParameterTypes().keySet());
            }
        }
        return parameters;
    }

    private static boolean isPythonScalarFunction(SqlOperator operator) {
        final FunctionDefinition definition =
                operator instanceof ScalarSqlFunction
                        ? ((ScalarSqlFunction) operator).scalarFunction()
                        : ShortcutUtils.unwrapFunctionDefinition(operator);
        return definition instanceof PythonFunction && definition.getKind() == FunctionKind.SCALAR;
    }

    /**
     * Rejects the constructs that a lambda body cannot contain, naming the offending node.
     *
     * <p>Nested lambdas are descended into -- each is validated in its own right, but reporting the
     * innermost offending node here keeps the error at the position the user wrote -- with their
     * parameters added to {@code lambdaParameters}, as they too cannot be aggregated over.
     * Sub-queries are not descended into: they are separate queries whose contents are validated on
     * their own terms.
     *
     * @param node a node of the lambda body
     * @param lambdaParameters names of the lambda parameters visible at {@code node}
     */
    private void validateLambdaBody(SqlNode node, Set<String> lambdaParameters) {
        if (node instanceof SqlLambda) {
            final SqlLambda lambda = (SqlLambda) node;
            final Set<String> nested = new HashSet<>(lambdaParameters);
            lambda.getParameters().forEach(parameter -> nested.add(parameter.toString()));
            validateLambdaBody(lambda.getExpression(), nested);
            return;
        }
        if (node instanceof SqlNodeList) {
            for (SqlNode child : (SqlNodeList) node) {
                if (child != null) {
                    validateLambdaBody(child, lambdaParameters);
                }
            }
            return;
        }
        if (!(node instanceof SqlCall)) {
            return;
        }
        final SqlCall call = (SqlCall) node;
        final SqlOperator operator = call.getOperator();
        if (operator instanceof SqlTableFunction) {
            throw newValidationError(
                    call, EXTRA_RESOURCE.unsupportedInLambdaBody("Table functions"));
        }
        if (ShortcutUtils.isFunctionKind(operator, FunctionKind.ASYNC_SCALAR)) {
            throw newValidationError(
                    call, EXTRA_RESOURCE.unsupportedInLambdaBody("Asynchronous scalar functions"));
        }
        if (isPythonScalarFunction(operator)) {
            throw newValidationError(
                    call, EXTRA_RESOURCE.unsupportedInLambdaBody("Python scalar functions"));
        }
        // A quantified comparison (IN, EXISTS, ...) counts as a sub-query construct even though the
        // operator itself is not of a QUERY kind: its left-hand side is a sibling of the query, so
        // "x IN (SELECT ...)" references the lambda parameter outside the query the walk below
        // would otherwise reach.
        final boolean isQuery =
                call instanceof SqlSelect
                        || call.getKind().belongsTo(SqlKind.QUERY)
                        || QUERY_CONSTRUCTS_IN_LAMBDA_BODY.contains(call.getKind());
        final String construct;
        if (call.getKind() == SqlKind.OVER) {
            construct = "OVER windows";
        } else if (operator.isAggregator()) {
            construct = "Aggregate functions";
        } else if (isQuery) {
            construct = "Subqueries";
        } else {
            construct = null;
        }
        if (construct != null) {
            final String captured = findLambdaParameterReference(call, lambdaParameters);
            if (captured != null) {
                // An unqualified name is not resolved against the subquery, so a subquery with a
                // column of a lambda parameter's name is rejected too; its message therefore
                // suggests qualifying the column or renaming the parameter.
                throw newValidationError(
                        call,
                        isQuery
                                ? EXTRA_RESOURCE.unsupportedSubqueryOverLambdaParameterInLambdaBody(
                                        captured)
                                : EXTRA_RESOURCE.unsupportedOverLambdaParameterInLambdaBody(
                                        construct, captured));
            }
        }
        if (isQuery) {
            return;
        }
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null) {
                validateLambdaBody(operand, lambdaParameters);
            }
        }
    }

    /**
     * Returns the name of the first lambda parameter referenced anywhere below {@code node}, or
     * null if it references none. An identifier references the parameter its first component names,
     * e.g. {@code r.amount} references {@code r}. A lambda nested below {@code node} shadows the
     * parameters it redeclares, and its own parameters do not count: they are bound within it.
     *
     * <p>The check is syntactic and must never miss a real reference, so inside a sub-query it
     * follows the sub-query's own declarations only where that is unambiguous:
     *
     * <ul>
     *   <li>A name that is only declared is not a reference: the alias and the column list of
     *       {@code AS}, the name and the column list of a {@code WITH} item, and a table name in
     *       {@code FROM}.
     *   <li>A qualified name {@code q.c} is not a reference if {@code q} is a table alias of an
     *       enclosing {@code SELECT} within {@code node}. The validator and the converter resolve
     *       it innermost scope first, i.e. against that table, not against the lambda parameter.
     *   <li>Any other name whose first component is a parameter is a reference. That includes an
     *       unqualified name that is also a column of the sub-query: inside a lambda body such a
     *       name always means the parameter, which spares the reader from resolving it by scope.
     * </ul>
     */
    private static @Nullable String findLambdaParameterReference(
            SqlNode node, Set<String> lambdaParameters) {
        return findLambdaParameterReference(node, lambdaParameters, Collections.emptySet());
    }

    private static @Nullable String findLambdaParameterReference(
            SqlNode node, Set<String> lambdaParameters, Set<String> tableAliases) {
        if (lambdaParameters.isEmpty()) {
            return null;
        }
        if (node instanceof SqlIdentifier) {
            final SqlIdentifier identifier = (SqlIdentifier) node;
            final String name = identifier.names.get(0);
            if (!lambdaParameters.contains(name)
                    || (!identifier.isSimple() && tableAliases.contains(name))) {
                return null;
            }
            return name;
        }
        if (node instanceof SqlLambda) {
            final SqlLambda lambda = (SqlLambda) node;
            final Set<String> shadowed = new HashSet<>(lambdaParameters);
            lambda.getParameters().forEach(parameter -> shadowed.remove(parameter.toString()));
            return findLambdaParameterReference(lambda.getExpression(), shadowed, tableAliases);
        }
        if (node instanceof SqlSelect) {
            return findLambdaParameterReferenceInSelect(
                    (SqlSelect) node, lambdaParameters, tableAliases);
        }
        if (node instanceof SqlWithItem) {
            return findLambdaParameterReference(
                    ((SqlWithItem) node).query, lambdaParameters, tableAliases);
        }
        if (node.getKind() == SqlKind.AS) {
            return findLambdaParameterReference(
                    ((SqlCall) node).operand(0), lambdaParameters, tableAliases);
        }
        final List<SqlNode> children;
        if (node instanceof SqlNodeList) {
            children = ((SqlNodeList) node).getList();
        } else if (node instanceof SqlCall) {
            children = ((SqlCall) node).getOperandList();
        } else {
            return null;
        }
        for (SqlNode child : children) {
            if (child != null) {
                final String found =
                        findLambdaParameterReference(child, lambdaParameters, tableAliases);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * Like {@link #findLambdaParameterReference(SqlNode, Set, Set)} for a {@code SELECT}. Its table
     * aliases are visible in its other clauses and in the sub-queries nested there, but not in its
     * own {@code FROM} items, apart from the condition of a join (Calcite registers a derived table
     * in the scope that encloses the {@code SELECT}).
     */
    private static @Nullable String findLambdaParameterReferenceInSelect(
            SqlSelect select, Set<String> lambdaParameters, Set<String> tableAliases) {
        final SqlNode from = select.getFrom();
        final Set<String> visible = new HashSet<>(tableAliases);
        if (from != null) {
            final String found =
                    findLambdaParameterReferenceInFrom(from, lambdaParameters, tableAliases);
            if (found != null) {
                return found;
            }
            collectTableAliases(from, visible);
        }
        for (SqlNode operand : select.getOperandList()) {
            if (operand != null && operand != from) {
                final String found =
                        findLambdaParameterReference(operand, lambdaParameters, visible);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private static @Nullable String findLambdaParameterReferenceInFrom(
            SqlNode from, Set<String> lambdaParameters, Set<String> tableAliases) {
        if (from instanceof SqlIdentifier) {
            // a table name
            return null;
        }
        if (from.getKind() == SqlKind.AS) {
            return findLambdaParameterReferenceInFrom(
                    ((SqlCall) from).operand(0), lambdaParameters, tableAliases);
        }
        if (from instanceof SqlJoin) {
            final SqlJoin join = (SqlJoin) from;
            for (SqlNode side : Arrays.asList(join.getLeft(), join.getRight())) {
                final String found =
                        findLambdaParameterReferenceInFrom(side, lambdaParameters, tableAliases);
                if (found != null) {
                    return found;
                }
            }
            final SqlNode condition = join.getCondition();
            if (condition == null) {
                return null;
            }
            final Set<String> visible = new HashSet<>(tableAliases);
            collectTableAliases(join, visible);
            return findLambdaParameterReference(condition, lambdaParameters, visible);
        }
        return findLambdaParameterReference(from, lambdaParameters, tableAliases);
    }

    /** Adds the aliases that the given {@code FROM} item declares, explicitly or implicitly. */
    private static void collectTableAliases(SqlNode from, Set<String> aliases) {
        if (from instanceof SqlJoin) {
            collectTableAliases(((SqlJoin) from).getLeft(), aliases);
            collectTableAliases(((SqlJoin) from).getRight(), aliases);
            return;
        }
        final String alias = SqlValidatorUtil.alias(from);
        if (alias != null) {
            aliases.add(alias);
        }
    }

    @Override
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        // Due to the improper translation of lateral table left outer join in Calcite, we need to
        // temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
        if (join.getJoinType() == JoinType.LEFT
                && SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
            SqlNode right = SqlUtil.stripAs(join.getRight());
            if (right instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) right;
                SqlNode operand0 = call.operand(0);
                if (operand0 instanceof SqlBasicCall
                        && ((SqlBasicCall) operand0).getOperator()
                                instanceof org.apache.calcite.sql.SqlWindowTableFunction) {
                    return;
                }
            }
            final SqlNode condition = join.getCondition();
            if (condition != null
                    && (!SqlUtil.isLiteral(condition)
                            || ((SqlLiteral) condition).getValueAs(Boolean.class)
                                    != Boolean.TRUE)) {
                throw new ValidationException(
                        String.format(
                                "Left outer joins with a table function do not accept a predicate such as %s. "
                                        + "Only literal TRUE is accepted.",
                                condition));
            }
        }
        super.validateJoin(join, scope);
    }

    @Override
    protected void registerNamespace(
            @Nullable SqlValidatorScope usingScope,
            @Nullable String alias,
            SqlValidatorNamespace ns,
            boolean forceNullable) {

        // Generate a new validator namespace for time travel scenario.
        // Since time travel only supports constant expressions, we need to ensure that the period
        // of
        // snapshot is not an identifier.
        Optional<SqlSnapshot> snapshot = getSnapShotNode(ns);
        if (usingScope != null
                && snapshot.isPresent()
                && !(hasInputReference(snapshot.get().getPeriod()))) {
            SqlSnapshot sqlSnapshot = snapshot.get();
            SqlNode periodNode = sqlSnapshot.getPeriod();
            SqlToRelConverter sqlToRelConverter = this.createSqlToRelConverter();
            RexNode rexNode = sqlToRelConverter.convertExpression(periodNode);
            RexNode simplifiedRexNode =
                    FlinkRexUtil.simplify(
                            sqlToRelConverter.getRexBuilder(),
                            rexNode,
                            relOptCluster.getPlanner().getExecutor());
            List<RexNode> reducedNodes = new ArrayList<>();
            relOptCluster
                    .getPlanner()
                    .getExecutor()
                    .reduce(
                            relOptCluster.getRexBuilder(),
                            Collections.singletonList(simplifiedRexNode),
                            reducedNodes);
            // check whether period is the unsupported expression
            final RexNode reducedNode = reducedNodes.get(0);
            if (!(reducedNode instanceof RexLiteral)) {
                throw new ValidationException(
                        String.format(
                                "Unsupported time travel expression: %s for the expression can not be reduced to a constant by Flink.",
                                periodNode));
            }

            RexLiteral rexLiteral = (RexLiteral) reducedNode;
            final RelDataType sqlType = rexLiteral.getType();
            if (!SqlTypeUtil.isTimestamp(sqlType)) {
                throw newValidationError(
                        periodNode,
                        Static.RESOURCE.illegalExpressionForTemporal(
                                sqlType.getSqlTypeName().getName()));
            }

            TimestampString timestampString = rexLiteral.getValueAs(TimestampString.class);
            checkNotNull(
                    timestampString,
                    "The time travel expression %s can not reduce to a valid timestamp string. This is a bug. Please file an issue.",
                    periodNode);

            TableConfig tableConfig = ShortcutUtils.unwrapContext(relOptCluster).getTableConfig();
            ZoneId zoneId = tableConfig.getLocalTimeZone();
            long timeTravelTimestamp =
                    TimestampData.fromEpochMillis(timestampString.getMillisSinceEpoch())
                            .toLocalDateTime()
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli();

            SchemaVersion schemaVersion = TimestampSchemaVersion.of(timeTravelTimestamp);
            IdentifierNamespace identifierNamespace = (IdentifierNamespace) ns;
            ns =
                    new IdentifierSnapshotNamespace(
                            identifierNamespace,
                            schemaVersion,
                            ((DelegatingScope) usingScope).getParent());

            sqlSnapshot.setOperand(
                    1,
                    SqlLiteral.createTimestamp(
                            SqlTypeName.TIMESTAMP,
                            timestampString,
                            rexLiteral.getType().getPrecision(),
                            sqlSnapshot.getPeriod().getParserPosition()));
        }

        super.registerNamespace(usingScope, alias, ns, forceNullable);
    }

    private static boolean hasInputReference(SqlNode node) {
        return node.accept(new SqlToRelConverter.SqlIdentifierFinder());
    }

    /**
     * Get the {@link SqlSnapshot} node in a {@link SqlValidatorNamespace}.
     *
     * <p>In general, if there is a snapshot expression, the enclosing node of IdentifierNamespace
     * is usually SqlSnapshot. However, if we encounter a situation with an "as" operator, we need
     * to identify whether the enclosingNode is an "as" call and if its first operand is
     * SqlSnapshot.
     *
     * @param ns The namespace used to find SqlSnapshot
     * @return SqlSnapshot found in {@param ns}, empty if not found
     */
    private Optional<SqlSnapshot> getSnapShotNode(SqlValidatorNamespace ns) {
        if (ns instanceof IdentifierNamespace) {
            SqlNode enclosingNode = ns.getEnclosingNode();
            // FOR SYSTEM_TIME AS OF [expression]
            if (enclosingNode instanceof SqlSnapshot) {
                return Optional.of((SqlSnapshot) enclosingNode);
                // FOR SYSTEM_TIME AS OF [expression] as [identifier]
            } else if (enclosingNode instanceof SqlBasicCall
                    && ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlAsOperator
                    && ((SqlBasicCall) enclosingNode).getOperandList().get(0)
                            instanceof SqlSnapshot) {
                return Optional.of(
                        (SqlSnapshot) ((SqlBasicCall) enclosingNode).getOperandList().get(0));
            }
        }
        return Optional.empty();
    }

    private SqlToRelConverter createSqlToRelConverter() {
        return new FlinkSqlToRelConverter(
                toRelContext,
                this,
                this.getCatalogReader().unwrap(CalciteCatalogReader.class),
                relOptCluster,
                frameworkConfig.getConvertletTable(),
                frameworkConfig.getSqlToRelConverterConfig());
    }

    @Override
    protected void addToSelectList(
            List<SqlNode> list,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList,
            SqlNode exp,
            SelectScope scope,
            boolean includeSystemVars) {
        // Extract column's origin to apply strategy
        if (!columnExpansionStrategies.isEmpty() && exp instanceof SqlIdentifier) {
            final SqlQualified qualified = scope.fullyQualify((SqlIdentifier) exp);
            if (qualified.namespace != null && qualified.namespace.getTable() != null) {
                final CatalogSchemaTable schemaTable =
                        (CatalogSchemaTable) qualified.namespace.getTable().table();
                final ResolvedSchema resolvedSchema =
                        schemaTable.getContextResolvedTable().getResolvedSchema();
                final String columnName = qualified.suffix().get(0);
                final Column column = resolvedSchema.getColumn(columnName).orElse(null);
                if (qualified.suffix().size() == 1 && column != null) {
                    if (includeExpandedColumn(column, columnExpansionStrategies)
                            || isDeclaredOnTimeColumn(scope, column)) {
                        super.addToSelectList(
                                list, aliases, fieldList, exp, scope, includeSystemVars);
                    }
                    return;
                }
            }
        }

        // Always add to list
        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    @Override
    protected @PolyNull SqlNode performUnconditionalRewrites(
            @PolyNull SqlNode node, boolean underFrom) {

        // Capture table arguments early:
        // TUMBLE(TABLE t, DESCRIPTOR(metadata_virtual), INTERVAL '1' MINUTE) or
        // SESSION(TABLE t PARTITION BY a, DESCRIPTOR(metadata_virtual), INTERVAL '1' MINUTE)
        // MyPtf(in => TABLE t PARTITION BY a, on_time => DESCRIPTOR(metadata_virtual))
        //
        // "TABLE t" is translated into an implicit "SELECT * FROM t". This would ignore columns
        // that are not expanded by default. However, the on_time descriptor explicitly states the
        // need for time columns. Therefore, explicit table expressions are captured before
        // rewriting and replaced with a "marker" SqlSelect that contains the descriptor
        // information. The "marker" SqlSelect is considered during column expansion.
        final List<SqlIdentifier> tableArgs = getTableOperands(node);

        final SqlNode rewritten = super.performUnconditionalRewrites(node, underFrom);

        if (!(node instanceof SqlBasicCall)) {
            return rewritten;
        }

        final SqlBasicCall call = (SqlBasicCall) node;
        checkNoNamedAndPositionalMixedArgs(call);
        checkDisabledSystemArgs(call);

        // Special case for MODEL
        if (node instanceof SqlExplicitModelCall) {
            // Convert it so that model can be accessed in planner. SqlExplicitModelCall
            // from parser can't access model.
            final SqlExplicitModelCall modelCall = (SqlExplicitModelCall) node;
            final SqlIdentifier modelIdentifier = modelCall.getModelIdentifier();
            final FlinkCalciteCatalogReader catalogReader =
                    (FlinkCalciteCatalogReader) getCatalogReader();
            final CatalogSchemaModel model = catalogReader.getModel(modelIdentifier.names);
            if (model != null) {
                return new SqlModelCall(modelCall, model);
            }
        }

        // Mark rewritten "TABLE t" with on_time columns
        if (tableArgs == null || tableArgs.stream().allMatch(Objects::isNull)) {
            return rewritten;
        }
        final List<SqlIdentifier> onTimeColumns = extractOnTime(call);
        if (onTimeColumns != null) {
            for (int i = 0; i < call.operandCount(); i++) {
                final SqlIdentifier tableArg = tableArgs.get(i);
                if (tableArg != null) {
                    final SqlNode opReplacement =
                            new ExplicitTableSqlSelect(tableArg, onTimeColumns);
                    // for f(TABLE t PARTITION BY c, ...)
                    if (call.operand(i).getKind() == SqlKind.SET_SEMANTICS_TABLE) {
                        final SqlCall setSemanticsTable = call.operand(i);
                        setSemanticsTable.setOperand(0, opReplacement);
                    } else if (call.operand(i).getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                        final SqlCall assignment = call.operand(i);
                        // for f(in => TABLE t PARTITION BY c, ...)
                        if (assignment.operand(0).getKind() == SqlKind.SET_SEMANTICS_TABLE) {
                            final SqlCall setSemanticsTable = assignment.operand(0);
                            setSemanticsTable.setOperand(0, opReplacement);
                        } else {
                            // for f(in => TABLE t, ...)
                            assignment.setOperand(0, opReplacement);
                        }
                    } else {
                        // for f(TABLE t, ...)
                        call.setOperand(i, opReplacement);
                    }
                }
                // for f([in =>] SELECT ..., ...)
            }
        }

        return rewritten;
    }

    /** Mixing positional and named arguments is not supported and crashes operand permutation. */
    private static void checkNoNamedAndPositionalMixedArgs(SqlBasicCall call) {
        if (!(call.getOperator() instanceof SqlFunction)) {
            return;
        }
        final List<SqlNode> operands = call.getOperandList();
        final boolean anyNamed =
                operands.stream()
                        .anyMatch(op -> op != null && op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT);
        final boolean anyPositional =
                operands.stream()
                        .anyMatch(op -> op != null && op.getKind() != SqlKind.ARGUMENT_ASSIGNMENT);
        if (anyNamed && anyPositional) {
            throw new ValidationException(
                    "Cannot mix positional and named arguments when calling function '"
                            + call.getOperator().getName()
                            + "'. Use either all positional arguments or all named arguments "
                            + "(e.g. arg => value).");
        }
    }

    /**
     * Rejects the implicit PTF system arguments (on_time, uid) for functions that disable them.
     *
     * <p>This must happen before Calcite permutes named arguments, because unknown named arguments
     * are silently dropped during permutation and would otherwise be lost. The actual rule and
     * error message live in {@link SystemTypeInference#checkNoSystemArguments} so that the Table
     * API path (which resolves calls without this validator) enforces it identically.
     */
    private static void checkDisabledSystemArgs(SqlBasicCall call) {
        final SqlOperator operator = call.getOperator();
        if (!(operator instanceof BridgingSqlFunction)) {
            return;
        }
        final TypeInference typeInference = ((BridgingSqlFunction) operator).getTypeInference();
        if (!typeInference.disableSystemArguments()) {
            return;
        }
        final Set<String> suppliedArgNames = new HashSet<>();
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null && operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                final SqlNode nameNode = ((SqlCall) operand).operand(1);
                if (nameNode instanceof SqlIdentifier) {
                    suppliedArgNames.add(((SqlIdentifier) nameNode).getSimple());
                }
            }
        }
        // A function that disabled the automatic system arguments may still declare an argument
        // with a reserved name (e.g. SNAPSHOT declares `on_time`); such declared names are allowed.
        final Set<String> declaredArgNames =
                typeInference.getStaticArguments().orElse(List.of()).stream()
                        .map(StaticArgument::getName)
                        .collect(Collectors.toSet());
        SystemTypeInference.checkNoSystemArguments(
                true, suppliedArgNames, declaredArgNames, operator.getName());
    }

    @Override
    public SqlNode maybeCast(SqlNode node, RelDataType currentType, RelDataType desiredType) {
        return super.maybeCast(node, currentType, desiredType);
    }

    @Override
    public @Nullable SqlCall makeNullaryCall(SqlIdentifier id) {
        if (id.names.size() == 1 && !id.isComponentQuoted(0)) {
            return super.makeNullaryCall(id);
        }
        return null;
    }

    // --------------------------------------------------------------------------------------------
    // Column expansion
    // --------------------------------------------------------------------------------------------

    /**
     * A special {@link SqlSelect} to capture the origin of a {@link SqlKind#EXPLICIT_TABLE} within
     * TVF operands.
     */
    static class ExplicitTableSqlSelect extends SqlSelect {

        private final List<SqlIdentifier> onTimeColumns;

        public ExplicitTableSqlSelect(SqlIdentifier table, List<SqlIdentifier> onTimeColumns) {
            super(
                    SqlParserPos.ZERO,
                    null,
                    SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
                    table,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            this.onTimeColumns = onTimeColumns;
        }
    }

    /**
     * Returns whether the given column has been declared in a {@link SqlKind#DESCRIPTOR} next to a
     * {@link SqlKind#EXPLICIT_TABLE} within TVF operands.
     */
    private static boolean isDeclaredOnTimeColumn(SelectScope scope, Column column) {
        if (!(scope.getNode() instanceof ExplicitTableSqlSelect)) {
            return false;
        }
        final ExplicitTableSqlSelect select = (ExplicitTableSqlSelect) scope.getNode();
        return select.onTimeColumns.stream()
                .map(SqlIdentifier::getSimple)
                .anyMatch(id -> id.equals(column.getName()));
    }

    /**
     * Returns all {@link SqlKind#EXPLICIT_TABLE} and {@link SqlKind#SET_SEMANTICS_TABLE} operands
     * within PTF operands. A list entry is {@code null} if the operand is not an {@link
     * SqlKind#EXPLICIT_TABLE} or {@link SqlKind#SET_SEMANTICS_TABLE}.
     */
    private static List<SqlIdentifier> getTableOperands(SqlNode node) {
        if (!(node instanceof SqlBasicCall)) {
            return null;
        }

        final SqlBasicCall call = (SqlBasicCall) node;

        if (!(call.getOperator() instanceof SqlFunction)) {
            return null;
        }

        final SqlFunction function = (SqlFunction) call.getOperator();

        if (!isTableFunction(function)) {
            return null;
        }

        return call.getOperandList().stream()
                .map(FlinkCalciteSqlValidator::extractExplicitTables)
                .collect(Collectors.toList());
    }

    /** Extracts "TABLE t" nodes before they get rewritten into "SELECT * FROM t". */
    private static @Nullable SqlIdentifier extractExplicitTables(SqlNode op) {
        if (op.getKind() == SqlKind.EXPLICIT_TABLE) {
            final SqlBasicCall opCall = (SqlBasicCall) op;
            if (opCall.operandCount() == 1 && opCall.operand(0) instanceof SqlIdentifier) {
                // for f(TABLE t, ...)
                return opCall.operand(0);
            }
        } else if (op.getKind() == SqlKind.SET_SEMANTICS_TABLE) {
            // for f(TABLE t PARTITION BY x)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return extractExplicitTables(opCall.operand(0));
        } else if (op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            // for f(in => TABLE t, ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return extractExplicitTables(opCall.operand(0));
        }
        return null;
    }

    /** Extracts the on_time argument of a PTF (or TIMECOL for window PTFs for legacy reasons). */
    private static @Nullable List<SqlIdentifier> extractOnTime(SqlBasicCall call) {
        // Extract from operand from PTF
        final SqlNode onTimeOperand;
        if (call.getOperator() instanceof SqlWindowTableFunction) {
            onTimeOperand = extractOperandByArgName(call, "TIMECOL");
        } else if (ShortcutUtils.isFunctionKind(call.getOperator(), FunctionKind.PROCESS_TABLE)) {
            onTimeOperand = extractOperandByArgName(call, "on_time");
        } else {
            onTimeOperand = null;
        }

        // No operand found
        if (onTimeOperand == null) {
            return null;
        }

        return extractDescriptors(onTimeOperand);
    }

    private static List<SqlIdentifier> extractDescriptors(SqlNode op) {
        if (op.getKind() == SqlKind.DESCRIPTOR) {
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return opCall.getOperandList().stream()
                    .filter(SqlIdentifier.class::isInstance)
                    .map(SqlIdentifier.class::cast)
                    .collect(Collectors.toList());
        }
        return List.of();
    }

    /**
     * Returns the operand for a given argument name from a BasicSqlCall. Supports both positional
     * and named arguments. If at least one ARGUMENT_ASSIGNMENT is used, named lookup is performed.
     * Otherwise, positional lookup using SqlOperandMetadata is used.
     *
     * @param call the SQL call to extract the operand from
     * @param argumentName the name of the argument to retrieve
     * @return the SqlNode for the operand, or null if not found or not supported
     */
    private static @Nullable SqlNode extractOperandByArgName(
            SqlBasicCall call, String argumentName) {
        // Check if operator supports SqlOperandMetadata
        final SqlOperator operator = call.getOperator();
        final SqlOperandTypeChecker typeChecker = operator.getOperandTypeChecker();
        if (!(typeChecker instanceof SqlOperandMetadata)) {
            return null;
        }

        final SqlOperandMetadata operandMetadata = (SqlOperandMetadata) typeChecker;

        // Detect if named arguments are used by checking for ARGUMENT_ASSIGNMENT
        final List<SqlNode> operands = call.getOperandList();
        final boolean hasNamedArguments =
                operands.stream().anyMatch(op -> op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT);

        if (hasNamedArguments) {
            // Named mode: search through ARGUMENT_ASSIGNMENT nodes
            for (SqlNode operand : operands) {
                if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                    final SqlBasicCall assignment = (SqlBasicCall) operand;
                    // operand(1) contains the parameter name as SqlIdentifier
                    final SqlIdentifier paramName = assignment.operand(1);
                    if (paramName.getSimple().equals(argumentName)) {
                        // operand(0) contains the actual value
                        return assignment.operand(0);
                    }
                }
            }
            return null;
        } else {
            // Positional mode: use SqlOperandMetadata to map name to position
            final List<String> paramNames = operandMetadata.paramNames();
            final int index = paramNames.indexOf(argumentName);
            if (index == -1 || index >= call.operandCount()) {
                return null;
            }
            return call.operand(index);
        }
    }

    private static boolean isTableFunction(SqlFunction function) {
        return function instanceof SqlTableFunction
                || function.getFunctionType() == SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
    }
}
