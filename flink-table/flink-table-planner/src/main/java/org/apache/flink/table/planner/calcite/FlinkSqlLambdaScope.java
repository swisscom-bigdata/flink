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
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A {@link SqlLambdaScope} whose lambda body may close over the columns of the enclosing query and
 * over the parameters of enclosing lambdas.
 *
 * <p>Calcite 1.41's {@link SqlLambdaScope} treats a lambda as a self-contained expression: any
 * identifier in the body that is not one of the lambda's own parameters is rejected with {@code
 * paramNotFoundInLambdaExpression}. Flink's higher-order functions need closures, e.g. {@code
 * ARRAY_TRANSFORM(a, x -> x + outer_col)} and {@code ARRAY_TRANSFORM(a2d, e -> ARRAY_TRANSFORM(e, x
 * -> x + e[1]))}, so the two resolution methods delegate to the parent scope instead. The
 * conversion side ({@code SqlToRelConverter#convertLambda}) roots the lambda blackboard at the
 * enclosing conversion context, so such a reference becomes a plain input reference of the current
 * row, which {@code HigherOrderFunctionUtil#liftCaptures} then hoists out of the (closed) lambda.
 *
 * <p><b>Upstream status.</b> <a href="https://issues.apache.org/jira/browse/CALCITE-6242">
 * CALCITE-6242</a> ("Enhance lambda closure", fixVersion 1.43.0) makes {@code fullyQualify} and
 * {@code resolveColumn} delegate to the parent scope in exactly this way, behind the new {@code
 * SqlConformance#allowLambdaClosure} flag. Once Flink is on Calcite 1.43.0 those two overrides can
 * be dropped and the conformance flag enabled instead. {@link #isParameter} has no upstream
 * equivalent: CALCITE-6242 still matches only the scope's own parameters, so a nested body closing
 * over an enclosing parameter is still reported as an ungrouped column by {@code AggChecker}. Note
 * also that CALCITE-6242 rejects a lambda parameter that shadows an enclosing one ({@code
 * duplicateLambdaParameter}), which Flink deliberately allows.
 *
 * <p><b>Shadowing.</b> A lambda parameter shadows a table alias of the enclosing query with the
 * same name, so that in {@code SELECT ARRAY_TRANSFORM(r.items, r -> r.amount) FROM t AS r} the
 * {@code r.amount} of the body is the field {@code amount} of the (row-typed) parameter. The
 * conversion side already binds every identifier whose first component is a parameter to that
 * parameter, so validation must agree: {@link #fullyQualify} qualifies such a compound identifier
 * against this scope, and {@link #resolve} hides an enclosing namespace of the parameter's name,
 * since the validator derives the type of {@code r.amount} from a namespace {@code r} before it
 * considers a column {@code r}. Calcite's main branch qualifies compound identifiers the same way,
 * but has no equivalent of the {@link #resolve} override.
 *
 * <p>Subclassing keeps this out of Flink's vendored copies of Calcite: {@link SqlLambdaScope} is
 * public and non-final and its three resolution methods are public, so only the {@code case LAMBDA}
 * branch of the (private) {@code SqlValidatorImpl#registerFrom}, which instantiates the scope, has
 * to be patched in the vendored validator.
 */
@Internal
public class FlinkSqlLambdaScope extends SqlLambdaScope {

    private final SqlLambda lambdaExpr;

    public FlinkSqlLambdaScope(SqlValidatorScope parent, SqlLambda lambdaExpr) {
        super(parent, lambdaExpr);
        this.lambdaExpr = lambdaExpr;
    }

    /**
     * True if the identifier refers to a lambda parameter visible here, i.e. its first component
     * names one. A compound identifier such as {@code r.amount} is a field of parameter {@code r}.
     *
     * <p>{@code AggChecker} calls this to exempt lambda parameters from the "expression is not
     * being grouped" check of an aggregating query. Calcite only matches this lambda's own
     * parameters. A body may also close over the parameters of the lambdas it is nested in, which
     * are no more columns of the enclosing query than its own are, so those are matched too.
     * Otherwise {@code ARRAY_TRANSFORM(a, e -> ARRAY_TRANSFORM(a, x -> x + e)) ... GROUP BY a}
     * would report that {@code e} is not being grouped.
     */
    @Override
    public boolean isParameter(SqlIdentifier id) {
        final String name = id.names.get(0);
        for (SqlValidatorScope current = this;
                current instanceof DelegatingScope;
                current = ((DelegatingScope) current).getParent()) {
            if (current instanceof SqlLambdaScope
                    && ((SqlLambdaScope) current).getParameterTypes().containsKey(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves an identifier of the lambda body. An identifier whose first component names one of
     * this lambda's parameters refers to that parameter, any further components to its fields.
     * Calcite throws {@code paramNotFoundInLambdaExpression} for anything else; delegate to the
     * parent scope instead so that the body can close over the columns of the enclosing query and
     * over the parameters of enclosing lambdas.
     */
    @Override
    public SqlQualified fullyQualify(SqlIdentifier identifier) {
        if (declaresParameter(identifier.names.get(0))) {
            validateFieldPath(identifier);
            return SqlQualified.create(this, 1, null, identifier);
        }
        return parent.fullyQualify(identifier);
    }

    /**
     * Checks that the components after the parameter name are fields of the bound parameter type.
     * The validator assumes that a qualified identifier has been checked this way and fails with an
     * internal error on a field of a type that has none, such as {@code x.f} for an {@code INT}
     * parameter {@code x}. Nothing is checked while the parameter is still unbound.
     */
    private void validateFieldPath(SqlIdentifier identifier) {
        RelDataType type = getParameterTypes().get(identifier.names.get(0));
        for (int i = 1; i < identifier.names.size(); i++) {
            if (type == null || SqlValidatorUtils.isUnresolvedLambdaParameter(type)) {
                return;
            }
            final String name = identifier.names.get(i);
            final RelDataTypeField field =
                    type.isStruct()
                            ? validator.getCatalogReader().nameMatcher().field(type, name)
                            : null;
            if (field == null) {
                throw validator.newValidationError(
                        identifier.getComponent(i), RESOURCE.unknownField(name));
            }
            type = field.getType();
        }
    }

    /**
     * Resolves a namespace (a table alias) by name. A lambda parameter is not a namespace but
     * shadows any enclosing namespace of the same name, see the class comment.
     */
    @Override
    public void resolve(
            List<String> names, SqlNameMatcher nameMatcher, boolean deep, Resolved resolved) {
        if (declaresParameter(names.get(0))) {
            return;
        }
        super.resolve(names, nameMatcher, deep, resolved);
    }

    /**
     * Returns the type of a column of the lambda body. A lambda parameter resolves to its (bound)
     * parameter type; any other column is resolved against the parent scope, for the same reason as
     * in {@link #fullyQualify}. Calcite fails an assertion for a non-parameter here.
     */
    @Override
    public @Nullable RelDataType resolveColumn(String columnName, SqlNode ctx) {
        final Map<String, RelDataType> parameterTypes = getParameterTypes();
        if (parameterTypes.containsKey(columnName)) {
            return parameterTypes.get(columnName);
        }
        return parent.resolveColumn(columnName, ctx);
    }

    private boolean declaresParameter(String name) {
        return lambdaExpr.getParameters().stream()
                .anyMatch(param -> ((SqlIdentifier) param).getSimple().equals(name));
    }
}
