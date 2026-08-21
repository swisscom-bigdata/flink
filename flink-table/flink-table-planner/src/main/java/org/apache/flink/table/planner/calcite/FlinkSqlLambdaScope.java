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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

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
     * True if the identifier matches the name of a lambda parameter visible here.
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
        final String name = id.toString();
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
     * Resolves an identifier of the lambda body. Calcite throws {@code
     * paramNotFoundInLambdaExpression} for anything that is not one of this lambda's parameters;
     * delegate to the parent scope instead so that the body can close over the columns of the
     * enclosing query and over the parameters of enclosing lambdas.
     */
    @Override
    public SqlQualified fullyQualify(SqlIdentifier identifier) {
        final boolean found =
                lambdaExpr.getParameters().stream()
                        .anyMatch(param -> param.equalsDeep(identifier, Litmus.IGNORE));
        if (found) {
            return SqlQualified.create(this, 1, null, identifier);
        }
        return parent.fullyQualify(identifier);
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
}
