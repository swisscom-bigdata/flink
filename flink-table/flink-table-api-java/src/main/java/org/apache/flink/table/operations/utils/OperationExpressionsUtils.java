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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.ModelReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedLambdaExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;

/**
 * Utility methods for transforming {@link Expression} to use them in {@link QueryOperation}s.
 *
 * <p>Note: Some of these utilities are intended to be used before expressions are fully resolved
 * and some afterwards.
 */
@Internal
public class OperationExpressionsUtils {

    /**
     * Functions that denote a property of the {@link org.apache.flink.table.api.GroupWindow} they
     * are called on.
     */
    public static final Set<FunctionDefinition> WINDOW_PROPERTIES =
            new HashSet<>(
                    Arrays.asList(
                            BuiltInFunctionDefinitions.WINDOW_START,
                            BuiltInFunctionDefinitions.WINDOW_END,
                            BuiltInFunctionDefinitions.PROCTIME,
                            BuiltInFunctionDefinitions.ROWTIME));

    /** Functions that declare the sort order of an expression in a {@code orderBy}. */
    public static final Set<FunctionDefinition> ORDERING =
            new HashSet<>(
                    Arrays.asList(
                            BuiltInFunctionDefinitions.ORDER_ASC,
                            BuiltInFunctionDefinitions.ORDER_DESC));

    // --------------------------------------------------------------------------------------------
    // Pre-expression resolution utils
    // --------------------------------------------------------------------------------------------

    /** Container for extracted expressions of the same family. */
    @Internal
    public static class CategorizedExpressions {
        private final List<Expression> projections;
        private final List<Expression> aggregations;
        private final List<Expression> windowProperties;

        CategorizedExpressions(
                List<Expression> projections,
                List<Expression> aggregations,
                List<Expression> windowProperties) {
            this.projections = projections;
            this.aggregations = aggregations;
            this.windowProperties = windowProperties;
        }

        public List<Expression> getProjections() {
            return projections;
        }

        public List<Expression> getAggregations() {
            return aggregations;
        }

        public List<Expression> getWindowProperties() {
            return windowProperties;
        }
    }

    /**
     * Extracts and deduplicates all aggregation and window property expressions (zero, one, or
     * more) from the given expressions.
     *
     * <p>The body of a lambda argument is descended into as well, although {@link
     * UnresolvedLambdaExpression#getChildren()} hides it from generic traversals: an aggregate
     * written there (e.g. {@code a.arrayTransform(x -> x.plus($("base").sum()))}) aggregates over
     * the enclosing query, not over the array, and so belongs to the enclosing aggregation.
     * Hoisting it out leaves the body capturing an ordinary column of the aggregation's output. An
     * aggregate over a lambda <em>parameter</em> has no such reading and is rejected outright.
     *
     * @param expressions a list of expressions to extract
     * @return a Tuple2, the first field contains the extracted and deduplicated aggregations, and
     *     the second field contains the extracted and deduplicated window properties.
     */
    public static CategorizedExpressions extractAggregationsAndProperties(
            List<Expression> expressions) {
        AggregationAndPropertiesSplitter splitter = new AggregationAndPropertiesSplitter();
        expressions.forEach(expr -> expr.accept(splitter));

        List<Expression> projections =
                expressions.stream()
                        .map(
                                expr ->
                                        expr.accept(
                                                new AggregationAndPropertiesReplacer(
                                                        splitter.aggregates, splitter.properties)))
                        .collect(Collectors.toList());

        List<Expression> aggregates = nameExpressions(splitter.aggregates);
        List<Expression> properties = nameExpressions(splitter.properties);

        return new CategorizedExpressions(projections, aggregates, properties);
    }

    private static List<Expression> nameExpressions(Map<Expression, String> expressions) {
        return expressions.entrySet().stream()
                .map(entry -> unresolvedCall(AS, entry.getKey(), valueLiteral(entry.getValue())))
                .collect(Collectors.toList());
    }

    private static class AggregationAndPropertiesSplitter
            extends ApiExpressionDefaultVisitor<Void> {

        private int uniqueId = 0;
        private final Map<Expression, String> aggregates = new LinkedHashMap<>();
        private final Map<Expression, String> properties = new LinkedHashMap<>();

        /** Lambda parameters in scope at the node currently being visited. */
        private final Set<String> lambdaParameters = new HashSet<>();

        @Override
        public Void visit(LookupCallExpression unresolvedCall) {
            throw new IllegalStateException(
                    "All lookup calls should be resolved by now. Got: " + unresolvedCall);
        }

        @Override
        public Void visit(UnresolvedCallExpression unresolvedCall) {
            FunctionDefinition functionDefinition = unresolvedCall.getFunctionDefinition();
            if (isFunctionOfKind(unresolvedCall, AGGREGATE)) {
                // An aggregate over a lambda parameter is not an aggregate of the enclosing query
                // and has no meaning of its own. Reported here rather than left to the lambda body
                // validation, which runs after the projection has been resolved against the
                // aggregation's output -- by then the columns the aggregate also references are
                // gone and the failure would be an unhelpful "cannot resolve field".
                if (referencesLambdaParameter(unresolvedCall, lambdaParameters)) {
                    throw new ValidationException(
                            unsupportedOverLambdaParameter("Aggregate functions"));
                }
                aggregates.computeIfAbsent(unresolvedCall, expr -> "EXPR$" + uniqueId++);
            } else if (WINDOW_PROPERTIES.contains(functionDefinition)) {
                properties.computeIfAbsent(unresolvedCall, expr -> "EXPR$" + uniqueId++);
            } else {
                unresolvedCall.getChildren().forEach(c -> c.accept(this));
            }
            return null;
        }

        @Override
        public Void visitNonApiExpression(Expression other) {
            if (other instanceof UnresolvedLambdaExpression) {
                final UnresolvedLambdaExpression lambda = (UnresolvedLambdaExpression) other;
                final List<String> declared =
                        lambda.getParameterNames().stream()
                                .filter(lambdaParameters::add)
                                .collect(Collectors.toList());
                lambda.getBody().accept(this);
                lambdaParameters.removeAll(declared);
            }
            return null;
        }

        @Override
        protected Void defaultMethod(Expression expression) {
            return null;
        }
    }

    /**
     * The error reported when a row-level construct in a lambda body is evaluated over a lambda
     * parameter. Shared with the lambda body validation in {@code ResolveCallByArgumentsRule},
     * which catches the cases that do not pass through aggregate extraction.
     *
     * <p>Unlike SQL, the message does not name the offending parameter: a Table API lambda
     * parameter carries a generated name (see {@code BaseExpressions#newLambdaParameterName}) that
     * would mean nothing to the reader.
     *
     * @param construct the user-facing name of the construct, e.g. {@code "Aggregate functions"}
     */
    public static String unsupportedOverLambdaParameter(String construct) {
        return String.format(
                "%s over a lambda parameter are not supported in the body of a lambda expression. "
                        + "A lambda parameter exists per element and has no group to be evaluated "
                        + "over; only the columns of the enclosing query can be used here.",
                construct);
    }

    /**
     * Whether {@code expression} references one of the given lambda parameters. A lambda nested
     * below it shadows the parameters it redeclares, and its own parameters do not count: they are
     * bound within it, so an aggregate in between still aggregates over the enclosing query.
     *
     * <p>Called before resolution, where a lambda parameter is an ordinary {@link
     * org.apache.flink.table.expressions.UnresolvedReferenceExpression} and can only be told from a
     * column by its name.
     */
    static boolean referencesLambdaParameter(Expression expression, Set<String> lambdaParameters) {
        if (lambdaParameters.isEmpty()) {
            return false;
        }
        if (expression instanceof UnresolvedReferenceExpression) {
            return lambdaParameters.contains(
                    ((UnresolvedReferenceExpression) expression).getName());
        }
        if (expression instanceof UnresolvedLambdaExpression) {
            final UnresolvedLambdaExpression lambda = (UnresolvedLambdaExpression) expression;
            final Set<String> shadowed = new HashSet<>(lambdaParameters);
            shadowed.removeAll(lambda.getParameterNames());
            return referencesLambdaParameter(lambda.getBody(), shadowed);
        }
        return expression.getChildren().stream()
                .anyMatch(child -> referencesLambdaParameter(child, lambdaParameters));
    }

    private static class AggregationAndPropertiesReplacer
            extends ApiExpressionDefaultVisitor<Expression> {

        private final Map<Expression, String> aggregates;
        private final Map<Expression, String> properties;

        private AggregationAndPropertiesReplacer(
                Map<Expression, String> aggregates, Map<Expression, String> properties) {
            this.aggregates = aggregates;
            this.properties = properties;
        }

        @Override
        public Expression visit(LookupCallExpression unresolvedCall) {
            throw new IllegalStateException(
                    "All lookup calls should be resolved by now. Got: " + unresolvedCall);
        }

        @Override
        public Expression visit(CallExpression call) {
            throw new IllegalStateException("All calls should still be unresolved by now.");
        }

        @Override
        public Expression visit(UnresolvedCallExpression unresolvedCall) {
            if (aggregates.get(unresolvedCall) != null) {
                return unresolvedRef(aggregates.get(unresolvedCall));
            } else if (properties.get(unresolvedCall) != null) {
                return unresolvedRef(properties.get(unresolvedCall));
            }

            final List<Expression> args =
                    unresolvedCall.getChildren().stream()
                            .map(c -> c.accept(this))
                            .collect(Collectors.toList());
            return unresolvedCall.replaceArgs(args);
        }

        @Override
        public Expression visitNonApiExpression(Expression other) {
            if (other instanceof UnresolvedLambdaExpression) {
                final UnresolvedLambdaExpression lambda = (UnresolvedLambdaExpression) other;
                final Expression newBody = lambda.getBody().accept(this);
                if (newBody != lambda.getBody()) {
                    return new UnresolvedLambdaExpression(lambda.getParameterNames(), newBody);
                }
            }
            return other;
        }

        @Override
        protected Expression defaultMethod(Expression expression) {
            return expression;
        }
    }

    // --------------------------------------------------------------------------------------------
    // utils that can be used both before and after resolution
    // --------------------------------------------------------------------------------------------

    private static final ExtractNameVisitor extractNameVisitor = new ExtractNameVisitor();

    /**
     * Extracts names from given expressions if they have one. Expressions that have names are:
     *
     * <ul>
     *   <li>{@link FieldReferenceExpression}
     *   <li>{@link TableReferenceExpression}
     *   <li>{@link LocalReferenceExpression}
     *   <li>{@link BuiltInFunctionDefinitions#AS}
     * </ul>
     *
     * @param expressions list of expressions to extract names from
     * @return corresponding list of optional names
     */
    public static List<Optional<String>> extractNames(List<ResolvedExpression> expressions) {
        return expressions.stream()
                .map(OperationExpressionsUtils::extractName)
                .collect(Collectors.toList());
    }

    /**
     * Extracts name from given expression if it has one. Expressions that have names are:
     *
     * <ul>
     *   <li>{@link FieldReferenceExpression}
     *   <li>{@link TableReferenceExpression}
     *   <li>{@link LocalReferenceExpression}
     *   <li>{@link BuiltInFunctionDefinitions#AS}
     * </ul>
     *
     * @param expression expression to extract name from
     * @return optional name of given expression
     */
    public static Optional<String> extractName(Expression expression) {
        return expression.accept(extractNameVisitor);
    }

    private static class ExtractNameVisitor extends ApiExpressionDefaultVisitor<Optional<String>> {

        @Override
        public Optional<String> visit(LookupCallExpression lookupCall) {
            throw new IllegalStateException("All lookup calls should be resolved by now.");
        }

        @Override
        public Optional<String> visit(UnresolvedCallExpression unresolvedCall) {
            if (unresolvedCall.getFunctionDefinition() == AS) {
                return extractValue(unresolvedCall.getChildren().get(1), String.class);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<String> visit(CallExpression call) {
            if (call.getFunctionDefinition() == AS) {
                return extractValue(call.getChildren().get(1), String.class);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<String> visit(LocalReferenceExpression localReference) {
            return Optional.of(localReference.getName());
        }

        @Override
        public Optional<String> visit(TableReferenceExpression tableReference) {
            return Optional.of(tableReference.getName());
        }

        @Override
        public Optional<String> visit(ModelReferenceExpression modelReference) {
            return Optional.of(modelReference.getName());
        }

        @Override
        public Optional<String> visit(FieldReferenceExpression fieldReference) {
            return Optional.of(fieldReference.getName());
        }

        @Override
        protected Optional<String> defaultMethod(Expression expression) {
            return Optional.empty();
        }
    }

    /**
     * Adds an input alias to all {@link FieldReferenceExpression} in the given {@code expression}.
     */
    public static ResolvedExpression scopeReferencesWithAlias(
            final String aliasName, final ResolvedExpression expression) {
        return expression.accept(
                new TableReferenceScopingVisitor(Collections.singletonMap(0, aliasName)));
    }

    /**
     * Adds an input alias to all {@link FieldReferenceExpression} in the given {@code expression}.
     * This method accepts multiple aliases for given input indices.
     */
    public static ResolvedExpression scopeReferencesWithAlias(
            final Map<Integer, String> inputAliases, final ResolvedExpression expression) {
        return expression.accept(new TableReferenceScopingVisitor(inputAliases));
    }

    private static class TableReferenceScopingVisitor
            extends ResolvedExpressionDefaultVisitor<ResolvedExpression> {

        private final Map<Integer, String> inputAliases;

        private TableReferenceScopingVisitor(Map<Integer, String> inputAliases) {
            this.inputAliases = inputAliases;
        }

        @Override
        public ResolvedExpression visit(CallExpression call) {
            List<ResolvedExpression> scopedChildren =
                    call.getChildren().stream()
                            .map(c -> c.accept(this))
                            .collect(Collectors.toList());
            return call.replaceArgs(scopedChildren, call.getOutputDataType());
        }

        @Override
        public ResolvedExpression visit(FieldReferenceExpression fieldReference) {
            return new FieldReferenceExpression(
                    fieldReference.getName(),
                    fieldReference.getOutputDataType(),
                    fieldReference.getInputIndex(),
                    fieldReference.getFieldIndex(),
                    inputAliases.get(fieldReference.getInputIndex()));
        }

        @Override
        protected ResolvedExpression defaultMethod(ResolvedExpression expression) {
            return expression;
        }
    }

    private OperationExpressionsUtils() {}
}
