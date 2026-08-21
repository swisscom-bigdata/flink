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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup;
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup;
import org.apache.flink.table.expressions.resolver.rules.ResolverRule;
import org.apache.flink.table.expressions.resolver.rules.ResolverRules;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/**
 * Tries to resolve all unresolved expressions such as {@link UnresolvedReferenceExpression} or
 * calls such as {@link BuiltInFunctionDefinitions#OVER}.
 *
 * <p>The default set of rules ({@link ExpressionResolver#getAllResolverRules()}) will resolve
 * following references:
 *
 * <ul>
 *   <li>flatten '*' and column functions to all fields of underlying inputs
 *   <li>join over aggregates with corresponding over windows into a single resolved call
 *   <li>resolve remaining unresolved references to fields, tables or local references
 *   <li>replace calls to {@link BuiltInFunctionDefinitions#FLATTEN}, {@link
 *       BuiltInFunctionDefinitions#WITH_COLUMNS}, etc.
 *   <li>performs call arguments types validation and inserts additional casts if possible
 * </ul>
 */
@Internal
public class ExpressionResolver {

    /** List of rules for (possibly) expanding the list of unresolved expressions. */
    public static List<ResolverRule> getExpandingResolverRules() {
        return Arrays.asList(
                ResolverRules.UNWRAP_API_EXPRESSION,
                ResolverRules.LOOKUP_CALL_BY_NAME,
                ResolverRules.FLATTEN_STAR_REFERENCE,
                ResolverRules.EXPAND_COLUMN_FUNCTIONS);
    }

    /** List of rules that will be applied during expression resolution. */
    public static List<ResolverRule> getAllResolverRules() {
        return Arrays.asList(
                ResolverRules.UNWRAP_API_EXPRESSION,
                ResolverRules.LOOKUP_CALL_BY_NAME,
                ResolverRules.FLATTEN_STAR_REFERENCE,
                ResolverRules.EXPAND_COLUMN_FUNCTIONS,
                ResolverRules.RESOLVE_TYPE,
                ResolverRules.OVER_WINDOWS,
                ResolverRules.RESOLVE_FIELD,
                ResolverRules.QUALIFY_BUILT_IN_FUNCTIONS,
                ResolverRules.RESOLVE_SQL_CALL,
                ResolverRules.RESOLVE_CALL_BY_ARGUMENTS);
    }

    private static final VerifyResolutionVisitor VERIFY_RESOLUTION_VISITOR =
            new VerifyResolutionVisitor();

    private final ReadableConfig config;

    private final ClassLoader userClassLoader;

    private final FieldReferenceLookup fieldLookup;

    private final TableReferenceLookup tableLookup;

    private final FunctionLookup functionLookup;

    private final DataTypeFactory typeFactory;

    private final SqlExpressionResolver sqlExpressionResolver;

    private final PostResolverFactory postResolverFactory = new PostResolverFactory();

    private final Map<String, LocalReferenceExpression> localReferences;

    /**
     * Parameters of the enclosing lambda(s), in scope order, when this resolver resolves the body
     * of a nested lambda. Kept separate from {@link #localReferences} (which may also hold
     * non-lambda locals such as over-window aliases) so that only lambda parameters are carried
     * into a further nested lambda's scope. Empty for a top-level (non-lambda) resolver.
     */
    private final List<LocalReferenceExpression> enclosingLambdaParameters;

    private final @Nullable DataType outputDataType;

    private final Map<Expression, LocalOverWindow> localOverWindows;

    private final boolean isGroupedAggregation;

    /**
     * @param preparedOverWindows the over-windows of the enclosing resolver, already resolved
     *     against its inputs. Passed only when creating the resolver of a lambda body, which shares
     *     the enclosing scope's windows but must not re-resolve them: its own field lookup hides
     *     the columns shadowed by the lambda parameters. Null everywhere else, in which case {@code
     *     localOverWindows} is resolved here.
     */
    private ExpressionResolver(
            ReadableConfig tableConfig,
            ClassLoader userClassLoader,
            TableReferenceLookup tableLookup,
            FunctionLookup functionLookup,
            DataTypeFactory typeFactory,
            SqlExpressionResolver sqlExpressionResolver,
            FieldReferenceLookup fieldLookup,
            List<OverWindow> localOverWindows,
            @Nullable Map<Expression, LocalOverWindow> preparedOverWindows,
            List<LocalReferenceExpression> localReferences,
            List<LocalReferenceExpression> enclosingLambdaParameters,
            @Nullable DataType outputDataType,
            boolean isGroupedAggregation) {
        this.config = Preconditions.checkNotNull(tableConfig);
        this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
        this.tableLookup = Preconditions.checkNotNull(tableLookup);
        this.fieldLookup = Preconditions.checkNotNull(fieldLookup);
        this.functionLookup = Preconditions.checkNotNull(functionLookup);
        this.typeFactory = Preconditions.checkNotNull(typeFactory);
        this.sqlExpressionResolver = Preconditions.checkNotNull(sqlExpressionResolver);

        this.localReferences =
                localReferences.stream()
                        .collect(
                                Collectors.toMap(
                                        LocalReferenceExpression::getName,
                                        Function.identity(),
                                        (u, v) -> {
                                            throw new IllegalStateException(
                                                    "Duplicate local reference: " + u);
                                        },
                                        LinkedHashMap::new));
        this.enclosingLambdaParameters = enclosingLambdaParameters;
        this.outputDataType = outputDataType;
        this.localOverWindows =
                preparedOverWindows != null
                        ? preparedOverWindows
                        : prepareOverWindows(localOverWindows);
        this.isGroupedAggregation = isGroupedAggregation;
    }

    /**
     * Creates a builder for {@link ExpressionResolver}. One can add additional properties to the
     * resolver like e.g. {@link GroupWindow} or {@link OverWindow}. You can also add additional
     * {@link ResolverRule}.
     *
     * @param tableConfig general configuration
     * @param tableCatalog a way to lookup a table reference by name
     * @param functionLookup a way to lookup call by name
     * @param typeFactory a way to lookup and create data types
     * @param inputs inputs to use for field resolution
     * @return builder for resolver
     */
    public static ExpressionResolverBuilder resolverFor(
            TableConfig tableConfig,
            ClassLoader userClassLoader,
            TableReferenceLookup tableCatalog,
            FunctionLookup functionLookup,
            DataTypeFactory typeFactory,
            SqlExpressionResolver sqlExpressionResolver,
            QueryOperation... inputs) {
        return new ExpressionResolverBuilder(
                inputs,
                tableConfig,
                userClassLoader,
                tableCatalog,
                functionLookup,
                typeFactory,
                sqlExpressionResolver);
    }

    /**
     * Resolves given expressions with configured set of rules. All expressions of an operation
     * should be given at once as some rules might assume the order of expressions.
     *
     * <p>After this method is applied the returned expressions should be ready to be converted to
     * planner specific expressions.
     *
     * @param expressions list of expressions to resolve.
     * @return resolved list of expression
     */
    public List<ResolvedExpression> resolve(List<Expression> expressions) {
        final Function<List<Expression>, List<Expression>> resolveFunction =
                concatenateRules(getAllResolverRules());
        final List<Expression> resolvedExpressions = resolveFunction.apply(expressions);
        return resolvedExpressions.stream()
                .map(e -> e.accept(VERIFY_RESOLUTION_VISITOR))
                .collect(Collectors.toList());
    }

    /**
     * Resolves given expressions with configured set of rules. All expressions of an operation
     * should be given at once as some rules might assume the order of expressions.
     *
     * <p>After this method is applied the returned expressions might contain unresolved expression
     * that can be used for further API transformations.
     *
     * @param expressions list of expressions to resolve.
     * @return resolved list of expression
     */
    public List<Expression> resolveExpanding(List<Expression> expressions) {
        final Function<List<Expression>, List<Expression>> resolveFunction =
                concatenateRules(getExpandingResolverRules());
        return resolveFunction.apply(expressions);
    }

    /**
     * Resolves the body of a lambda expression in a scope that contains the given lambda
     * parameters.
     *
     * <p>The body may close over the parameters of any enclosing lambdas (for nested higher-order
     * calls) and over the input columns of the surrounding query (a {@link
     * org.apache.flink.table.expressions.FieldReferenceExpression}), matching the scoping used on
     * the SQL side. The given parameters shadow enclosing parameters, and parameters shadow input
     * columns, of the same name. Both kinds of capture are closure-converted out of the lambda when
     * it is converted to a {@link org.apache.calcite.rex.RexLambda} (see {@code
     * HigherOrderFunctionUtil#liftCaptures}).
     *
     * @param body the (unresolved) lambda body
     * @param parameters the lambda parameters with their bound types
     * @return the resolved body
     */
    public ResolvedExpression resolveLambdaBody(
            Expression body, List<LocalReferenceExpression> parameters) {
        // The parameters in scope are those of this lambda plus those of any enclosing lambdas,
        // with
        // this lambda's parameters shadowing enclosing ones of the same name (declaration order is
        // kept so the outer-to-inner scope order is preserved). Keying by name is unambiguous
        // because a lambda's own parameter names are unique (see UnresolvedLambdaExpression).
        final Map<String, LocalReferenceExpression> scopeParameters = new LinkedHashMap<>();
        for (LocalReferenceExpression enclosing : enclosingLambdaParameters) {
            scopeParameters.put(enclosing.getName(), enclosing);
        }
        for (LocalReferenceExpression parameter : parameters) {
            scopeParameters.put(parameter.getName(), parameter);
        }
        final List<LocalReferenceExpression> scopeParameterList =
                new ArrayList<>(scopeParameters.values());

        // The parameters shadow input columns of the same name, so hide those columns from the
        // field lookup (references are resolved as fields before local references).
        final FieldReferenceLookup lambdaFieldLookup =
                fieldLookup.withoutFields(scopeParameters.keySet());
        final ExpressionResolver childResolver =
                new ExpressionResolver(
                        config,
                        userClassLoader,
                        tableLookup,
                        functionLookup,
                        typeFactory,
                        sqlExpressionResolver,
                        lambdaFieldLookup,
                        Collections.emptyList(),
                        // the body shares the enclosing scope's over-windows, so that
                        // "a.arrayTransform(x -> x.plus($("base").sum().over($("w"))))" can
                        // reference a window declared on the surrounding table
                        localOverWindows,
                        scopeParameterList,
                        scopeParameterList,
                        null,
                        isGroupedAggregation);
        final List<ResolvedExpression> resolved =
                childResolver.resolve(Collections.singletonList(body));
        if (resolved.size() != 1) {
            throw new TableException(
                    "A lambda body must resolve to a single expression. Got: " + resolved);
        }
        return resolved.get(0);
    }

    /**
     * Enables the creation of resolved expressions for transformations after the actual resolution.
     */
    public PostResolverFactory postResolverFactory() {
        return postResolverFactory;
    }

    private Function<List<Expression>, List<Expression>> concatenateRules(
            List<ResolverRule> rules) {
        return rules.stream()
                .reduce(
                        Function.identity(),
                        (function, resolverRule) ->
                                function.andThen(
                                        exprs ->
                                                resolverRule.apply(
                                                        exprs, new ExpressionResolverContext())),
                        Function::andThen);
    }

    private Map<Expression, LocalOverWindow> prepareOverWindows(List<OverWindow> overWindows) {
        return overWindows.stream()
                .map(this::resolveOverWindow)
                .collect(Collectors.toMap(LocalOverWindow::getAlias, Function.identity()));
    }

    private List<Expression> prepareExpressions(List<Expression> expressions) {
        return expressions.stream()
                .flatMap(e -> resolveExpanding(Collections.singletonList(e)).stream())
                .map(this::resolveFieldsInSingleExpression)
                .collect(Collectors.toList());
    }

    private Expression resolveFieldsInSingleExpression(Expression expression) {
        List<Expression> expressions =
                ResolverRules.RESOLVE_FIELD.apply(
                        Collections.singletonList(expression), new ExpressionResolverContext());

        if (expressions.size() != 1) {
            throw new TableException(
                    "Expected a single expression as a result. Got: " + expressions);
        }

        return expressions.get(0);
    }

    private static class VerifyResolutionVisitor
            extends ApiExpressionDefaultVisitor<ResolvedExpression> {

        @Override
        public ResolvedExpression visit(CallExpression call) {
            call.getChildren().forEach(c -> c.accept(this));
            return call;
        }

        @Override
        protected ResolvedExpression defaultMethod(Expression expression) {
            if (expression instanceof ResolvedExpression) {
                return (ResolvedExpression) expression;
            }
            throw new TableException(
                    "All expressions should have been resolved at this stage. Unexpected expression: "
                            + expression);
        }
    }

    private class ExpressionResolverContext implements ResolverRule.ResolutionContext {

        @Override
        public ReadableConfig configuration() {
            return config;
        }

        @Override
        public ClassLoader userClassLoader() {
            return userClassLoader;
        }

        @Override
        public FieldReferenceLookup referenceLookup() {
            return fieldLookup;
        }

        @Override
        public TableReferenceLookup tableLookup() {
            return tableLookup;
        }

        @Override
        public FunctionLookup functionLookup() {
            return functionLookup;
        }

        @Override
        public DataTypeFactory typeFactory() {
            return typeFactory;
        }

        public SqlExpressionResolver sqlExpressionResolver() {
            return sqlExpressionResolver;
        }

        @Override
        public PostResolverFactory postResolutionFactory() {
            return postResolverFactory;
        }

        @Override
        public ResolvedExpression resolveLambdaBody(
                Expression body, List<LocalReferenceExpression> parameters) {
            return ExpressionResolver.this.resolveLambdaBody(body, parameters);
        }

        @Override
        public Optional<LocalReferenceExpression> getLocalReference(String alias) {
            return Optional.ofNullable(localReferences.get(alias));
        }

        @Override
        public List<LocalReferenceExpression> getLocalReferences() {
            return new ArrayList<>(localReferences.values());
        }

        @Override
        public Optional<DataType> getOutputDataType() {
            return Optional.ofNullable(outputDataType);
        }

        @Override
        public Optional<LocalOverWindow> getOverWindow(Expression alias) {
            return Optional.ofNullable(localOverWindows.get(alias));
        }

        @Override
        public boolean isGroupedAggregation() {
            return isGroupedAggregation;
        }
    }

    private LocalOverWindow resolveOverWindow(OverWindow overWindow) {
        return new LocalOverWindow(
                overWindow.getAlias(),
                prepareExpressions(overWindow.getPartitioning()),
                resolveFieldsInSingleExpression(overWindow.getOrder()),
                overWindow.getPreceding().map(this::resolveFieldsInSingleExpression).orElse(null),
                overWindow.getFollowing().map(this::resolveFieldsInSingleExpression).orElse(null));
    }

    /**
     * Factory for creating resolved expressions after the actual resolution has happened. This is
     * required when a resolved expression stack needs to be modified in later transformations.
     *
     * <p>Note: Further resolution or validation will not happen anymore, therefore the created
     * expressions must be valid.
     */
    @Internal
    public class PostResolverFactory {

        public CallExpression as(ResolvedExpression expression, String alias) {
            return createCallExpression(
                    BuiltInFunctionDefinitions.AS,
                    Arrays.asList(expression, valueLiteral(alias)),
                    expression.getOutputDataType());
        }

        public CallExpression cast(ResolvedExpression expression, DataType dataType) {
            return createCallExpression(
                    BuiltInFunctionDefinitions.CAST,
                    Arrays.asList(expression, typeLiteral(dataType)),
                    dataType);
        }

        public CallExpression row(DataType dataType, ResolvedExpression... expression) {
            return createCallExpression(
                    BuiltInFunctionDefinitions.ROW, Arrays.asList(expression), dataType);
        }

        public CallExpression array(DataType dataType, ResolvedExpression... expression) {
            return createCallExpression(
                    BuiltInFunctionDefinitions.ARRAY, Arrays.asList(expression), dataType);
        }

        public CallExpression map(DataType dataType, ResolvedExpression... expression) {
            return createCallExpression(
                    BuiltInFunctionDefinitions.MAP, Arrays.asList(expression), dataType);
        }

        public CallExpression wrappingCall(
                BuiltInFunctionDefinition definition, ResolvedExpression expression) {
            return createCallExpression(
                    definition,
                    Collections.singletonList(expression),
                    expression.getOutputDataType()); // the output type is equal to the input type
        }

        public CallExpression get(
                ResolvedExpression composite, ValueLiteralExpression key, DataType dataType) {
            return createCallExpression(
                    BuiltInFunctionDefinitions.GET, Arrays.asList(composite, key), dataType);
        }

        private CallExpression createCallExpression(
                BuiltInFunctionDefinition builtInDefinition,
                List<ResolvedExpression> resolvedArgs,
                DataType outputDataType) {
            final ContextResolvedFunction resolvedFunction =
                    functionLookup.lookupBuiltInFunction(builtInDefinition);
            return resolvedFunction.toCallExpression(resolvedArgs, outputDataType);
        }
    }

    /** Builder for creating {@link ExpressionResolver}. */
    @Internal
    public static class ExpressionResolverBuilder {

        private final TableConfig tableConfig;
        private final ClassLoader userClassLoader;
        private final List<QueryOperation> queryOperations;
        private final TableReferenceLookup tableCatalog;
        private final FunctionLookup functionLookup;
        private final DataTypeFactory typeFactory;
        private final SqlExpressionResolver sqlExpressionResolver;
        private List<OverWindow> logicalOverWindows = new ArrayList<>();
        private List<LocalReferenceExpression> localReferences = new ArrayList<>();
        private @Nullable DataType outputDataType;
        private boolean isGroupedAggregation;

        private ExpressionResolverBuilder(
                QueryOperation[] queryOperations,
                TableConfig tableConfig,
                ClassLoader userClassLoader,
                TableReferenceLookup tableCatalog,
                FunctionLookup functionLookup,
                DataTypeFactory typeFactory,
                SqlExpressionResolver sqlExpressionResolver) {
            this.tableConfig = tableConfig;
            this.userClassLoader = userClassLoader;
            this.queryOperations = Arrays.asList(queryOperations);
            this.tableCatalog = tableCatalog;
            this.functionLookup = functionLookup;
            this.typeFactory = typeFactory;
            this.sqlExpressionResolver = sqlExpressionResolver;
        }

        public ExpressionResolverBuilder withOverWindows(List<OverWindow> windows) {
            this.logicalOverWindows = Preconditions.checkNotNull(windows);
            return this;
        }

        public ExpressionResolverBuilder withLocalReferences(
                LocalReferenceExpression... localReferences) {
            this.localReferences = Arrays.asList(localReferences);
            return this;
        }

        public ExpressionResolverBuilder withOutputDataType(@Nullable DataType outputDataType) {
            this.outputDataType = outputDataType;
            return this;
        }

        public ExpressionResolverBuilder withGroupedAggregation(boolean isGroupedAggregation) {
            this.isGroupedAggregation = isGroupedAggregation;
            return this;
        }

        public ExpressionResolver build() {
            return new ExpressionResolver(
                    tableConfig,
                    userClassLoader,
                    tableCatalog,
                    functionLookup,
                    typeFactory,
                    sqlExpressionResolver,
                    new FieldReferenceLookup(queryOperations),
                    logicalOverWindows,
                    null,
                    localReferences,
                    Collections.emptyList(),
                    outputDataType,
                    isGroupedAggregation);
        }
    }
}
