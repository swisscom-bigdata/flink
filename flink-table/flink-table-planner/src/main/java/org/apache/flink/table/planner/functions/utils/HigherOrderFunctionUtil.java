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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.fun.SqlCurrentDateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utilities for higher-order function calls that carry a {@link RexLambda} argument.
 *
 * <p>Calcite models a lambda as a <em>closed</em> expression: a {@link RexLambda} holds only its
 * parameters and body, with no place for values captured from the enclosing scope. A lambda body
 * may nevertheless reference three kinds of free variable:
 *
 * <ul>
 *   <li>an outer column of the surrounding query (a {@link RexInputRef});
 *   <li>a parameter of an <em>enclosing</em> lambda, for nested higher-order calls (a {@link
 *       RexLambdaRef} whose index is not one of the current lambda's own parameters); and
 *   <li>an expression of the surrounding query that is not evaluated per element at all -- an OVER
 *       window ({@link RexOver}), a sub-query ({@link RexSubQuery}) or a call whose value the
 *       operator computes per row ({@code CURRENT_WATERMARK}, {@code PROCTIME()} and the current
 *       time functions, see {@link #isOperatorValue}), each of which yields one value per row of
 *       the enclosing query. (An aggregate over an outer column needs nothing here: {@code
 *       SqlToRelConverter#convertLambda} already converts it to a reference into the {@code
 *       Aggregate} output, so the body sees an ordinary {@link RexInputRef}.)
 * </ul>
 *
 * <p>None can stay buried in the body: field-usage analysis (and hence column trimming) does not
 * look inside lambda bodies, so a captured column would be trimmed away and dangle; an enclosing
 * parameter reference collides with the current lambda's own parameters on Calcite's positional
 * {@link RexLambdaRef} index (the front end assigns nested lambda parameters distinct, cumulative
 * indices while converting so that such references remain identifiable up to this point); and an
 * OVER window or sub-query is only recognized -- by {@code ProjectToWindowRule} and {@code
 * SubQueryRemoveRule} respectively -- where the enclosing query can evaluate it, never inside a
 * per-element expression that ultimately reaches code generation; and a compiled lambda body has no
 * access to the operator, which reads the watermark, materializes the processing time and takes the
 * current time once per row.
 *
 * <p>{@link #liftCaptures(RexCall, RexBuilder)} therefore <em>closure-converts</em> every free
 * variable out of the lambda:
 *
 * <ul>
 *   <li>every distinct free variable in the body becomes an additional, trailing lambda parameter;
 *   <li>the corresponding free variable is appended as a trailing operand of the enclosing call,
 *       where it is evaluated in the scope that binds it; and
 *   <li>the resulting closed lambda's parameters are renumbered back to the ordinary 0-based
 *       positional form expected by code generation and serialization.
 * </ul>
 *
 * <p>For example {@code ARRAY_TRANSFORM(a, x -> x + col)} becomes {@code ARRAY_TRANSFORM(a, (x,
 * cap) -> x + cap, col)}, and {@code ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))}
 * becomes {@code ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, (x, cap) -> x + cap[1], a))} — the
 * inner call gains the trailing operand {@code a}, evaluated in the outer lambda's body where
 * {@code a} is bound. The lift is driven bottom-up (an inner call is closed before its enclosing
 * call is processed), so a capture more than one level out is threaded hop by hop: it is lifted
 * onto each intervening call in turn, gaining a parameter on every enclosing lambda it passes
 * through.
 *
 * <p>A body may also read a column of an enclosing query through a correlation variable ({@code
 * $cor0.a}), in a sub-query, a {@code LATERAL} sub-query or the argument of {@code UNNEST}. {@link
 * #liftCaptures(RelNode, RexBuilder)} lifts such a reference like a column, so the correlated value
 * becomes an ordinary call operand. Sub-query removal and decorrelation re-index the column
 * references of a sub-query and rewrite its correlated expressions, which they do not do inside a
 * lambda body; after lifting, a body contains neither.
 */
public final class HigherOrderFunctionUtil {

    /**
     * Reserved name prefix of a lifted capture parameter (see {@link #captureName}). The name only
     * keeps plans readable: every stage recognizes a lifted parameter by its index (see {@link
     * #CAPTURE_PARAM_INDEX_BASE} and {@link #isCaptureParameter}), never by its name. The character
     * {@code $} cannot appear in an unquoted SQL identifier, and SQL lambda parameter names that
     * begin with this prefix are rejected during validation (see {@code
     * FlinkCalciteSqlValidator#validateLambda}), so that a plan never shows a SQL lambda's own
     * parameter under a capture's name.
     */
    public static final String CAPTURE_PARAM_PREFIX = "cap$";

    /**
     * First index assigned to a lifted capture parameter (see {@link #captureName}). Capture
     * parameters are numbered {@code CAPTURE_PARAM_INDEX_BASE, +1, +2, ...} while a lambda's own
     * parameters keep the indices {@code 0, 1, 2, ...}.
     *
     * <p>The index, not the name, is what marks a parameter as lifted. {@link
     * RexLambdaRef#equals(Object)} compares only the index and the type -- a parameter's name is
     * deliberately not part of its identity, because two lambdas that differ only in parameter
     * names denote the same function. Calcite therefore deduplicates such lambdas when it builds a
     * {@link org.apache.calcite.rex.RexProgram}, and a shared lambda would carry the names of
     * whichever call site was converted first. Numbering captures apart keeps a lambda that
     * captures structurally distinct from one that does not, so the two can never be merged, and
     * lets every stage recover which parameters are lifted from the lambda alone.
     *
     * <p>The base is far above any reachable parameter count: a lambda argument is passed to {@code
     * eval()} as a {@code FunctionN} or {@code FunctionDataN}, so a lambda has at most {@link
     * org.apache.flink.table.types.logical.FunctionType#MAX_CONVERTIBLE_PARAMETER_COUNT}
     * user-visible parameters.
     */
    public static final int CAPTURE_PARAM_INDEX_BASE = 1_000_000;

    private HigherOrderFunctionUtil() {}

    /** Whether {@code name} is (or would be mistaken for) a lifted capture parameter name. */
    public static boolean isCaptureName(String name) {
        return name.startsWith(CAPTURE_PARAM_PREFIX);
    }

    /**
     * Whether {@code parameter} is a lifted capture rather than one of the lambda's own parameters,
     * i.e. whether it is bound behind the generated function object instead of being passed by the
     * caller. See {@link #CAPTURE_PARAM_INDEX_BASE} for why this is decided by the index.
     */
    public static boolean isCaptureParameter(RexLambdaRef parameter) {
        return parameter.getIndex() >= CAPTURE_PARAM_INDEX_BASE;
    }

    /**
     * The position of the parameter with index {@code parameterIndex} in {@code parameters}, i.e.
     * the field it reads once a lambda body is evaluated against a row of the parameter types.
     * Capture parameters are numbered apart from a lambda's own parameters, so an index is not
     * itself a position.
     *
     * @throws IllegalStateException if no parameter carries that index
     */
    public static int parameterPosition(List<RexLambdaRef> parameters, int parameterIndex) {
        for (int position = 0; position < parameters.size(); position++) {
            if (parameters.get(position).getIndex() == parameterIndex) {
                return position;
            }
        }
        throw new IllegalStateException(
                "Lambda body references parameter #"
                        + parameterIndex
                        + ", which is not declared by the lambda: "
                        + parameters);
    }

    /**
     * Lifts the free-variable captures (outer columns, enclosing lambda parameters, OVER windows
     * and sub-queries) of every {@link RexLambda} operand of {@code call} into additional lambda
     * parameters and trailing call operands, and renumbers each closed lambda's parameters to
     * 0-based positional form (see the class Javadoc). If the call has no lambda operand, or no
     * lambda captures anything and every lambda's parameters are already 0-based, the call is
     * returned unchanged.
     *
     * <p>A call may carry more than one lambda operand (a function may declare several lambda
     * arguments). Each lambda is lifted independently; its captured free variables are appended as
     * trailing call operands after the regular operands, in lambda-operand (left-to-right) order.
     * The capture parameters each lambda gains, recognized by their index (see {@link
     * #isCaptureParameter}), record how many of the trailing operands belong to it, so code
     * generation ({@code BridgingSqlFunctionCallGen}) can partition the trailing operands back to
     * the owning lambda.
     */
    public static RexCall liftCaptures(RexCall call, RexBuilder rexBuilder) {
        return liftCaptures(call, rexBuilder, false);
    }

    private static RexCall liftCaptures(
            RexCall call, RexBuilder rexBuilder, boolean liftCorrelations) {
        final List<RexNode> operands = call.getOperands();
        boolean hasLambda = false;
        for (RexNode operand : operands) {
            if (operand instanceof RexLambda) {
                hasLambda = true;
                break;
            }
        }
        if (!hasLambda) {
            return call;
        }

        // A call is lifted again when a rewrite after conversion, such as decorrelation, has put a
        // column into a lambda body. The capture operands it already has trail the regular
        // operands, grouped by lambda in the order of the lambdas. A lambda's new captures are
        // added to the end of its group, so that code generation can still assign each trailing
        // operand to its lambda.
        int existingCaptureCount = 0;
        for (RexNode operand : operands) {
            if (operand instanceof RexLambda) {
                existingCaptureCount += captureCount((RexLambda) operand);
            }
        }
        final int regularCount = operands.size() - existingCaptureCount;
        final List<RexNode> newOperands = new ArrayList<>(operands.subList(0, regularCount));
        final List<RexNode> trailingCaptures = new ArrayList<>();
        int existingCapturePos = regularCount;
        boolean changed = false;
        for (int pos = 0; pos < regularCount; pos++) {
            if (!(operands.get(pos) instanceof RexLambda)) {
                continue;
            }
            final RexLambda lambda = (RexLambda) operands.get(pos);
            final int lambdaCaptureCount = captureCount(lambda);
            trailingCaptures.addAll(
                    operands.subList(existingCapturePos, existingCapturePos + lambdaCaptureCount));
            existingCapturePos += lambdaCaptureCount;
            final LiftedLambda lifted = liftLambda(lambda, rexBuilder, liftCorrelations);
            if (lifted == null) {
                continue;
            }
            newOperands.set(pos, lifted.lambda);
            trailingCaptures.addAll(lifted.captures);
            changed = true;
        }
        if (!changed) {
            checkClosed(call, liftCorrelations);
            return call;
        }
        newOperands.addAll(trailingCaptures);
        final RexCall lifted =
                (RexCall) rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        checkClosed(lifted, liftCorrelations);
        return lifted;
    }

    /**
     * Lifts the captures of every higher-order call in the tree, including the calls in
     * sub-queries, and lifts a reference to an enclosing query (a correlation variable) like a
     * column. This is the lifting right after conversion.
     */
    public static RelNode liftCaptures(RelNode rel, RexBuilder rexBuilder) {
        return liftCapturesInTree(rel, captureLiftingShuttle(rexBuilder, true));
    }

    private static RelNode liftCapturesInTree(RelNode rel, RexShuttle rexShuttle) {
        return rel.accept(
                new RelHomogeneousShuttle() {
                    @Override
                    public RelNode visit(RelNode other) {
                        return super.visit(other).accept(rexShuttle);
                    }
                });
    }

    /**
     * The number of lifted capture parameters of {@code lambda} (see {@link #isCaptureParameter}).
     */
    public static int captureCount(RexLambda lambda) {
        int count = 0;
        for (RexLambdaRef parameter : lambda.getParameters()) {
            if (isCaptureParameter(parameter)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns {@code node} with each lambda shown as written, for printing a plan only. Every
     * capture parameter in a lambda body is replaced by the value that the call passes for it, and
     * the call's capture operands are dropped. For example, {@code ARRAY_TRANSFORM($0, (x, cap$1)
     * -> +(x, cap$1), $1)} becomes {@code ARRAY_TRANSFORM($0, (x) -> +(x, $1))}. A parameter whose
     * name the printed value would use is shown renamed (see {@link #writtenParameterNames}).
     *
     * <p>The result must not be planned, compared or serialized: its lambda bodies are no longer
     * closed (see the class Javadoc), and the identity of an expression in the planner (its digest)
     * keeps the lifted form.
     */
    public static RexNode toWrittenForm(RexNode node, RexBuilder rexBuilder) {
        return node.accept(new WrittenFormShuttle(rexBuilder));
    }

    /**
     * Returns the names under which the declared parameters of {@code lambda} are shown when it is
     * printed as written (see {@link #toWrittenForm}), in declaration order. A parameter keeps its
     * name unless the printed text of a captured value uses it, which would then appear to read the
     * parameter: the column {@code c} in {@code c -> c + t.c}, for example. Such a parameter is
     * shown as {@code c_1}, or with the next suffix that no captured value and no parameter of this
     * or a nested lambda uses. A name after a dot is a field, such as {@code c} in {@code $cor0.c},
     * and does not conflict.
     */
    public static List<String> writtenParameterNames(
            RexLambda lambda, List<String> capturedValues) {
        final Set<String> taken = new HashSet<>();
        lambda.getParameters().forEach(parameter -> taken.add(parameter.getName()));
        lambda.getExpression()
                .accept(
                        new RexVisitorImpl<Void>(true) {
                            @Override
                            public Void visitLambda(RexLambda nestedLambda) {
                                nestedLambda
                                        .getParameters()
                                        .forEach(parameter -> taken.add(parameter.getName()));
                                return nestedLambda.getExpression().accept(this);
                            }
                        });
        final List<String> names = new ArrayList<>();
        for (RexLambdaRef parameter : lambda.getParameters()) {
            if (isCaptureParameter(parameter)) {
                continue;
            }
            String name = parameter.getName();
            if (isUsedIn(name, capturedValues)) {
                int suffix = 1;
                String candidate;
                do {
                    candidate = name + "_" + suffix++;
                } while (taken.contains(candidate) || isUsedIn(candidate, capturedValues));
                taken.add(candidate);
                name = candidate;
            }
            names.add(name);
        }
        return names;
    }

    private static boolean isUsedIn(String name, List<String> texts) {
        final Pattern pattern =
                Pattern.compile(
                        "(?<![\\p{L}\\p{N}_$.])" + Pattern.quote(name) + "(?![\\p{L}\\p{N}_$])");
        return texts.stream().anyMatch(text -> pattern.matcher(text).find());
    }

    /** See {@link #toWrittenForm}. */
    private static final class WrittenFormShuttle extends RexShuttle {

        private final RexBuilder rexBuilder;

        private WrittenFormShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            final List<RexNode> operands = call.getOperands();
            int captureOperandCount = 0;
            for (RexNode operand : operands) {
                if (operand instanceof RexLambda) {
                    captureOperandCount += captureCount((RexLambda) operand);
                }
            }
            if (captureOperandCount == 0) {
                return super.visitCall(call);
            }
            final int regularCount = operands.size() - captureOperandCount;
            final List<RexNode> capturedValues = new ArrayList<>();
            for (RexNode operand : operands.subList(regularCount, operands.size())) {
                capturedValues.add(operand.accept(this));
            }
            final List<RexNode> newOperands = new ArrayList<>(regularCount);
            int valuePos = 0;
            for (RexNode operand : operands.subList(0, regularCount)) {
                if (operand instanceof RexLambda) {
                    final RexLambda lambda = (RexLambda) operand;
                    final int count = captureCount(lambda);
                    newOperands.add(
                            writtenLambda(
                                    lambda, capturedValues.subList(valuePos, valuePos + count)));
                    valuePos += count;
                } else {
                    newOperands.add(operand.accept(this));
                }
            }
            return call.clone(call.getType(), newOperands);
        }

        @Override
        public RexNode visitLambda(RexLambda lambda) {
            final RexNode body = lambda.getExpression().accept(this);
            if (body == lambda.getExpression()) {
                return lambda;
            }
            return rexBuilder.makeLambdaCall(body, lambda.getParameters());
        }

        // A sub-query prints its own plan, which shows its lambdas as written too.
        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            final RexSubQuery visited = (RexSubQuery) super.visitSubQuery(subQuery);
            final RelNode rel =
                    visited.rel.accept(
                            new RelHomogeneousShuttle() {
                                @Override
                                public RelNode visit(RelNode other) {
                                    return super.visit(other).accept(WrittenFormShuttle.this);
                                }
                            });
            return rel == visited.rel ? visited : visited.clone(rel);
        }

        private RexNode writtenLambda(RexLambda lambda, List<RexNode> capturedValues) {
            final List<String> names =
                    writtenParameterNames(
                            lambda,
                            capturedValues.stream()
                                    .map(RexNode::toString)
                                    .collect(Collectors.toList()));
            final Map<Integer, RexNode> replacements = new HashMap<>();
            final List<RexLambdaRef> declaredParams = new ArrayList<>();
            int valuePos = 0;
            for (RexLambdaRef parameter : lambda.getParameters()) {
                if (isCaptureParameter(parameter)) {
                    replacements.put(parameter.getIndex(), capturedValues.get(valuePos++));
                } else {
                    final String name = names.get(declaredParams.size());
                    final RexLambdaRef shown =
                            name.equals(parameter.getName())
                                    ? parameter
                                    : new RexLambdaRef(
                                            parameter.getIndex(), name, parameter.getType());
                    replacements.put(parameter.getIndex(), shown);
                    declaredParams.add(shown);
                }
            }
            final RexNode body =
                    lambda.getExpression()
                            .accept(
                                    new RexShuttle() {
                                        @Override
                                        public RexNode visitLambdaRef(RexLambdaRef lambdaRef) {
                                            final RexNode replacement =
                                                    replacements.get(lambdaRef.getIndex());
                                            return replacement != null ? replacement : lambdaRef;
                                        }

                                        // A cast of a capture to the type that its value already
                                        // has, such as the cast that turns a captured time
                                        // attribute into a timestamp, does nothing once the value
                                        // is shown in its place.
                                        @Override
                                        public RexNode visitCall(RexCall call) {
                                            final RexNode visited = super.visitCall(call);
                                            if (call.getKind() != SqlKind.CAST
                                                    || !(call.getOperands().get(0)
                                                            instanceof RexLambdaRef)
                                                    || !isCaptureParameter(
                                                            (RexLambdaRef)
                                                                    call.getOperands().get(0))) {
                                                return visited;
                                            }
                                            final RexNode value =
                                                    ((RexCall) visited).getOperands().get(0);
                                            return value.getType().equals(visited.getType())
                                                    ? value
                                                    : visited;
                                        }

                                        @Override
                                        public RexNode visitLambda(RexLambda nestedLambda) {
                                            // A nested lambda is its own scope. The operands that
                                            // fill its captures belong to this body and are
                                            // replaced.
                                            return nestedLambda;
                                        }
                                    });
            return rexBuilder.makeLambdaCall(body.accept(this), declaredParams);
        }
    }

    /**
     * Returns a shuttle that applies {@link #liftCaptures(RexCall, RexBuilder)} to every
     * higher-order call of an expression, bottom-up, so that lifting a nested call propagates to
     * the lambda that encloses it. Expressions without a capturing lambda are left unchanged, and
     * applying the shuttle to an already lifted expression is a no-op.
     */
    public static RexShuttle captureLiftingShuttle(RexBuilder rexBuilder) {
        return captureLiftingShuttle(rexBuilder, false);
    }

    /**
     * Like {@link #captureLiftingShuttle(RexBuilder)}, but if {@code liftCorrelations} is set, also
     * lifts a correlation variable or a field of one ({@code $cor0.a}) as a capture, and rejects
     * one that is left in a lambda body. Used for a whole query, right after conversion and again
     * after decorrelation (see {@link #liftCaptures(RelNode, RexBuilder)}): sub-query removal and
     * decorrelation do not rewrite a correlation inside a lambda body, so it becomes a trailing
     * call operand, which they rewrite like any other operand.
     */
    public static RexShuttle captureLiftingShuttle(
            RexBuilder rexBuilder, boolean liftCorrelations) {
        return new CaptureLiftingShuttle(rexBuilder, liftCorrelations);
    }

    /**
     * Verifies the closed-lambda invariant that {@link #liftCaptures} establishes and that every
     * downstream stage (code generation and compiled-plan serialization) relies on: after lifting,
     * every {@link RexLambda} reachable from {@code node} is <em>closed</em> and in positional
     * form. Concretely, for each lambda:
     *
     * <ul>
     *   <li>its own parameters carry the indices {@code 0, 1, 2, ...}, followed by its lifted
     *       capture parameters carrying {@code CAPTURE_PARAM_INDEX_BASE, +1, +2, ...} (see {@link
     *       #CAPTURE_PARAM_INDEX_BASE});
     *   <li>its body references no outer column ({@link RexInputRef}), no OVER window ({@link
     *       RexOver}), no sub-query ({@link RexSubQuery}) and no operator value ({@link
     *       #isOperatorValue}) -- all of these are free variables that must have been lifted into
     *       trailing call operands; and
     *   <li>every {@link RexLambdaRef} in its body resolves to one of that lambda's own declared
     *       parameters -- no reference to an enclosing lambda's parameter survives inside a nested
     *       lambda body.
     * </ul>
     *
     * <p>A violation indicates a bug in capture lifting or in the front-end conversion, not invalid
     * user input, so it fails fast with an {@link IllegalStateException} rather than producing a
     * lambda that would later crash opaquely in Janino code generation or be serialized into an
     * unrestorable compiled plan.
     */
    public static void checkClosed(RexNode node) {
        checkClosed(node, false);
    }

    /**
     * Like {@link #checkClosed(RexNode)}, but if {@code rejectCorrelations} is set, a correlation
     * variable in a lambda body is a violation too. This holds for a whole query from the lifting
     * right after conversion on (see {@link #liftCaptures(RelNode, RexBuilder)}), and therefore for
     * every compiled plan.
     */
    public static void checkClosed(RexNode node, boolean rejectCorrelations) {
        node.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitLambda(RexLambda lambda) {
                        // Validate this lambda (and, recursively, any nested lambdas in its body)
                        // as one closed scope; do not let the outer shuttle descend into the body
                        // again.
                        checkLambdaClosed(lambda, rejectCorrelations);
                        return lambda;
                    }
                });
    }

    private static void checkLambdaClosed(RexLambda lambda, boolean rejectCorrelations) {
        final List<RexLambdaRef> params = lambda.getParameters();
        if (!isPositional(params)) {
            throw new IllegalStateException(
                    "Lambda parameters are not in positional form after capture lifting: "
                            + lambda);
        }
        final Set<Integer> declaredIndices = new LinkedHashSet<>();
        for (RexLambdaRef param : params) {
            declaredIndices.add(param.getIndex());
        }
        lambda.getExpression()
                .accept(
                        new RexShuttle() {
                            @Override
                            public RexNode visitInputRef(RexInputRef inputRef) {
                                throw new IllegalStateException(
                                        "Lambda body still references outer column "
                                                + inputRef
                                                + " after capture lifting; the lambda is not"
                                                + " closed: "
                                                + lambda);
                            }

                            @Override
                            public RexNode visitLambdaRef(RexLambdaRef lambdaRef) {
                                if (declaredIndices.contains(lambdaRef.getIndex())) {
                                    return lambdaRef;
                                }
                                throw new IllegalStateException(
                                        "Lambda body references parameter #"
                                                + lambdaRef.getIndex()
                                                + ", which is not one of its own parameters "
                                                + declaredIndices
                                                + " after capture lifting; the lambda is not"
                                                + " closed: "
                                                + lambda);
                            }

                            @Override
                            public RexNode visitOver(RexOver over) {
                                throw new IllegalStateException(
                                        "Lambda body still contains an OVER window after capture"
                                                + " lifting; the lambda is not closed: "
                                                + lambda);
                            }

                            @Override
                            public RexNode visitSubQuery(RexSubQuery subQuery) {
                                throw new IllegalStateException(
                                        "Lambda body still contains a sub-query after capture"
                                                + " lifting; the lambda is not closed: "
                                                + lambda);
                            }

                            @Override
                            public RexNode visitCall(RexCall call) {
                                if (isLiftableOperatorValue(call)) {
                                    throw new IllegalStateException(
                                            "Lambda body still contains "
                                                    + call
                                                    + " after capture lifting; the lambda is not"
                                                    + " closed: "
                                                    + lambda);
                                }
                                return super.visitCall(call);
                            }

                            @Override
                            public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                                if (!rejectCorrelations) {
                                    return variable;
                                }
                                throw new IllegalStateException(
                                        "Lambda body still references correlation variable "
                                                + variable
                                                + " after capture lifting; the lambda is not"
                                                + " closed: "
                                                + lambda);
                            }

                            @Override
                            public RexNode visitLambda(RexLambda nestedLambda) {
                                // A nested lambda is its own closed scope; validate it recursively
                                // rather than resolving its references against this lambda's
                                // parameters.
                                checkLambdaClosed(nestedLambda, rejectCorrelations);
                                return nestedLambda;
                            }
                        });
    }

    /** The closed, 0-based form of a single lambda together with its captured free variables. */
    private static final class LiftedLambda {
        private final RexLambda lambda;
        private final List<RexNode> captures;

        private LiftedLambda(RexLambda lambda, List<RexNode> captures) {
            this.lambda = lambda;
            this.captures = captures;
        }
    }

    /**
     * Lifts the free-variable captures of a single {@code lambda} into trailing lambda parameters,
     * returning the closed, 0-based lambda together with its captured free variables in first-seen
     * order. Returns {@code null} if the lambda captures nothing and its parameters are already
     * 0-based positional (nothing to lift for this lambda).
     */
    private static LiftedLambda liftLambda(
            RexLambda lambda, RexBuilder rexBuilder, boolean liftCorrelations) {
        final List<RexLambdaRef> declaredParams = lambda.getParameters();
        final Set<Integer> declaredParamIndices =
                declaredParams.stream().map(RexLambdaRef::getIndex).collect(Collectors.toSet());

        // Collect the distinct free variables of the body in first-seen order, without descending
        // into nested (already-closed) lambdas: an outer column reference, a reference to a
        // parameter bound by an enclosing lambda (a RexLambdaRef whose index is not one of this
        // lambda's own parameters), a whole OVER window / sub-query / operator value, and, if
        // correlations are lifted, a correlation variable or a field of one.
        final Map<Object, RexNode> captures = new LinkedHashMap<>();
        lambda.getExpression()
                .accept(
                        new RexShuttle() {
                            @Override
                            public RexNode visitInputRef(RexInputRef inputRef) {
                                captures.putIfAbsent(captureKey(inputRef), inputRef);
                                return inputRef;
                            }

                            @Override
                            public RexNode visitLambdaRef(RexLambdaRef lambdaRef) {
                                if (!declaredParamIndices.contains(lambdaRef.getIndex())) {
                                    captures.putIfAbsent(captureKey(lambdaRef), lambdaRef);
                                }
                                return lambdaRef;
                            }

                            @Override
                            public RexNode visitOver(RexOver over) {
                                // Captured whole: the window is evaluated per row of the enclosing
                                // query, so its operands belong there too and must not be rewritten
                                // into references to this lambda's parameters.
                                captures.putIfAbsent(captureKey(over), over);
                                return over;
                            }

                            @Override
                            public RexNode visitSubQuery(RexSubQuery subQuery) {
                                captures.putIfAbsent(captureKey(subQuery), subQuery);
                                return subQuery;
                            }

                            @Override
                            public RexNode visitCall(RexCall call) {
                                if (isLiftableOperatorValue(call)) {
                                    captures.putIfAbsent(captureKey(call), call);
                                    return call;
                                }
                                return super.visitCall(call);
                            }

                            @Override
                            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                                if (liftCorrelations && isCorrelationAccess(fieldAccess)) {
                                    captures.putIfAbsent(captureKey(fieldAccess), fieldAccess);
                                    return fieldAccess;
                                }
                                return super.visitFieldAccess(fieldAccess);
                            }

                            @Override
                            public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                                if (liftCorrelations) {
                                    captures.putIfAbsent(captureKey(variable), variable);
                                }
                                return variable;
                            }

                            @Override
                            public RexNode visitLambda(RexLambda nestedLambda) {
                                // A nested lambda's parameter references are closed over its own
                                // parameters, not free variables of this lambda; do not descend.
                                return nestedLambda;
                            }
                        });

        // Fast path: nothing to lift and the parameters are already in 0-based positional form.
        if (captures.isEmpty() && isPositional(declaredParams)) {
            return null;
        }

        // Renumber this lambda's own parameters to 0..k-1 (declaration order) and append one new
        // parameter per capture after them, numbered from CAPTURE_PARAM_INDEX_BASE so that the
        // captures stay recognizable and the lambda cannot be merged with one that captures
        // differently. Captures of an earlier lifting stay captures, in their order, before the new
        // ones. The resulting lambda is closed and in positional form.
        final Map<Integer, RexLambdaRef> renumberedOwnParams = new LinkedHashMap<>();
        final List<RexLambdaRef> newParameters = new ArrayList<>();
        final List<RexLambdaRef> earlierCaptures = new ArrayList<>();
        int nextParamIndex = 0;
        for (RexLambdaRef param : declaredParams) {
            if (isCaptureParameter(param)) {
                earlierCaptures.add(param);
                continue;
            }
            final RexLambdaRef renumbered =
                    new RexLambdaRef(nextParamIndex++, param.getName(), param.getType());
            newParameters.add(renumbered);
            renumberedOwnParams.put(param.getIndex(), renumbered);
        }
        int captureOrdinal = 0;
        for (RexLambdaRef param : earlierCaptures) {
            final RexLambdaRef renumbered =
                    new RexLambdaRef(
                            CAPTURE_PARAM_INDEX_BASE + captureOrdinal++,
                            param.getName(),
                            param.getType());
            newParameters.add(renumbered);
            renumberedOwnParams.put(param.getIndex(), renumbered);
        }
        final Map<Object, RexLambdaRef> captureToParam = new LinkedHashMap<>();
        for (Map.Entry<Object, RexNode> entry : captures.entrySet()) {
            final RexNode capture = entry.getValue();
            final RexLambdaRef param =
                    new RexLambdaRef(
                            CAPTURE_PARAM_INDEX_BASE + captureOrdinal,
                            captureName(capture, captureOrdinal),
                            capture.getType());
            captureOrdinal++;
            newParameters.add(param);
            captureToParam.put(entry.getKey(), param);
        }

        // Rewrite the body: replace own-parameter references with their renumbered form and each
        // captured free variable with its new parameter. Nested lambdas are left untouched (their
        // references, and any trailing capture operands they already carry, are handled when the
        // corresponding call is lifted).
        final RexNode newBody =
                lambda.getExpression()
                        .accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        final RexLambdaRef param =
                                                captureToParam.get(captureKey(inputRef));
                                        return param != null ? param : inputRef;
                                    }

                                    @Override
                                    public RexNode visitLambdaRef(RexLambdaRef lambdaRef) {
                                        final RexLambdaRef own =
                                                renumberedOwnParams.get(lambdaRef.getIndex());
                                        if (own != null) {
                                            return own;
                                        }
                                        final RexLambdaRef param =
                                                captureToParam.get(captureKey(lambdaRef));
                                        return param != null ? param : lambdaRef;
                                    }

                                    @Override
                                    public RexNode visitOver(RexOver over) {
                                        final RexLambdaRef param =
                                                captureToParam.get(captureKey(over));
                                        return param != null ? param : over;
                                    }

                                    @Override
                                    public RexNode visitSubQuery(RexSubQuery subQuery) {
                                        final RexLambdaRef param =
                                                captureToParam.get(captureKey(subQuery));
                                        return param != null ? param : subQuery;
                                    }

                                    @Override
                                    public RexNode visitCall(RexCall call) {
                                        if (isLiftableOperatorValue(call)) {
                                            final RexLambdaRef param =
                                                    captureToParam.get(captureKey(call));
                                            return param != null ? param : call;
                                        }
                                        return super.visitCall(call);
                                    }

                                    @Override
                                    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                                        if (liftCorrelations && isCorrelationAccess(fieldAccess)) {
                                            final RexLambdaRef param =
                                                    captureToParam.get(captureKey(fieldAccess));
                                            return param != null ? param : fieldAccess;
                                        }
                                        return super.visitFieldAccess(fieldAccess);
                                    }

                                    @Override
                                    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                                        final RexLambdaRef param =
                                                captureToParam.get(captureKey(variable));
                                        return param != null ? param : variable;
                                    }

                                    @Override
                                    public RexNode visitLambda(RexLambda nestedLambda) {
                                        return nestedLambda;
                                    }
                                });
        final RexLambda newLambda = (RexLambda) rexBuilder.makeLambdaCall(newBody, newParameters);

        // Return the closed lambda together with its captured free variables in first-seen order.
        // The caller appends the captures as trailing call operands, where they are evaluated in
        // the scope that binds them.
        return new LiftedLambda(newLambda, new ArrayList<>(captures.values()));
    }

    /**
     * A key that identifies a free variable across the collection and rewrite passes. An OVER
     * window or sub-query is keyed by its {@link RexNode#toString() digest}, so two occurrences of
     * the same expression in one body share a single capture while two distinct expressions do not.
     *
     * <p>The digest is a sound semantic key for both cases. A {@link RexOver}'s digest embeds the
     * full window specification via {@code RexWindow.appendDigest} -- partition keys, order keys
     * (with direction and null ordering), the frame bounds, {@code ROWS}/{@code RANGE} mode and
     * exclusion -- as well as the operands, {@code DISTINCT}/{@code IGNORE NULLS} flags and the
     * result type, so two windows that differ in any of these do not collide. A {@link
     * RexSubQuery}'s digest embeds its operands and the full plan of its sub-query relation via
     * {@code RelOptUtil.toString}, which renders correlation variables by id, so two sub-queries
     * that differ in shape or correlation do not collide either.
     */
    private static Object captureKey(RexNode node) {
        if (node instanceof RexInputRef) {
            return "col#" + ((RexInputRef) node).getIndex();
        }
        if (node instanceof RexLambdaRef) {
            return "lambda#" + ((RexLambdaRef) node).getIndex();
        }
        return "expr#" + node;
    }

    /**
     * The name of the lifted parameter that stands in for a captured free variable, where {@code
     * ordinal} is its position among the captures of the lambda being lifted. The name only makes a
     * plan readable -- what marks the parameter as lifted is its index (see {@link
     * #CAPTURE_PARAM_INDEX_BASE}) -- but it is kept distinct from any user-chosen name so that the
     * two cannot be confused in a plan or an error message. The prefix is applied idempotently so
     * that a capture threaded through several nesting levels is not re-prefixed.
     */
    private static String captureName(RexNode node, int ordinal) {
        if (node instanceof RexInputRef) {
            return CAPTURE_PARAM_PREFIX + ((RexInputRef) node).getIndex();
        }
        if (node instanceof RexLambdaRef) {
            final String name = ((RexLambdaRef) node).getName();
            return isCaptureName(name) ? name : CAPTURE_PARAM_PREFIX + name;
        }
        // An OVER window, a sub-query, an operator value or a correlation has no name of its own,
        // so it is named
        // after its position among this lambda's captures. The embedded "$" keeps the name
        // distinct from a captured column ("cap$<column index>", always digits) and, in practice,
        // from a captured enclosing parameter ("cap$<parameter name>").
        final String kind;
        if (node instanceof RexOver) {
            kind = "over$";
        } else if (node instanceof RexSubQuery) {
            kind = "sub$";
        } else if (isProctimeCall(node)) {
            kind = "proctime$";
        } else if (isCurrentWatermarkCall(node)) {
            kind = "wm$";
        } else if (isCurrentTimeCall(node)) {
            kind = "time$";
        } else {
            kind = "cor$";
        }
        return CAPTURE_PARAM_PREFIX + kind + ordinal;
    }

    /**
     * Whether {@code call} is a value that the operator around a higher-order call computes once
     * per row: {@code CURRENT_WATERMARK} reads the operator's watermark, the processing time of
     * {@code PROCTIME()} is only materialized where the operator converts time attributes, and a
     * current time function such as {@code CURRENT_TIMESTAMP} takes the time once per row in
     * streaming mode. A compiled lambda body has no access to the operator, so such a call is
     * lifted like an OVER window. Each value is the same for every element of a row, so computing
     * it once per row does not change the result.
     */
    private static boolean isOperatorValue(RexCall call) {
        return isProctimeCall(call) || isCurrentWatermarkCall(call) || isCurrentTimeCall(call);
    }

    /**
     * Whether {@code call} is an operator value (see {@link #isOperatorValue}) that can be lifted:
     * one that does not read a lambda parameter. {@code CURRENT_WATERMARK} reads a parameter only
     * if the parameter is a time attribute, as an element of an array of time attributes. Such a
     * call stays in the body, where code generation rejects it.
     */
    private static boolean isLiftableOperatorValue(RexCall call) {
        return isOperatorValue(call) && !containsLambdaRef(call);
    }

    private static boolean isProctimeCall(RexNode node) {
        return node instanceof RexCall
                && ((RexCall) node).getOperator() == FlinkSqlOperatorTable.PROCTIME;
    }

    private static boolean isCurrentWatermarkCall(RexNode node) {
        return node instanceof RexCall
                && ShortcutUtils.unwrapFunctionDefinition(node)
                        == BuiltInFunctionDefinitions.CURRENT_WATERMARK;
    }

    /**
     * Whether {@code node} is a current time function: {@code CURRENT_TIMESTAMP}, {@code NOW()},
     * {@code LOCALTIMESTAMP}, {@code CURRENT_DATE}, {@code CURRENT_TIME}, {@code LOCALTIME} or
     * {@code CURRENT_ROW_TIMESTAMP()}.
     */
    private static boolean isCurrentTimeCall(RexNode node) {
        return node instanceof RexCall
                && (((RexCall) node).getOperator() instanceof SqlAbstractTimeFunction
                        || ((RexCall) node).getOperator() instanceof SqlCurrentDateFunction);
    }

    private static boolean containsLambdaRef(RexNode node) {
        final boolean[] found = {false};
        node.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitLambdaRef(RexLambdaRef lambdaRef) {
                        found[0] = true;
                        return null;
                    }
                });
        return found[0];
    }

    /**
     * Whether {@code fieldAccess} reads a field of a correlation variable, possibly nested, such as
     * {@code $cor0.a} or {@code $cor0.r.f}. A field of a lambda parameter, such as {@code x.f}, is
     * not one.
     */
    private static boolean isCorrelationAccess(RexFieldAccess fieldAccess) {
        RexNode reference = fieldAccess.getReferenceExpr();
        while (reference instanceof RexFieldAccess) {
            reference = ((RexFieldAccess) reference).getReferenceExpr();
        }
        return reference instanceof RexCorrelVariable;
    }

    /**
     * Whether the parameters already carry the positional form that {@link #liftCaptures} builds.
     */
    private static boolean isPositional(List<RexLambdaRef> params) {
        int expectedOwnIndex = 0;
        int expectedCaptureIndex = CAPTURE_PARAM_INDEX_BASE;
        boolean inCaptures = false;
        for (RexLambdaRef param : params) {
            if (!inCaptures && param.getIndex() == expectedOwnIndex) {
                expectedOwnIndex++;
                continue;
            }
            // Captures follow the lambda's own parameters and are never interleaved with them
            inCaptures = true;
            if (param.getIndex() != expectedCaptureIndex) {
                return false;
            }
            expectedCaptureIndex++;
        }
        return true;
    }

    /** See {@link #captureLiftingShuttle(RexBuilder, boolean)}. */
    private static final class CaptureLiftingShuttle extends RexShuttle {

        private final RexBuilder rexBuilder;
        private final boolean liftCorrelations;

        private CaptureLiftingShuttle(RexBuilder rexBuilder, boolean liftCorrelations) {
            this.rexBuilder = rexBuilder;
            this.liftCorrelations = liftCorrelations;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            final RexCall visited = (RexCall) super.visitCall(call);
            return liftCaptures(visited, rexBuilder, liftCorrelations);
        }

        // A sub-query is a separate tree that a RexShuttle does not descend into. Its lambdas are
        // lifted like those of the enclosing query, and its references to the enclosing query
        // (correlation variables) are lifted like columns.
        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            final RexSubQuery visited = (RexSubQuery) super.visitSubQuery(subQuery);
            final RelNode rel =
                    liftCapturesInTree(visited.rel, captureLiftingShuttle(rexBuilder, true));
            return rel == visited.rel ? visited : visited.clone(rel);
        }

        // RexShuttle#visitLambda in Calcite 1.41.0 (the pinned version) visits the body but returns
        // the original lambda, discarding any rewrite. Rebuild the lambda with the visited body so
        // that lifting a nested higher-order call (bottom-up) propagates to its enclosing lambda,
        // where an outer capture then becomes a trailing operand that this pass can lift further
        // out. CALCITE-7497 (fixVersion 1.42.0) gives the base class this same shape, so this
        // override becomes redundant on the 1.42.0 upgrade.
        @Override
        public RexNode visitLambda(RexLambda lambda) {
            final RexNode newBody = lambda.getExpression().accept(this);
            if (newBody == lambda.getExpression()) {
                return lambda;
            }
            return rexBuilder.makeLambdaCall(newBody, lambda.getParameters());
        }
    }
}
