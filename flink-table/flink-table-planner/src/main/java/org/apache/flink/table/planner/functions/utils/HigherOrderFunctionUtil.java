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

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 *       window ({@link RexOver}) or a sub-query ({@link RexSubQuery}), each of which yields one
 *       value per row of the enclosing query. (An aggregate over an outer column needs nothing
 *       here: {@code SqlToRelConverter#convertLambda} already converts it to a reference into the
 *       {@code Aggregate} output, so the body sees an ordinary {@link RexInputRef}.)
 * </ul>
 *
 * <p>None can stay buried in the body: field-usage analysis (and hence column trimming) does not
 * look inside lambda bodies, so a captured column would be trimmed away and dangle; an enclosing
 * parameter reference collides with the current lambda's own parameters on Calcite's positional
 * {@link RexLambdaRef} index (the front end assigns nested lambda parameters distinct, cumulative
 * indices while converting so that such references remain identifiable up to this point); and an
 * OVER window or sub-query is only recognized -- by {@code ProjectToWindowRule} and {@code
 * SubQueryRemoveRule} respectively -- where the enclosing query can evaluate it, never inside a
 * per-element expression that ultimately reaches code generation.
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
 */
public final class HigherOrderFunctionUtil {

    /**
     * Reserved name prefix of a lifted capture parameter (see {@link #captureName}). The suffix
     * {@code $} cannot appear in an unquoted SQL identifier, and lambda parameter names beginning
     * with this prefix are rejected during validation (see {@code
     * SqlValidatorImpl#validateLambda}), so a lifted capture parameter can never collide with a
     * user-chosen lambda parameter. Code generation relies on this to distinguish trailing capture
     * operands from a function's declared arguments (see {@code BridgingSqlFunctionCallGen}).
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
     * eval()} as a {@code FunctionData} / {@code BiFunctionData} / {@code TriFunctionData}, so a
     * function can declare at most three user-visible parameters.
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
     * <p>A call may carry more than one lambda operand (a user-defined function that declares
     * several lambda arguments). Each lambda is lifted independently; its captured free variables
     * are appended as trailing call operands after the regular operands, in lambda-operand
     * (left-to-right) order. Every lambda's own {@code cap$}-named parameters record how many of
     * the trailing operands belong to it, so code generation ({@code BridgingSqlFunctionCallGen})
     * can partition the trailing operands back to the owning lambda.
     */
    public static RexCall liftCaptures(RexCall call, RexBuilder rexBuilder) {
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

        final List<RexNode> newOperands = new ArrayList<>(operands);
        final List<RexNode> trailingCaptures = new ArrayList<>();
        boolean changed = false;
        for (int pos = 0; pos < operands.size(); pos++) {
            if (!(operands.get(pos) instanceof RexLambda)) {
                continue;
            }
            final LiftedLambda lifted = liftLambda((RexLambda) operands.get(pos), rexBuilder);
            if (lifted == null) {
                continue;
            }
            newOperands.set(pos, lifted.lambda);
            trailingCaptures.addAll(lifted.captures);
            changed = true;
        }
        if (!changed) {
            checkClosed(call);
            return call;
        }
        newOperands.addAll(trailingCaptures);
        final RexCall lifted =
                (RexCall) rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        checkClosed(lifted);
        return lifted;
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
     *       RexOver}) and no sub-query ({@link RexSubQuery}) -- all of these are free variables
     *       that must have been lifted into trailing call operands; and
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
        node.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitLambda(RexLambda lambda) {
                        // Validate this lambda (and, recursively, any nested lambdas in its body)
                        // as one closed scope; do not let the outer shuttle descend into the body
                        // again.
                        checkLambdaClosed(lambda);
                        return lambda;
                    }
                });
    }

    private static void checkLambdaClosed(RexLambda lambda) {
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
                            public RexNode visitLambda(RexLambda nestedLambda) {
                                // A nested lambda is its own closed scope; validate it recursively
                                // rather than resolving its references against this lambda's
                                // parameters.
                                checkLambdaClosed(nestedLambda);
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
    private static LiftedLambda liftLambda(RexLambda lambda, RexBuilder rexBuilder) {
        final List<RexLambdaRef> ownParams = lambda.getParameters();
        final Set<Integer> ownParamIndices =
                ownParams.stream().map(RexLambdaRef::getIndex).collect(Collectors.toSet());

        // Collect the distinct free variables of the body in first-seen order, without descending
        // into nested (already-closed) lambdas: an outer column reference, a reference to a
        // parameter bound by an enclosing lambda (a RexLambdaRef whose index is not one of this
        // lambda's own parameters), or a whole OVER window / sub-query.
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
                                if (!ownParamIndices.contains(lambdaRef.getIndex())) {
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
                            public RexNode visitLambda(RexLambda nestedLambda) {
                                // A nested lambda's parameter references are closed over its own
                                // parameters, not free variables of this lambda; do not descend.
                                return nestedLambda;
                            }
                        });

        // Fast path: nothing to lift and the parameters are already in 0-based positional form.
        if (captures.isEmpty() && isPositional(ownParams)) {
            return null;
        }

        // Renumber this lambda's own parameters to 0..k-1 (declaration order) and append one new
        // parameter per capture after them, numbered from CAPTURE_PARAM_INDEX_BASE so that the
        // captures stay recognizable and the lambda cannot be merged with one that captures
        // differently. The resulting lambda is closed and in positional form.
        final Map<Integer, RexLambdaRef> renumberedOwnParams = new LinkedHashMap<>();
        final List<RexLambdaRef> newParameters = new ArrayList<>();
        int nextParamIndex = 0;
        for (RexLambdaRef param : ownParams) {
            final RexLambdaRef renumbered =
                    new RexLambdaRef(nextParamIndex++, param.getName(), param.getType());
            newParameters.add(renumbered);
            renumberedOwnParams.put(param.getIndex(), renumbered);
        }
        final Map<Object, RexLambdaRef> captureToParam = new LinkedHashMap<>();
        int captureOrdinal = 0;
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
        // An OVER window or sub-query has no name of its own, so it is named after its position
        // among this lambda's captures. The embedded "$" keeps the name distinct from a captured
        // column ("cap$<column index>", always digits) and, in practice, from a captured enclosing
        // parameter ("cap$<parameter name>").
        return CAPTURE_PARAM_PREFIX + (node instanceof RexOver ? "over$" : "sub$") + ordinal;
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
}
