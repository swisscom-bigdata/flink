<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Higher-order functions and lambda expressions: Layer 0 + Layer 1 (the foundation)

| | |
|---|---|
| **Status** | Under discussion |
| **Parent FLIP** | FLIP-XXXX: Higher-order functions and lambda expressions in Flink SQL and Table API |
| **JIRA** | [FLINK-31207](https://issues.apache.org/jira/browse/FLINK-31207) |

> **What this document is.** The parent FLIP specifies the complete design: lambda expressions, a
> `FUNCTION` type, built-in higher-order functions, Table API and Python support, lambda arguments
> for user-defined functions, and compiled-plan support. It is delivered in layers, foundation
> first. This document covers only the foundation, checked against the code on this branch:
>
> - **Step 0** (`[FLINK-31207][table-planner] Narrow the generic RAW guard in leastRestrictive`).
> - **Layer 1** (`[FLINK-31207][table] Fix gaps in lambda and array higher-order functions`): the
>   lambda mechanism plus `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE` and `ARRAY_ZIP_WITH`, in
>   SQL, the Java Table API and PyFlink.
>
> **What is not here.** Two later layers, specified by the parent FLIP. Neither removes or reshapes
> anything layer 1 introduces, so a `+1` here commits to nothing a later layer would have to revise.
>
> - **Layer 2:** further built-ins on the unchanged mechanism (see [*Future work*](#future-work)).
> - **Layer 3:** lambda arguments for user-defined functions, with the function-kind guards
>   (`SystemTypeInference`, `BridgingSqlAggFunction`, `StaticArgument`), lambdas in PTF scalar
>   arguments and `udfs.md` (see [*What a `+1` authorizes*](#what-a-1-on-layer-0--1-authorizes)).

**Contents**

- [Motivation](#motivation)
- [Design overview](#design-overview)
- [Public interfaces](#public-interfaces)
- [Proposed changes](#proposed-changes)
- [Step 0: narrowing the generic RAW guard in `leastRestrictive`](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive)
- [Known limitations](#known-limitations)
- [Compatibility, deprecation, and migration plan](#compatibility-deprecation-and-migration-plan)
- [Delivery: how layer 0 + 1 is cut, sized, and reviewed](#delivery-how-layer-0--1-is-cut-sized-and-reviewed)
- [Test plan](#test-plan)
- [Documentation](#documentation)
- [Rejected alternatives](#rejected-alternatives)
- [Future work](#future-work)
- [Calcite fork: what is upstream, what is residual](#calcite-fork-what-is-upstream-what-is-residual)
- [Appendix (non-normative)](#appendix-non-normative)

## Motivation

Flink can build and inspect `ARRAY` values but cannot run a computation over each element inline.
Today that takes `UNNEST` followed by re-aggregation, which is verbose and changes the row structure
of the query. Batch and interactive engines (Spark, Presto/Trino, Snowflake, Databricks) and
streaming SQL (ksqlDB) solve this with *higher-order functions* that take *lambda expressions*.

Calcite added lambda support in CALCITE-3679, and the Calcite version Flink uses already ships the
matching `RexLambda`/`FunctionSqlType` plumbing. Layer 1 makes lambdas a first-class concept in
Flink SQL and the Table API, and adds four array higher-order functions:

```sql
SELECT
  ARRAY_TRANSFORM(vals, x -> x * 10),             -- [1,2,3]        -> [10,20,30]
  ARRAY_FILTER(vals, x -> x > 1),                 -- [1,2,3]        -> [2,3]
  ARRAY_REDUCE(vals, 0, (acc, x) -> acc + x),     -- [1,2,3], 0     -> 6
  ARRAY_ZIP_WITH(vals, ns, (x, y) -> x + y)       -- [1,2,3],[10,20,30] -> [11,22,33]
FROM my_table;
```

### Naming in other engines

The names and shapes follow existing engines (— means no direct equivalent):

| Flink | Spark / Databricks | Trino / Presto | Snowflake | ksqlDB |
|---|---|---|---|---|
| `ARRAY_TRANSFORM` | `transform` | `transform` | `TRANSFORM` | `TRANSFORM` |
| `ARRAY_FILTER` | `filter` | `filter` | `FILTER` | `FILTER` |
| `ARRAY_REDUCE` | `aggregate` (alias `reduce`) | `reduce` | `REDUCE` | `REDUCE` |
| `ARRAY_ZIP_WITH` | `zip_with` | `zip_with` | — | — |

- **`ARRAY_*` prefix vs. bare names.** The prefix matches Flink's existing array built-ins and
  avoids two clashes: `TRANSFORM` with Hive script-transform syntax, and `FILTER` with the SQL
  `FILTER (WHERE …)` aggregate clause (see [*Rejected alternatives*](#rejected-alternatives)).
- **Lambda syntax.** Spark, Trino, Snowflake and Flink (via Calcite) write `x -> body`; ksqlDB
  writes `x => body`.
- **Snowflake** added `FILTER` and `TRANSFORM` in May 2024 and `REDUCE` later that year.
- **ksqlDB** puts the accumulator first in its reduce lambda, as Flink's `(acc, x)` does.
- **Trino/Presto `reduce`** and Spark's `aggregate` also take a separate output (finish) function:
  `reduce(array, initialState, inputFunction(S,T,S), outputFunction(S,R))`. Flink's `ARRAY_REDUCE`
  has no finish step; wrapping the call in another expression gives the same result.
- **Lambda arity.** No engine compared here ships a lambda with more than three parameters. Only
  engines whose UDFs receive a lambda through a *fixed set* of functional interfaces state a bound:
  ksqlDB ("Currently, ksqlDB supports up to three arguments in a single lambda function"), whose
  UDFs recognise `Function`, `BiFunction` and its own `TriFunction`. Spark's grammar accepts
  `(a, b, c, d) -> …`; its cap shows only outside SQL (three `createLambda` overloads and PySpark's
  `1 <= arity <= 3` check). Trino has no cap: each function supplies its own functional interface.
  Snowflake documents arity per function and allows lambdas only in built-in higher-order functions.
  Flink bounds it at four, like ksqlDB ([*Known limitations*](#known-limitations)).

## Design overview

A **lambda** is an anonymous function `params -> body` that a higher-order function applies to
elements. It is not a storable value: the receiving function gets a short-lived object and applies
it in its own loop. To carry lambdas through validation, inference, planning and the Table API
expression tree, layer 1 adds a **`FUNCTION` type** recording the parameter count: a *helper type
for planning and the API* that marks an operand as a lambda and fixes its runtime representation. It
is **not** a value type: using it for a column, a persisted return type or state is rejected.

The parameter count selects the conversion class and is all the type carries. Parameter and result
types matter only for validation and inference; `CallContext#getLambdaArgument(int)` exposes them as
a `LambdaInfo`. The argument-only helper types `DESCRIPTOR` and `SYMBOL` work the same way: neither
has parameters, a descriptor's column list is read through `CallContext#getArgumentValue`, and a
symbol is itself the value. This keeps the new root's blast radius at the level of `DESCRIPTOR` (see
[*Rejected alternatives*](#rejected-alternatives)).

### Which representation is public, and which is serialized

| Representation | Lives in | Visibility | In a compiled plan |
|---|---|---|---|
| `FunctionType` / `DataTypes.FUNCTION(n)` (a `LogicalType`/`DataType`) | `flink-table-common` | **`@PublicEvolving`** | never serialized |
| `FunctionRelDataType` (a Calcite `RelDataType`) | `flink-table-planner` | `@Internal` | never serialized |
| `RexLambda` / `RexLambdaRef` (the planner IR node) | planner IR (Calcite) | not user-facing | **this is what is serialized** |

**The `FUNCTION` *type* is never serialized, in either representation.** A compiled plan stores the
`RexLambda` (parameter references and body), which `RexBuilder.makeLambdaCall` rebuilds with its
body's type on deserialization. A lambda operand's `FUNCTION` type is not part of the planner's
expression tree, so neither `LogicalTypeJsonSerializer` nor `RelDataTypeJsonSerializer` has a
`FUNCTION` case (see [*Compiled plan*](#compiled-plan-flink-table-planner)). Since the planner-side
type is internal anyway, plan serialization argues neither for nor against a public type.

The first row is public because two public contracts need it, and Table API programs and function
type inference compile against `flink-table-api-java` and `flink-table-common`, which do not depend
on `flink-table-planner` (see [*Rejected alternatives*](#rejected-alternatives)):

- `ResolvedExpression#getOutputDataType` must not return null, so the resolved, `@PublicEvolving`
  `LambdaExpression` has to return a real `DataType`.
- `CallContext#getArgumentDataTypes()` gives an `InputTypeStrategy` a `DataType` for *every*
  argument position, including the lambda's; a `LambdaInputTypeStrategy` recognises it by
  `LogicalTypeRoot.FUNCTION`.

## Public interfaces

### What a `+1` on layer 0 + 1 authorizes

This list collects the permanent commitments of the normative sections below; step 0 adds none.

**Fifteen new `@PublicEvolving` types.**

- `org.apache.flink.table.expressions.LambdaExpression` (`flink-table-api-java`)
- `org.apache.flink.table.expressions.UnresolvedLambdaExpression` (`flink-table-api-java`)
- `org.apache.flink.table.types.logical.FunctionType` (`flink-table-common`)
- `org.apache.flink.util.function.Function0`, `Function1`, `Function2`, `Function3`, `Function4`
  (`flink-core-api`): the external representation and conversion classes of a lambda argument, one
  per arity. The JDK cannot name this range: it has no function interface above two parameters, and
  `Supplier` names its method `get()`, not `apply()`. `Function1`–`Function3` extend
  `java.util.function.Function`, `java.util.function.BiFunction` and Flink's `TriFunction` without
  adding anything, so those three remain accepted conversions. In layer 1 the family types the Table
  API builder methods; no user code receives a `FunctionN` at runtime until layer 3.
- `org.apache.flink.table.data.FunctionData0`…`FunctionData4` (`flink-table-common`): the internal
  representation and conversion classes, one per arity, each with a single `apply` over internal
  data; the layer-1 built-ins receive their lambda through them. Like `ArrayData` and `RowData`,
  they are public because a function reading internal data must name them in its `eval` signature.
- `org.apache.flink.table.types.inference.LambdaInfo` (`flink-table-common`)
- `org.apache.flink.table.types.inference.LambdaInputTypeStrategy` (`flink-table-common`): a
  sub-interface of `InputTypeStrategy`. Its one added abstract method,
  `getExpectedLambdaParameterTypes(CallContext, int)`, binds lambda parameter types for the
  expression resolver and the planner. It is **the only way to declare a lambda argument**; the
  layer-1 built-ins use it.

**One promotion.** `org.apache.flink.table.expressions.LocalReferenceExpression` moves from
`@Internal` to `@PublicEvolving`, because `LambdaExpression.getParameters()` returns it (see
[*Compatibility*](#compatibility-deprecation-and-migration-plan)).

**No factory vocabulary.** Layer 1 adds no public `LambdaTypeStrategies` holder and no
`LambdaParameterTypeStrategy` interface; the public `LambdaInputTypeStrategy`, which both binding
paths consult, suffices. The `@Internal` `InputTypeStrategies` holder (38 factories, most unrelated
to this work) is **not** promoted. Factories can be added later, additively.

**Additions to existing public types.**

- The enum constant `org.apache.flink.table.types.logical.LogicalTypeRoot.FUNCTION`
- The default method `LogicalTypeVisitor#visit(FunctionType)`
- The default method `CallContext#getLambdaArgument(int pos)`
- `org.apache.flink.table.api.DataTypes.FUNCTION(int parameterCount)`, for zero to four parameters
- The constant `FunctionType.MAX_CONVERTIBLE_PARAMETER_COUNT`, with value `4`
- The constants `org.apache.flink.table.functions.BuiltInFunctionDefinitions.ARRAY_TRANSFORM`,
  `ARRAY_FILTER`, `ARRAY_REDUCE` and `ARRAY_ZIP_WITH`, public through their `@PublicEvolving`
  holder; they build the Table API calls and identify the functions in resolved expressions. Their
  type, `BuiltInFunctionDefinition`, stays `@Internal`.

**Four SQL function names**, reserved as built-ins: `ARRAY_TRANSFORM`, `ARRAY_FILTER`,
`ARRAY_REDUCE`, `ARRAY_ZIP_WITH`. **Four Table API methods**: `arrayTransform`, `arrayFilter`,
`arrayReduce`, `arrayZipWith`. **Four PyFlink methods**: `array_transform`, `array_filter`,
`array_reduce`, `array_zip_with`. **Lambda syntax itself**: `x -> expr` and `(x, y) -> expr`, valid
only as an argument of a higher-order function.

**One behavioural commitment:** [*the lambda object contract*](#the-lambda-object-contract) in the
`LambdaInputTypeStrategy` Javadoc; it binds built-ins now and user code, unchanged, from layer 3.

**What layer 1 does not commit to.** Lambda arguments are **restricted to built-in functions**. A
user-defined function of any kind whose input type strategy implements `LambdaInputTypeStrategy` is
rejected during validation on both surfaces. Scalar, table and aggregate functions get:

> Invalid lambda expression at position 1. Lambda arguments are currently only supported by
> built-in functions, but 'my_function' is a user-defined function.

Process table functions are rejected earlier, on their static signature: a `FUNCTION`-typed static
argument trips the new `StaticArgument` entry guard
([*Type system*](#type-system-flink-table-common)). Lifting the restriction is purely additive and
is what layer 3 does, so these parts of the parent FLIP are left to a later vote, if any:

- the function-kind rule: scalar, table and process table functions may host a lambda; aggregate,
  table-aggregate and asynchronous functions may not
- `StaticArgument.scalar(name, DataTypes.FUNCTION(n), false)` for process table functions, which the
  `StaticArgument` [entry guard](#type-system-flink-table-common) rejects in layer 1
- default implementations of the three methods `LambdaInputTypeStrategy` inherits, making it a
  functional interface; adding them later is source- and binary-compatible

### 0. Exact signatures

Every member listed above, as declared on the branch:

```java
// flink-table-api-java — org.apache.flink.table.expressions
@PublicEvolving
public final class LambdaExpression implements ResolvedExpression {
    @Internal
    public LambdaExpression(List<LocalReferenceExpression> parameters, ResolvedExpression body);
    public List<LocalReferenceExpression> getParameters();
    public ResolvedExpression getBody();
    public DataType getOutputDataType();          // FUNCTION(getParameters().size());
                                                  // the body's type is getBody().getOutputDataType()
    public List<ResolvedExpression> getResolvedChildren();
}

@PublicEvolving
public final class UnresolvedLambdaExpression implements Expression {
    public UnresolvedLambdaExpression(List<String> parameterNames, Expression body);
    public List<String> getParameterNames();
    public Expression getBody();
}

// Promoted from @Internal. The constructor stays package-private.
@PublicEvolving
public class LocalReferenceExpression implements ResolvedExpression {
    public String getName();
}

// flink-table-common — org.apache.flink.table.types.inference
@PublicEvolving
public final class LambdaInfo {
    public LambdaInfo(
            @Nullable Expression body, List<Field> parameterFields, DataType returnDataType);
    /** Throws IllegalStateException if the body is not available in the current phase. */
    public Expression getBody();
    public boolean hasBody();
    public List<Field> getParameterFields();
    public Field[] getParameterFieldsArray();
    public DataType getReturnDataType();
}

@PublicEvolving
public interface LambdaInputTypeStrategy extends InputTypeStrategy {
    Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos);
}

// flink-table-common — org.apache.flink.table.types.inference.CallContext
default Optional<LambdaInfo> getLambdaArgument(int pos);   // Optional.empty() by default

// flink-core-api — org.apache.flink.util.function
// The FunctionN family. Every member is a conversion class of the FUNCTION type. Arities 1..3
// extend the existing JDK/Flink types and add nothing, so those types remain accepted as
// equivalent conversions.
@PublicEvolving
@FunctionalInterface
public interface Function0<R> extends Supplier<R> {
    R apply();
    @Override default R get() { return apply(); }
}
@PublicEvolving
@FunctionalInterface
public interface Function1<T1, R> extends Function<T1, R> {}
@PublicEvolving
@FunctionalInterface
public interface Function2<T1, T2, R> extends BiFunction<T1, T2, R> {}
@PublicEvolving
@FunctionalInterface
public interface Function3<T1, T2, T3, R> extends TriFunction<T1, T2, T3, R> {}
@PublicEvolving
@FunctionalInterface
public interface Function4<T1, T2, T3, T4, R> { R apply(T1 t1, T2 t2, T3 t3, T4 t4); }

// flink-table-common — org.apache.flink.table.data
@PublicEvolving @FunctionalInterface
public interface FunctionData0 { Object apply(); }
@PublicEvolving @FunctionalInterface
public interface FunctionData1 { Object apply(Object arg0); }
@PublicEvolving @FunctionalInterface
public interface FunctionData2 { Object apply(Object arg0, Object arg1); }
@PublicEvolving @FunctionalInterface
public interface FunctionData3 { Object apply(Object arg0, Object arg1, Object arg2); }
@PublicEvolving @FunctionalInterface
public interface FunctionData4 { Object apply(Object arg0, Object arg1, Object arg2, Object arg3); }

// flink-table-common — org.apache.flink.table.types.logical
@PublicEvolving
public final class FunctionType extends LogicalType {
    public static final int MAX_CONVERTIBLE_PARAMETER_COUNT = 4;
    /** Rejects any count outside 0..MAX_CONVERTIBLE_PARAMETER_COUNT with a ValidationException. */
    public FunctionType(int parameterCount);
    /** Internal: skips the convertibility check for the lifted-capture arities that the type
      * parser and the planner's type factory can produce. */
    @Internal
    public static FunctionType ofUncheckedArity(int parameterCount);
    /** Internal: the arity-indexed internal conversion class, with FunctionData1 as a placeholder
      * for a lifted-capture arity outside 0..MAX_CONVERTIBLE_PARAMETER_COUNT. */
    @Internal
    public static Class<?> internalConversionClass(int parameterCount);
    public int getParameterCount();
}

// flink-table-common — org.apache.flink.table.api.DataTypes
public static DataType FUNCTION(int parameterCount);

// flink-table-common — org.apache.flink.table.types.logical.LogicalTypeRoot
FUNCTION(LogicalTypeFamily.EXTENSION)

// flink-table-common — org.apache.flink.table.types.logical.LogicalTypeVisitor<R>
default R visit(FunctionType functionType);   // visit((LogicalType) functionType) by default

// flink-table-common — org.apache.flink.table.functions.BuiltInFunctionDefinitions
// The holder is @PublicEvolving; the field type BuiltInFunctionDefinition stays @Internal.
public static final BuiltInFunctionDefinition ARRAY_TRANSFORM;
public static final BuiltInFunctionDefinition ARRAY_FILTER;
public static final BuiltInFunctionDefinition ARRAY_REDUCE;
public static final BuiltInFunctionDefinition ARRAY_ZIP_WITH;
```

`getParameterCount()` is the only public accessor for the arity. Both conversion-class mappings,
external and internal, are private lookup tables in `FunctionType` indexed by arity, so adding an
arity costs two table entries. `LogicalTypeUtils.toInternalConversionClass` delegates to the
`@Internal` `FunctionType.internalConversionClass(int)`, which is not part of the committed surface.

The four Table API DSL methods on `BaseExpressions<InType, OutType>`, with their exact generics:

```java
public OutType arrayTransform(Function1<OutType, OutType> lambdaFunction);
public OutType arrayFilter(Function1<OutType, OutType> lambdaFunction);
public OutType arrayReduce(InType initialValue, Function2<OutType, OutType, OutType> lambdaFunction);
public OutType arrayZipWith(InType array, Function2<OutType, OutType, OutType> lambdaFunction);
```

Here `Function1` and `Function2` are *builder-side* interfaces, not runtime conversion classes: each
is applied once, when the call is built, to construct the body expression. As they extend the JDK
types, Java lambdas, method references and Scala closures target them like the supertypes.

### 1. Lambda expressions

**SQL** (Calcite syntax, Calcite 1.41):

```
x -> expression
(x, y) -> expression
```

**Table API** (Java). A host-language lambda maps parameter expressions to the body expression:

```java
$("vals").arrayTransform(x -> x.times(10));
$("vals").arrayFilter(x -> x.isGreater(1));
$("vals").arrayReduce(lit(0), (acc, x) -> acc.plus(x));
$("vals").arrayZipWith($("ns"), (x, y) -> x.plus(y));
```

**Python Table API**:

```python
col("vals").array_transform(lambda x: x * 10)
col("vals").array_filter(lambda x: x > 1)
col("vals").array_reduce(lit(0), lambda acc, x: acc + x)
col("vals").array_zip_with(col("ns"), lambda x, y: x + y)
```

A lambda is valid only as an argument of a higher-order function, and anywhere else it is an
error. In SQL the parser rejects a lambda outside an argument position, because the grammar
accepts `->` only there. A lambda passed to a function that does not accept one at that position
parses, and the validator rejects it. In the Table API the expression resolver rejects both.

**Expression API.** A lambda is a node of the public expression tree (`UnresolvedLambdaExpression`,
`LambdaExpression`). Since `LambdaExpression#getParameters()` returns
`List<LocalReferenceExpression>`, `LocalReferenceExpression` is promoted from `@Internal` to
`@PublicEvolving` ([*Compatibility*](#compatibility-deprecation-and-migration-plan)).

### 2. The `FUNCTION` type

- A new `LogicalTypeRoot.FUNCTION`, a `FunctionType(parameterCount)` that carries no payload beyond
  the parameter count (the shape of `DescriptorType` and `SymbolType`), and a
  `LogicalTypeVisitor.visit(FunctionType)` default method, all `@PublicEvolving`.
- A new `DataTypes.FUNCTION(parameterCount)` factory.
- The serializable string form is `FUNCTION(n)`, which round-trips through `LogicalTypeParser`.
  The parser gains a `FUNCTION` keyword (so an unquoted field or type name `function` must be
  quoted; see [*Compatibility*](#compatibility-deprecation-and-migration-plan) item 7) and reads the
  count `n` as a *required* precision: a bare `FUNCTION` names no type, and `asSerializableString()`
  always writes the count. Apart from the keyword and a small required-precision rule
  (`parseRequiredPrecision`), it needs no new token type, nested-type production or `->` syntax (see
  [*Rejected alternatives*](#rejected-alternatives)). The `FUNCTION`
  type itself is never written into a compiled plan (see [*Design overview*](#design-overview)).
- The lambda's parameter types and result type are **not** part of the type. They are read from
  `CallContext#getLambdaArgument(pos)` (see
  [§5](#5-declaring-a-lambda-argument-lambdainputtypestrategy-lambdainfo-callcontext)).
- Conversion classes by parameter count. Externally they are `Function0`…`Function4`, with
  `Function`, `BiFunction` and `TriFunction` accepted as equivalents for arities one to three.
  Internally they are the new `@PublicEvolving` `FunctionData0`…`FunctionData4`.
- Nullability is canonicalized: a lambda is never a runtime value, so `copy(boolean)` ignores its
  argument and an explicit `NOT NULL` in the string form is accepted and ignored (see
  [*Compatibility*](#compatibility-deprecation-and-migration-plan) item 6).
- Guards. The type cannot be a table column type, a persisted function return type, or a state
  type, and each of these is rejected with a clear message.

### 3. Built-in higher-order functions

Four new built-in system functions, each a `SCALAR` `BuiltInFunctionDefinition` with a
`LambdaInputTypeStrategy` input strategy and a `BuiltInScalarFunction` runtime class (see
[appendix A3](#a3-code-generation-mechanics)). Their names become reserved built-in names (see
[*Compatibility*](#compatibility-deprecation-and-migration-plan) for a colliding user function).

| Function | Signature | Result |
|---|---|---|
| `ARRAY_TRANSFORM` | `(ARRAY<E>, E -> R)` | `ARRAY<R>` |
| `ARRAY_FILTER` | `(ARRAY<E>, E -> BOOLEAN)` | `ARRAY<E>` |
| `ARRAY_REDUCE` | `(ARRAY<E>, A, (A, E) -> A)` | `A` |
| `ARRAY_ZIP_WITH` | `(ARRAY<E1>, ARRAY<E2>, (E1, E2) -> R)` | `ARRAY<R>` |

**ARRAY_TRANSFORM** preserves element order. **ARRAY_FILTER** keeps an element only if the
predicate is `TRUE`; `FALSE` and `NULL` exclude it.

**ARRAY_REDUCE** is a left fold over the array, in order, starting from the initial value. `A` is
the initial value's type or a lossless widening of it
([*`ARRAY_REDUCE` accumulator typing*](#array_reduce-accumulator-typing)). Each reducer result is
cast to `A`, and an incompatible body is rejected during validation on both surfaces. The call
returns `A`, nullable if the array, the initial value or the body is nullable. A body that can
produce `NULL` also makes the accumulator parameter nullable, so on both surfaces a reducer that
branches on `acc IS NULL` sees an accumulator that an earlier iteration set to `NULL`.

```sql
ARRAY_REDUCE(ARRAY[1, 2, 3], 0, (acc, x) -> acc + x)              -- 6         INT
ARRAY_REDUCE(ARRAY['a', 'b', 'c'], '', (acc, x) -> acc || x)     -- 'abc'     STRING
```

**ARRAY_ZIP_WITH** passes the elements at each position to the lambda. The shorter array is padded
with `NULL`, so the result length is `max(len1, len2)`.

```sql
ARRAY_ZIP_WITH(ARRAY[1, 2], ARRAY[3, 4, 5], (x, y) -> COALESCE(x, 0) + y)   -- [4, 6, 5]
```

NULL handling:

| Input | ARRAY_TRANSFORM | ARRAY_FILTER | ARRAY_REDUCE | ARRAY_ZIP_WITH |
|---|---|---|---|---|
| array `NULL` | `NULL` | `NULL` | `NULL` | `NULL` if **either** array is `NULL` |
| `NULL` element | body applied; `NULL`→`NULL` (kept) | predicate `NULL` → excluded | passed to merge lambda | padded position → `NULL` passed for that side |
| empty | empty array | empty array | initial accumulator | empty array (both empty) |

**A mutable body result is never aliased.** A body that produces a mutable value (`ARRAY[...]`,
`ROW(...)`) gives a **distinct** result per element: `ARRAY_TRANSFORM(a, x -> ARRAY[x])` returns
one array per element, never *N* references to the last one. This is guaranteed for every buffered
body result (`ARRAY_TRANSFORM`, `ARRAY_ZIP_WITH`, `ARRAY_REDUCE`'s accumulator), which is copied
with the result type's serializer ([appendix A3](#a3-code-generation-mechanics)). It is pinned by
`HigherOrderFunctionsBatchITCase#testMutableLambdaResultsAreNotAliased` and
`ArrayReduceAccumulatorOwnershipITCase`.

#### `ARRAY_REDUCE` accumulator typing

The reducer's result becomes the next accumulator, so it has to fit the accumulator's type, but the
initial value is usually typed more narrowly than what the fold builds up. A string literal is a
fixed-length `CHAR(n)`, so `''` is `CHAR(0)`, and `acc || x` does not fit in it. `0` is an `INT`,
and adding a `DECIMAL(3, 2)` element to it gives a `DECIMAL(13, 2)`. `ARRAY[0]` is an
`ARRAY<INT NOT NULL>`, and `ARRAY[x]` over a nullable element is an `ARRAY<INT>`. Layer 1 neither
takes the initial value's type literally nor types every accumulator broadly
([*Rejected alternatives*](#rejected-alternatives)):

> `ARRAY_REDUCE` uses the initial value's type as the accumulator type when the reducer's result is
> assignable to it. Otherwise Flink widens the accumulator once, without loss and within the same
> type family, to the smallest stable supertype of both, and checks again. If the result still does
> not fit, the call is rejected with a hint to cast.

In steps, for an initial value of type `I`:

1. **Keep.** Bind the accumulator parameter to `I` and resolve the body, giving `R`. If `R` is
   *assignable* to `I`, `I` is the accumulator type. Assignable means that the common type of `R`
   and `I` is `I` itself, computed with Flink's `LogicalTypeMerging.findCommonType` on both
   surfaces. Top-level nullability is ignored. So an `INT` body is assignable to a `BIGINT` or a
   `DECIMAL(12, 2)` accumulator, but not to a `DECIMAL(10, 2)` one, because an `INT` needs 10
   integer digits.
2. **Widen once.** Otherwise take the common type `C` of `I` and `R`. Widening is allowed only if
   `C` stays in `I`'s *type family*: both are character strings, binary strings, exact numerics, or
   approximate numerics; or they share the type root, and each pair of children (of an `ARRAY`, a
   `ROW`, or another constructed type) stays in its family. So an `INT` may become a `DECIMAL` but
   not a `DOUBLE`, and a `DATE` does not become a `TIMESTAMP`. `C` is then widened further, to the
   bound the fold converges to. An accumulator that has to grow once usually grows again on the
   next iteration (a concatenation, the precision of a decimal sum), so a supertype that is just
   large enough for one step would fail the re-check:

   | `I` and `R` meet in | Accumulator type |
   | --- | --- |
   | a character string (`CHAR`, `VARCHAR`) | `STRING` |
   | a binary string (`BINARY`, `VARBINARY`) | `BYTES` |
   | `DECIMAL`, including an integer `I` and a decimal `R` | `DECIMAL(38, s)`, where `s` is the scale of `C` |
   | any other type, including the other numeric types | `C` (`TINYINT` → `INT`, `INT` → `BIGINT`, `FLOAT` → `DOUBLE`) |
   | a constructed type | widened per element and field by these rules, keeping `I`'s field names |

   A widening that loses information is not done. Two decimals that together need more than 38
   digits have a common type with a smaller scale than `I`'s, which would round every accumulated
   value.
3. **Check again.** Bind the accumulator parameter to the widened type, resolve the body again,
   and require its result to be assignable. There is no second widening.

The widened type is the accumulator type in every respect. The initial value is cast to it (unless
the cast only changes a length with the same representation, such as `CHAR(0)` to `STRING`), each
reducer result is cast to it, and it is the result type of the call.

```sql
-- kept: the result fits the initial value's type
ARRAY_REDUCE(ARRAY[1, 2, 3], CAST(0 AS BIGINT), (acc, x) -> acc + x)  -- 6      BIGINT
ARRAY_REDUCE(ARRAY['a', 'b'], 'zz', (acc, x) -> acc)                    -- 'zz'   CHAR(2)
-- widened once, without loss
ARRAY_REDUCE(ARRAY['a', 'b', 'c'], '', (acc, x) -> acc || x)           -- 'abc'  STRING
ARRAY_REDUCE(ARRAY[1.25, 2.50], 0, (acc, x) -> acc + x)                 -- 3.75   DECIMAL(38, 2)
ARRAY_REDUCE(ARRAY[1, 2, 3], CAST(0 AS TINYINT), (acc, x) -> acc + x)   -- 6      INT
ARRAY_REDUCE(ARRAY[1, NULL], ARRAY[0], (acc, x) -> ARRAY[x])            -- [NULL] ARRAY<INT>
-- rejected: INT and DOUBLE are in different families
ARRAY_REDUCE(ARRAY[1, 2], 0, (acc, x) -> acc + x * CAST(0.5 AS DOUBLE))
```

A rejection names the accumulator type (and the initial value's type, if it was widened) and the
type the body returns. It ends with a hint that says which cast fixes the call:

| Why the body does not fit | Hint |
| --- | --- |
| It has no common type with the accumulator | Cast the reducer's result to the accumulator type. |
| The common type is in another family (a `DOUBLE` body for an `INT` accumulator) | Cast the initial value to the common type to accumulate in it, or cast the reducer's result to the accumulator type. |
| The common type would lose information (a decimal sum with more than 38 digits) | Cast the reducer's result to the accumulator type. |
| It needs a second widening | Cast the initial value to the type that widening would give. |

Why this rule:

- **It respects what the user wrote.** A fold whose result fits keeps the initial value's type, as
  ordinary SQL typing would. Widening happens only on evidence: the reducer's resolved body.
- **Widening never changes a value silently.** Being lossless and within a family, a widened fold
  computes what the wider type would, or fails validation with a hint. Crossing families (exact to
  approximate numeric, `DATE` to `TIMESTAMP`) is a semantic choice left to the user's cast.
  Assignability, including at the 38-digit decimal cap, is `findCommonType`'s
  ([*Known limitations*](#known-limitations)).
- **It is bounded and predictable.** One widening and one re-check: inference cannot chase a
  growing type, and a user can see the result type from the initial value and the body.
- **It is identical on both surfaces.** Both reach the rule in
  `ArrayHigherOrderFunctionInputTypeStrategy` through the same two hooks
  ([§5](#5-declaring-a-lambda-argument-lambdainputtypestrategy-lambdainfo-callcontext)). Where
  their own arithmetic types a body differently, the accumulator follows (see
  [*Known limitations*](#known-limitations)).
- **It is local to type inference.** The widened type reaches the plan as the accumulator
  parameter's type and the call's return type; that parameter, not the initial value, is
  authoritative. Nothing in code generation, the runtime or plan serialization depends on how it
  was derived.

### 4. Table API / Python methods

New methods on the expression DSL in Java and Python: `arrayTransform` / `array_transform`,
`arrayFilter` / `array_filter`, `arrayReduce` / `array_reduce`, and `arrayZipWith` /
`array_zip_with`, matching the built-ins above. They are declared once, on `BaseExpressions`.
`BaseExpressionsLambdaCompletenessTest` checks that every built-in definition taking a lambda has a
matching DSL method, so a later higher-order built-in cannot miss its Table API binding.

**No Scala-specific API is added** (FLIP-265). The Scala DSL's `ImplicitExpressionOperations`
extends `BaseExpressions` and inherits the four methods, and SAM conversion adapts a Scala closure
to the `Function1` / `Function2` parameter. That is inheritance, not a commitment: nothing in this
layer is written, tested or documented against the deprecated Scala Table API.

### 5. Declaring a lambda argument: `LambdaInputTypeStrategy`, `LambdaInfo`, `CallContext`

A function declares a lambda argument by using the `@PublicEvolving` `LambdaInputTypeStrategy` as
its input type strategy; in layer 1 only built-in functions may
([*What a `+1` on layer 0 + 1 authorizes*](#what-a-1-on-layer-0--1-authorizes)). Condensed, the
`ARRAY_TRANSFORM` strategy reads:

```java
class ArrayTransformInputTypeStrategy implements LambdaInputTypeStrategy {

    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(2);
    }

    public Optional<List<DataType>> getExpectedLambdaParameterTypes(CallContext call, int pos) {
        // arg1 is a one-parameter lambda bound to the element type of arg0
        return pos == 1
            ? Optional.of(List.of(call.getArgumentDataTypes().get(0).getChildren().get(0)))
            : Optional.empty();
    }

    public Optional<List<DataType>> inferInputTypes(CallContext call, boolean throwOnFailure) {
        final List<DataType> args = call.getArgumentDataTypes();
        if (args.size() != 2
                || !args.get(0).getLogicalType().is(LogicalTypeRoot.ARRAY)
                || !args.get(1).getLogicalType().is(LogicalTypeRoot.FUNCTION)) {
            return call.fail(throwOnFailure, "Expected an ARRAY and a lambda.");
        }
        return Optional.of(args); // unchanged: a lambda argument is never cast
    }

    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(Signature.of(Argument.of("ARRAY"), Argument.of("(x) -> y")));
    }
}

TypeInference.newBuilder()
    .inputTypeStrategy(new ArrayTransformInputTypeStrategy())
    .outputTypeStrategy(call ->
        Optional.of(DataTypes.ARRAY(
            call.getLambdaArgument(1).orElseThrow().getReturnDataType())))
    .build();
```

**`LambdaInfo` depends on the phase, not on the surface.** `CallContext.getLambdaArgument(pos)`
returns an `Optional<LambdaInfo>`. Its `getParameterFields()` and `getReturnDataType()`, which an
output-type strategy needs, are always available; `getBody()` only during specialization
(`SpecializedFunction#specialize`), and it throws otherwise (`hasBody()` tells which). SQL
validation has no `Expression` body, so the Table API withholds it too: type inference, including
`TypeInferenceReturnInference`, sees only the parameter and result types.

**Representation.** A lambda argument's external and internal representations are both conversion
classes of its `FUNCTION` type: the `FunctionN` matching the arity, and
`org.apache.flink.table.data.FunctionDataN` (named like `ArrayType`/`ArrayData`). The
representation belongs to the *receiving function*: one that reads an `ARRAY` argument as
`ArrayData` hands its lambda `StringData`, not `String`. The built-ins get the internal form from
the existing "internalize every argument" step of `BuiltInScalarFunction`. A declaration that
disagrees with the `eval` method fails the planner's `eval`-signature check, like any argument.

**`LambdaInputTypeStrategy` is the cross-surface contract.** `ResolveCallByArgumentsRule` (Table
API) and `TypeInferenceOperandChecker` (SQL) both dispatch on `instanceof LambdaInputTypeStrategy`
and call `getExpectedLambdaParameterTypes` the same way, before the body is resolved. A strategy
that does not implement it cannot accept a lambda at any position, which is why it is public.
Implementing it *is* the declaration; there is no separate step that one surface could consult and
the other forget. A strategy that follows the rule below binds the same types on both surfaces.

**A strategy reads only the arguments that precede the lambda.** The parameter types of the lambda
at position *n* may be derived only from the arguments at positions 0 to *n* − 1, in signature
order (named arguments are first mapped to their declared positions; no layer-1 built-in declares
any). The reason is the Table API resolver: it reorders named arguments into signature order
(`executeAssignment`) and resolves the arguments from left to right, so when it reaches a lambda
only the preceding ones are in its call context. SQL validates the operands first and passes all
of them, but the contract does not promise the later ones. A strategy that read them would work in
SQL but fail in the Table API, either on an index or with a report that the parameter types cannot
be derived from the preceding argument types.

All four built-ins follow the rule:

| Function | Lambda position | Parameter types derived from |
| --- | --- | --- |
| `ARRAY_TRANSFORM`, `ARRAY_FILTER` | 1 | the element type of argument 0 |
| `ARRAY_REDUCE` | 2 | argument 1 (the accumulator) and the element type of argument 0 |
| `ARRAY_ZIP_WITH` | 2 | the element types of arguments 0 and 1 |

Spark and Trino also write the collection before the lambda that reads it. The narrower rule is
the safe one: narrowing it after release would break strategies that read later arguments, while
widening it, as [*Future work*](#future-work) describes, is compatible.

A derived list may hold at most four types. Both binding paths reject a longer one with a
validation error from `LambdaTypeValidation` (see [*Known limitations*](#known-limitations)). An
empty list is valid and means a zero-parameter lambda. `Optional.empty()` means the types cannot be
derived, and the call fails with a signature error.

Two further steps are **not** part of the public contract. Only `ARRAY_REDUCE` needs them:

- **Refining a parameter type once the body is known.** `ARRAY_REDUCE` widens the accumulator
  parameter once if the body's result does not fit (see
  [*`ARRAY_REDUCE` accumulator typing*](#array_reduce-accumulator-typing)) and makes it nullable
  if the body is. After resolving the body, both surfaces ask for refined parameter types and,
  while the strategy returns some, re-bind them and resolve the body again. Refinement must be
  monotonic, and is bounded by `RefinableLambdaInputTypeStrategy.MAX_PARAMETER_TYPE_ADJUSTMENTS`
  (2: one widening, one nullability change). Not converging by then is an internal error
  (`TableException`), not a validation error, because no user input can cause it.
- **Requiring a body result type.** The reducer body only has to be *assignable* to the
  accumulator, so the framework wraps it in a `CAST` to the accumulator type before compiling it.
  `FunctionType` carries only the parameter count, so the strategy supplies that target type.

Both hooks live on the `@Internal` `RefinableLambdaInputTypeStrategy extends
LambdaInputTypeStrategy`, which only the built-in array strategy implements; a plain
`LambdaInputTypeStrategy` declares its parameter types once and never coerces a body. A refined
list whose arity changed, which only a broken internal strategy can produce, fails clearly on both
surfaces with the same `LambdaTypeValidation` error ("Invalid input type strategy of function '…'.
The refined lambda parameter types at position … must have … parameter(s) but … were returned."),
not on an index or by silently ignoring a surplus.

#### The lambda object contract

The function object is a handle on an expression the framework compiled, bound to one invocation
of `eval` and the thread running it. The table is the normative, **behavioural** contract between
the framework and a higher-order function, shipped as the Javadoc of `LambdaInputTypeStrategy`. In
layer 1 it binds only Flink's built-ins; layer 3 extends the same text to user-defined functions.

| Aspect | Contract |
|---|---|
| **Lifetime** | The object is valid **only for the duration of the `eval` invocation it is passed to**. A function **must not** retain it. That means: do not store it in a field, in state, or in any structure that outlives the call; do not return it; do not pass it to an object that outlives the call; do not serialize it (it is a generated class with no serialized form). Behaviour after `eval` returns is undefined, and the framework may reuse or invalidate anything behind the object. |
| **Identity** | A fresh object is created for **each evaluation of the enclosing call**: per row for a top-level call, per element when the call itself is inside a lambda body. Object identity carries no meaning across rows and **must not** be used as a cache key. |
| **Captures** | Every captured value (outer column, enclosing lambda parameter, aggregate, `OVER` window, sub-query result) is evaluated **before** `eval` is entered and bound behind the object. All applications within one `eval` invocation therefore see the same snapshot of captures. |
| **Invocation count and order** | The function owns its loop. It **may** apply the object zero, one, or any number of times, in any order, with any values of the declared parameter types. The framework **never** applies the lambda on the function's behalf, so a body with a side effect or a failure only runs if the function applies it. |
| **Arguments and result** | `apply` accepts and returns the conversion classes declared for the lambda argument. An argument **may** be `null` only if the declared type of that parameter (`LambdaInfo#getParameterFields`) is nullable. The compiled body trusts the declared types, so `null` for a `NOT NULL` parameter is undefined behaviour: the body skips the null check for it, so the result is a `NullPointerException` or a wrong value. A function that may pass `null`, for example to pad the shorter of two inputs, **must** declare that parameter nullable. `apply` returns `null` (SQL `NULL`) only if the body's result type (`LambdaInfo#getReturnDataType`) is nullable, and then the function **must not** assume a non-null result. The layer-1 built-ins comply: `ARRAY_ZIP_WITH` declares both parameters nullable because it pads with `NULL`, and `ARRAY_REDUCE` makes the accumulator nullable as soon as the body can return `NULL`. An external `FunctionN` receives external values. Its internal counterpart `FunctionDataN` receives internal data structures, and a mutable result may be backed by memory the evaluator owns and reuses on the next application. A function that keeps or buffers such an internal result across another application **must deep-copy it first**. |
| **Nesting** | Applications of **different** objects can nest to any depth. A body may contain another higher-order call whose function receives its own object, and the inner application finishes before the outer one resumes. An object is never re-entered from its own body, because SQL has no recursion, and a function **must not** build such a call itself. |
| **Thread affinity** | The object **must** be applied only from the thread that invoked `eval`, and only while that call is on the stack. It is not thread-safe: applications must not overlap, and the object must not be handed to another thread, an executor, a parallel stream, or a `CompletableFuture` callback. |
| **Exceptions** | An unchecked exception (`RuntimeException` or `Error`) raised while the body is evaluated propagates out of `apply` **unchanged**. A checked exception cannot be declared by the functional interface's `apply`, so the generated code wraps it in `FlinkRuntimeException` with the original as its cause. A function **should** let either form propagate, which fails the task like any other function failure. Catching it is allowed, but then the application produces no result, anything the body touched is in an unspecified state, and the enclosing call is not retried. |
| **`open` / `close`** | A lambda object exists only per call. **None** is available in `open(FunctionContext)` or `close()`, and a function **must not** try to obtain, keep, or apply one there. The compiled body is initialized when the enclosing operator opens and closed with it, separately from the function's own lifecycle. A function called *from* a body is created, opened, and closed like one called anywhere else. |
| **Determinism** | The object is exactly as deterministic as the body the caller wrote. A non-deterministic body makes the enclosing call non-deterministic (`RexUtil.isDeterministic` descends into the lambda), so a function **must not** assume that the same arguments give the same result twice. |

**What is checked.** Only the structural rules: which functions may declare a lambda argument, its
arity and its parameter types. Lifetime and thread affinity are **not** checked, because a guard
would sit on the per-element path, so violations can go unnoticed. A retained object stays callable
after `eval` returns, and concurrent applications may seem to work for plain arithmetic but corrupt
the reusable buffers of the call site's single compiled body once the body builds a `ROW` or
`ARRAY` or calls a function. Both rules therefore state undefined behaviour, not a testable error.

### 6. Composition

Lambdas and higher-order calls are ordinary scalar expressions and compose in both directions. A
lambda **body** can call any scalar function, built-in or user-defined, including another
higher-order built-in. A higher-order **call** can appear anywhere a scalar expression can: in
another lambda's body, as an argument of an ordinary function, in `WHERE`, `GROUP BY`, `HAVING` or
`ORDER BY`, or as an aggregate's argument. Nesting has no depth limit. At every level the body can
reference its own parameters, those of *all* enclosing lambdas (passed down one level at a time,
inner names shadowing outer ones), and any value the enclosing query produces for the current row.

The following compositions are supported in layer 1, on both surfaces, and are verified by running
queries and checking the computed result, not the plan (see [*Test plan*](#test-plan)). They use
columns `a ARRAY<INT>`, `a2d ARRAY<ARRAY<INT>>`, `base INT`, and an ordinary (non-higher-order)
user-defined scalar function `my_add`:

| Composition | Example |
|---|---|
| Ordinary function in a lambda body | `ARRAY_TRANSFORM(a, x -> my_add(x, base))` |
| Higher-order built-in in a higher-order built-in body | `ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))` |
| Higher-order call as an argument of an ordinary function | `CARDINALITY(ARRAY_FILTER(a, x -> x > 0))` |
| Higher-order call outside the projection | `... WHERE ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) > 5`, and the same call in `GROUP BY`, `HAVING`, `ORDER BY`, or under `SUM(...)` |
| Row-level construct over outer columns in a body | `ARRAY_TRANSFORM(a, x -> x + SUM(base)) ... GROUP BY a`; likewise an `OVER` window, or a scalar sub-query (SQL only: it has no Table API form) |

A user-defined scalar function in a lambda body is an ordinary call inside generated code: created,
opened and closed with the operator, and evaluated once per application
(`HigherOrderFunctionsBatchITCase#testNonDeterministicLambdaBodyIsEvaluatedPerCall`). Its `open()`
gets the operator's `FunctionContext` (job parameters, task information, metric group) at any
nesting depth; the first two are checked by
`#testUserDefinedFunctionInLambdaBodyIsOpenedWithOperatorContext`. In a watermark expression pushed
into a source, it gets the source's metric group
(`#testUserDefinedFunctionInPushedDownWatermarkLambdaUsesSourceMetricGroup`). With
`table.exec.udf-metric-enabled`, its calls feed the same per-function metrics as calls outside a
body (`UdfMetricsITCase#testLambdaBodySharesOneHandle`).

The parent FLIP's composition rows in which a *user-defined higher-order* function hosts, or is
hosted by, a built-in one belong to layer 3. Their mechanism (capture lifting hop by hop, one
generated function object per lambda) is in layer 1, exercised by built-ins nesting in each other.

What does not compose is the short list in [*Known limitations*](#known-limitations), each case
rejected during validation. Chief among them: a lambda is an **argument**, not a value (see
[*Public interfaces*](#public-interfaces)), so it cannot be stored, returned or passed on.

## Proposed changes

Everything below that is not listed in [*Public interfaces*](#public-interfaces) is internal
(`FunctionRelDataType`, `HigherOrderFunctionUtil`, `LambdaFunctionFactory`, the code-generation and
planner-rule changes) and carries no compatibility promise.

### Type system (`flink-table-common`)

- `LogicalTypeRoot.FUNCTION`, `FunctionType(parameterCount)`,
  `LogicalTypeVisitor.visit(FunctionType)`, `DataTypes.FUNCTION(parameterCount)`, and guards that
  prevent use as a column, as state, or as a persisted type.
- `CallContext#getLambdaArgument(pos)` returns the `LambdaInfo`, which carries what the type does
  not. As a `default` method returning `Optional.empty()`, it keeps existing implementations of the
  `@PublicEvolving` `CallContext` compiling. The three contexts behind a real higher-order call
  override it: the Table API resolver's, `CallBindingCallContext` (SQL validation) and
  `OperatorBindingCallContext` (return-type inference and code generation).
- The framework's *wrapping* contexts delegate rather than answer "no lambda" for a call that has
  one: `CastCallContext`, and the split context of `SubsequenceInputTypeStrategy` with its position
  shift (future-proofing: no built-in combines `compositeSequence()` with a lambda argument).
  `UnknownCallContext` answers empty, so a strategy declaring a lambda argument fails to infer.
- `LambdaTypeValidation` (`@Internal`) holds the arity checks shared by both binding paths (zero to
  four parameters; a refinement keeps the arity). `LogicalTypeChecks.hasFunctionType` finds a
  `FUNCTION` type nested anywhere inside a type, for the materialization guards below.

#### `LogicalTypeRoot.FUNCTION`: exhaustive-switch audit

Every exhaustive `switch` over `LogicalTypeRoot` must account for a new root, permanently. Every
such site was audited against one principle: `FUNCTION` is a planning-only helper type,
**intercepted before any internal data path**, and never a column, field, state or serialized
internal value. At runtime it is only a short-lived `FunctionData0`…`FunctionData4` or
`Function0`…`Function4` object passed to `eval` without an internal serializer, and no plan stores
the type ([*Design overview*](#design-overview)), so the type serializers have no `FUNCTION` case.

**What the principle commits to.** Five choke points reject the type with a clear message, and
together they make it unreachable everywhere else:

- `InternalSerializers` covers every runtime materialization of a value. It is therefore the last
  line of defence for "not a column", "not a state type" and "not a persisted value" alike.
- `DefaultSchemaResolver` covers the declared type of any column (physical, metadata or computed),
  also nested, because a schema is persisted long before a serializer is built.
- `TypeInferenceUtil#inferOutputType` covers a declared **return** type.
- `TypeInferenceUtil#inferStateInfo` covers a declared **state** type.
- `LogicalTypeCasts` covers every cast to or from the type, implicit or explicit. Only the
  identity (a function type of the same arity) is compatible.

Two **entry guards**, both pinned by `UserDefinedFunctionLambdaArgumentTest`, reject earlier a
declaration that would let something other than a lambda stand where the type expects one:

- `StaticArgument` rejects a `FUNCTION` type in a static signature (an `eval` parameter, an
  `@ArgumentHint`, or `StaticArgument.scalar(...)`), top-level or nested. Only an input type
  strategy derives a lambda's parameter types; a static `FUNCTION` argument would let a `NULL`, or
  an omitted optional argument, reach code generation where a lambda is expected.
- `ValueLiteralExpression` rejects a `FUNCTION` type for a literal, for example
  `nullOf(DataTypes.FUNCTION(1))` in the Table API, since a lambda cannot be a value.

The built-in higher-order functions do not trip the `StaticArgument` guard. After specialization,
`BuiltInScalarFunction#getTypeInference` restates a call with a `FUNCTION` argument as an input
type strategy (an `InputTypeStrategies.sequence` of `explicit` strategies) instead of as
`typedArguments`, which build `StaticArgument`s. The strategy also carries the lambda's
representation, which hands the function object internal data. Other built-ins are unchanged.

`leastRestrictive` is the one exception on these otherwise unreachable paths:
[*Step 0*](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive) *narrows* it, without
opening it, so that an unresolved lambda parameter (plain `ANY`) defers to a later validation pass
while a mismatch between generic RAW types still fails fast.

[*Appendix A2*](#a2-logicaltyperootfunction-exhaustive-switch-audit-site-by-site) sorts every site
into four categories with the tests that pin it, and explains why each guard sits where it does
rather than one layer later and what the remaining `switch` sites do with a stray `FUNCTION`.

### SQL planner (`flink-table-planner`)

- **No Calcite operator and no operand type checker of their own.** Each of the four functions is
  a `BuiltInFunctionDefinition` in `flink-table-common` with declarative input and output type
  strategies and a runtime class. SQL reaches it through `BridgingSqlFunction` and the shared
  `TypeInferenceOperandChecker`, like every other bridged built-in. The rules below are therefore
  properties of the *type strategies*, enforced the same way on both surfaces and shared where the
  shapes match (`ArrayHigherOrderFunctionInputTypeStrategy`, `ArrayZipWithInputTypeStrategy`).
  - `ARRAY_TRANSFORM` and `ARRAY_FILTER` bind the element type to the single lambda parameter.
    `ARRAY_TRANSFORM` allows any body type and returns `ARRAY<body-type>`. `ARRAY_FILTER` requires
    a `BOOLEAN` body and returns the input array type.
  - `ARRAY_REDUCE` binds parameter 0 to the accumulator type and parameter 1 to the element type,
    and returns the accumulator type, which `adjustLambdaParameterTypes` may widen once. The body
    must be assignable to it under `LogicalTypeMerging.findCommonType`, and the initial value is
    cast to it unless only a length changes (see
    [*`ARRAY_REDUCE` accumulator typing*](#array_reduce-accumulator-typing)).
  - `ARRAY_ZIP_WITH` binds the two lambda parameters to the two array element types, both nullable
    because the shorter array is padded, and returns `ARRAY<body-type>`.
- `FunctionType ↔ RelDataType` bridging in `FlinkTypeFactory`, through the new `@Internal`
  `FunctionRelDataType`. In the reverse direction it also accepts Calcite's `FunctionSqlType`.
- `FlinkTypeFactory.leastRestrictive` lets an unresolved lambda parameter (plain `ANY`) defer to
  Calcite's default logic and narrows the guard against mismatched generic RAW types to
  `instanceof GenericRelDataType` (see
  [*Step 0*](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive)).
- The same deferral applies to the operators that *build* a type from their operands instead of
  using Flink type inference: the `ARRAY[...]` and `MAP[...]` value constructors, and `CAST`. A
  body is typed before the enclosing call binds the parameter, so an operand that depends on it,
  even indirectly, is still a placeholder, which `FlinkTypeFactory`'s collection-type factories and
  `SqlTypeUtil.convertTypeToSpec` reject. In that pass `SqlArrayConstructor`/`SqlMapConstructor`
  return `ANY` and `SqlCastFunction.canCastFrom` accepts the placeholder; both recompute when the
  operand checker re-validates the body. The shared predicate
  (`SqlValidatorUtils.containsUnresolvedLambdaParameter`) also looks inside struct fields, array
  components and map key/value types (`ROW(x, 1)` derives as `ROW(ANY, INT)`), and treats Flink's
  encapsulated RAW type (`GenericRelDataType`, also `SqlTypeName.ANY`) as resolved.
- **Deferred inference inside a body.** A function with Flink type inference in a lambda body,
  whether a nested higher-order call or an ordinary built-in such as `IFNULL(param, 0)`, defers its
  operand and return type inference while an enclosing lambda parameter it depends on is still a
  placeholder, and runs it again once the enclosing operand checker binds the parameter
  (`TypeInferenceOperandChecker`, `TypeInferenceOperandInference`, `TypeInferenceReturnInference`).
  Because this guard runs for *every* call backed by type inference, it inspects the operands as
  `SqlCallBinding#operands()` reports them (named arguments permuted, optional ones padded with
  `DEFAULT`) and derives each operand's type from the node, not from its position in the raw call.
- **Deferred field access inside a body.** A top-level lambda's parameters are bound before its
  body is typed. A *nested* lambda's body, and a body in a stand-alone expression (`callSql`, a
  computed column), are also typed once earlier, while the parameter is still the placeholder. A
  field access in that pass has no fields to look in, so it is deferred too:
  - `FlinkSqlNameMatcher.field` returns a placeholder-typed field instead of failing, for an
    identifier path such as `r.n` or `o.items`.
  - `FlinkCalciteSqlValidator.deriveType` returns the placeholder for a `DOT` call on a placeholder
    record, as in `o.items[1].price` or `IFNULL(r, ROW(0)).n`, instead of "Incompatible types".
  - Re-validation with the bound types still fails an unknown field as "Unknown field", which
    `TypeInferenceOperandChecker` passes through as is; for a nested lambda, the enclosing call's
    operand check would otherwise report it as an internal error ("This is a bug").

  Without them, `ARRAY_TRANSFORM(orders, o -> ARRAY_FILTER(o.items, i -> i.qty > 0))` would fail
  with an internal error. The first two act only on a placeholder type, which no query without a
  lambda produces. Pinned by `HigherOrderFunctionsBatchITCase#testFieldAccessInNestedLambdaBody`.
- **Binding failures are validation errors on both surfaces.** An invalid *argument count* is
  reported first, as the usual signature mismatch. A lambda-binding failure is reported without
  the call's argument types, because rendering the synthesized `FUNCTION` type of an operand whose
  parameter is still a plain `ANY` would itself fail. When the strategy returns `Optional.empty()`,
  the lambda is usually not at fault: a preceding argument of the wrong type (a non-`ARRAY` first
  argument of `ARRAY_TRANSFORM`) leaves nothing to derive the parameter types from; a lambda at a
  position for which the function declares none ends up here too. Both surfaces then report the
  expected signature, rendered from `getExpectedSignatures`.

  | Condition | Message |
  | --- | --- |
  | The lambda's parameter count differs from the declared one | SQL: a message that names the position. Table API: the equivalent error from the expression resolver, which does not name the position. |
  | The strategy returns `Optional.empty()` (SQL) | Calcite's "Cannot apply 'ARRAY_TRANSFORM' to arguments of type … Supported form(s): …" |
  | The strategy returns `Optional.empty()` (Table API) | "Invalid input arguments. Expected signatures are: …", with a cause that names the lambda position and the preceding argument types |
  | The function declares no lambda argument at all (SQL) | "Invalid lambda expression at position *n*. Function '*f*' does not accept a lambda expression at any position." |
  | The function declares no lambda argument at all (Table API) | "Function '*f*' does not accept a lambda expression at position *n*." |

- **No planner-side code generation for the four built-ins.** Each is an ordinary
  `BuiltInScalarFunction` reached through `BridgingSqlFunction` → `BridgingFunctionGenUtil`. Its
  loop lives in the runtime class, which copies every non-null buffered body result with the
  internal serializer of its type (`TypeSerializer#copy`; see
  [*Public interfaces → 3*](#3-built-in-higher-order-functions) and
  [appendix A3](#a3-code-generation-mechanics)).
- As a result, `ExprCodeGenerator` keeps no lambda-binding state. A lambda operand is not a runtime
  value, so `visitCall` emits a placeholder for it, and the operand is consumed through
  `CallContext#getLambdaArgument`. `visitLambda` and `visitLambdaRef` are reachable only when a
  lambda escapes a higher-order call, and both throw. A lambda body becomes executable code only in
  `BridgingFunctionGenUtil.generateLambdaFunctionObject` (see
  [*The generated function object*](#the-generated-function-object)).
- `FlinkRexUtil`: renders `RexLambda`/`RexLambdaRef` in plan strings.
- **Closures over outer columns and enclosing lambda parameters** (see
  [*Lambda captures*](#lambda-captures) below). `FlinkSqlLambdaScope`, a Flink-owned subclass of
  Calcite's `SqlLambdaScope`, delegates identifiers that are not parameters to the parent scope
  instead of rejecting them. The copied `SqlToRelConverter.convertLambda`/`lookupExp` resolve a
  reference to an enclosing lambda's parameter to that parameter's `RexLambdaRef`, and a column to
  an input reference of the current row.
- **A lambda parameter shadows a table alias of the same name.** In
  `SELECT ARRAY_TRANSFORM(o.items, o -> o.amount) FROM orders AS o`, the body's `o.amount` is the
  field `amount` of the row-typed parameter `o`. Conversion binds every identifier whose first
  component names a parameter to that parameter, and validation agrees:
  `FlinkSqlLambdaScope.fullyQualify` qualifies such a compound identifier against the lambda scope
  and checks each further component against the fields of the bound parameter type, and
  `FlinkSqlLambdaScope.resolve` hides an enclosing namespace with the parameter's name. Calcite's
  main branch qualifies compound identifiers the same way, but has no equivalent of `resolve`.
- **A type strategy sees a lambda's parameters in declaration order.**
  `CallBindingCallContext.getLambdaArgument` builds the `LambdaInfo` parameter fields from the
  `SqlLambda`'s declared parameter names, looking each type up by name in the lambda's scope. It
  does not read them from the lambda's derived `FunctionSqlType`, for two reasons:
  - Calcite's `SqlLambda.SqlLambdaOperator#deriveType` pairs the declared names, in order, with
    `SqlLambdaScope.getParameterTypes().values()`. That is a `HashMap`, which iterates in hash
    order, so for some pairs of names the derived type lists the parameter types in the wrong
    order. `(x, acc)` is one, and so are many of the generated names (`$lambdaParam$N`) of a Table
    API lambda serialized to SQL. Calcite's `main` branch still does this, and no Calcite issue is
    filed yet.
  - The `FunctionSqlType` digest leaves out the parameter names, so the canonical instance can
    carry the names of another lambda with the same types.

  Conversion is not affected, because `SqlToRelConverter.convertLambda` looks every parameter up
  by name. But `ARRAY_REDUCE` reads its accumulator type from the first parameter field, and any
  other strategy that reads `LambdaInfo` would see the same wrong order; the `(x, acc)` reducer in
  `HigherOrderFunctionsITCase` and the Table API cases that the test base also runs as SQL fail
  without this.
- **`convertLambda` converts the body in the enclosing conversion context**, not just over the
  enclosing inputs: the lambda blackboard inherits the enclosing root, the enclosing `AggConverter`
  when the query aggregates, and the enclosing registered sub-queries. This lets a body reference
  an aggregate or a sub-query of the enclosing query as a plain reference into the `Aggregate`
  output, a shape capture lifting already handles. It is also needed for an ordinary capture to be
  correct: in "agg mode" a group key must resolve against the `Aggregate` output, which matches the
  pre-aggregation row only when the `GROUP BY` order happens to match the input order. Relatedly,
  `FlinkSqlLambdaScope.isParameter`, whose only caller is Calcite's `AggChecker`, also considers the
  parameters of enclosing lambdas of the same query, so a nested body that closes over one is not
  reported as an ungrouped column (see
  [*Residual after both upstream fixes*](#residual-after-both-upstream-fixes)).
- **Validator rejections** (`FlinkCalciteSqlValidator.validateLambda`, messages in Flink's
  `ExtraCalciteResource`): a lambda parameter name with the reserved `cap$` prefix or repeated
  within one lambda (see [*Lambda captures*](#lambda-captures)); a table function, an asynchronous
  scalar function or a Python scalar function anywhere in a body; and an aggregate, an `OVER`
  window or a sub-query *over a lambda parameter*, which exists per element and has no group to be
  evaluated over. Which names in a sub-query count as the parameter is described under
  [*Known limitations*](#known-limitations).

### Lambda captures

A lambda body may reference free variables of the surrounding scope: columns of the surrounding
query, as in `ARRAY_TRANSFORM(arr, x -> x + base)`; for nested higher-order calls, the parameters
of an *enclosing* lambda, as in `ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))`;
and values the enclosing query produces once per row rather than once per element, an `OVER`
window (`RexOver`) or a sub-query (`RexSubQuery`).

Calcite models a `RexLambda` as a *closed* expression, with only parameters and a body and no
place for captured values. Capture lifting (`HigherOrderFunctionUtil.liftCaptures`) therefore
turns each distinct free variable of a body (an outer `RexInputRef`, a free `RexLambdaRef` bound by
an enclosing lambda, or a whole `RexOver`/`RexSubQuery`, whose operands belong to the enclosing
row) into an additional, trailing lambda parameter, and appends it as a trailing operand of the
enclosing call, where it is evaluated in the scope that binds it. The `RexLambda` that reaches code
generation and serialization stays closed. An aggregate over an outer column needs no rule of its
own: conversion has already turned it into a reference into the `Aggregate` output.
So `ARRAY_TRANSFORM(arr, x -> x + base)` becomes `ARRAY_TRANSFORM(arr, (x, cap) -> x + cap, base)`.
And `ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))` becomes
`ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, (x, cap) -> x + cap[1], a))`: the inner call gains
the trailing operand `a`, evaluated in the outer lambda's body, where `a` is bound. A capture from
more than one level out is **passed down one level at a time**: each lambda in between gains it as
a parameter and forwards it inward. Code generation evaluates the trailing operands at the call
site, so each is evaluated at the loop depth of the scope that binds it, and binds the lifted
parameters to those values behind the function object.

Calcite's `RexLambdaRef` index is *positional*: every lambda's first parameter has index `0`, so a
free reference to an enclosing parameter cannot be told apart from the inner lambda's own first
parameter. The front end therefore gives nested lambda parameters distinct (cumulative) indices
during conversion, and `liftCaptures` lifts the free references and renumbers each closed lambda's
parameters, so no IR or serde change is needed. A scope-qualified `RexLambdaRef` belongs upstream
(residual 5 in [*Residual after both upstream fixes*](#residual-after-both-upstream-fixes)).

**A lambda reference is not purely positional.** A lambda's own parameters keep `0, 1, 2, …`, but
its lifted capture parameters are numbered from a reserved base
(`HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE`, 1,000,000), far above the at most four
parameters a lambda declares of its own. The index, not the name, marks a parameter as lifted, and
it has to: `RexLambdaRef#equals` compares only index and type, because lambdas that differ only in
parameter names are the same function, and Calcite deduplicates them when it builds a `RexProgram`.
Without the reserved base, a capturing and a non-capturing lambda could merge into one node
carrying the names of whichever call site was converted first (pinned by
`HigherOrderFunctionsBatchITCase#testLambdasThatDifferOnlyInTheirCapturesAreNotMerged`). The base
also lets every later stage (the operand checker, the `CallContext`, the code generator in
`BridgingSqlFunctionCallGen`) recover from the lambda alone which parameters are lifted.

Lifting runs bottom-up on both surfaces, so an inner call is closed before its enclosing one: in
SQL as a `RelHomogeneousShuttle` in `FlinkPlannerImpl.rel` (before decorrelation and trimming) and
in `FlinkPlannerImpl.rex` for a stand-alone expression, in the Table API in `ExpressionConverter`.
`HigherOrderFunctionUtil.checkClosed` asserts the result, so plan serde needs no special handling.

**Join conditions.** A join's `ON` condition cannot wait for that shuttle. During conversion,
`RelBuilder#join` pushes a predicate that references only one input, or an equi-join operand, into
that input, and the correlated (`LATERAL`) branch of `createJoin` rewrites references to the left
input into correlated field accesses. On the pinned 1.41.0, `RelOptUtil.InputFinder` does not
descend into a lambda body and the re-indexing `RexShuttle`s discard a rewritten one, so both steps
miss a captured column: a predicate that captures the *other* input is pushed into one input and
fails, and one that captures a right-input column is pushed without shifting the capture and
silently reads the wrong column. `FlinkSqlToRelConverter`, a Flink-owned subclass of the vendored
`SqlToRelConverter`, therefore lifts the condition's captures in the protected `createJoin` and
`createAsofJoin` before delegating. The subclass becomes redundant with the 1.43.0 upgrade, once
CALCITE-7497 has fixed the shuttles and CALCITE-6242 the visitor descent. Pinned by
`HigherOrderFunctionsBatchITCase#testLambdaCaptureInJoinCondition`.

**Stand-alone SQL expressions.** A computed column, a watermark expression and the Table API's
`callSql` (PyFlink's `call_sql`) are validated by `validateParameterizedExpression` against a
`ParameterScope` over the table's columns and converted by `SqlToRelConverter.convertExpression`.
Calcite registers validator scopes only for a query, and a `ParameterScope` returns itself as the
scope of every operand, so a lambda would get no `SqlLambdaScope` and, as on Calcite `main`, fail
as a computed column although it works in a `SELECT`. Three pieces fix this:

- `FlinkCalciteSqlValidator` overrides `validateParameterizedExpression` to register the
  expression's outermost lambdas through the protected `registerQuery`, and `validateCall` to
  validate a lambda directly under a `ParameterScope` in its own scope. Both overrides survive the
  Calcite upgrades, since neither release changes the stand-alone path.
- The vendored `convertLambda` block, like CALCITE-6242 upstream, lets the body inherit the whole
  enclosing `nameToNodeMap`, which is how a column resolves without blackboard inputs.
- `FlinkPlannerImpl.rex` lifts captures out of the converted expression.

A computed column such as `c AS ARRAY_TRANSFORM(arr, x -> x + base)` persists the expression
unparsed (`` `ARRAY_TRANSFORM`(`arr`, `x` -> `x` + `base`) ``), which re-parses to the same lambda.
A watermark expression with a lambda works with a `WatermarkAssigner` and when pushed into a source
(`SupportsWatermarkPushDown`), which instantiates the generated generator itself. Pinned by
`HigherOrderFunctionsBatchITCase#testLambdaInStandaloneSqlExpression`,
`#testLambdaInWatermarkExpression` and `#testLambdaInPartitionPredicate`.

**Capturing inside an aggregate argument, a group key or a sort key.** A capturing higher-order
call may sit in an aggregate argument, as in `SUM(ARRAY_REDUCE(arr, 0, (acc, x) -> acc + x * base))`
(also with `DISTINCT` or `FILTER`, and in `HAVING` or `ORDER BY`), in a `GROUP BY` key, or in a
plain `ORDER BY` expression. Aggregate arguments and group keys are converted against the
pre-aggregation row, so the captured column is an input reference of the query's own row, not a
correlation. The validator registers such a lambda under a clause scope of its `SELECT`
(`AggregatingSelectScope` or `GroupByScope`), while the column resolves in the underlying
`SelectScope`, so `SqlToRelConverter.isSameQueryLambdaCapture` steps through those clause scopes,
which never cross a query boundary, before it compares scopes. Pinned by
`HigherOrderFunctionsBatchITCase#testColumnCaptureInAggregateArgument` and
`#testColumnCaptureInGroupBy`.

**Names in a lambda body in `ORDER BY`.** A body resolves names the same way in every clause: its
own parameters and those of enclosing lambdas first, then the columns of the `FROM` clause, then
those of enclosing queries. `ORDER BY` is the one clause whose expressions also see the aliases of
the select list, and it looks those up first. A lambda body in `ORDER BY` does not see them:

- `FlinkSqlLambdaScope` takes the scope below the `OrderByScope` as the lambda's parent, the
  `SelectScope` (or the `AggregatingSelectScope` of an aggregating or `DISTINCT` query). Otherwise
  the `OrderByScope` would resolve `base` to the alias and leave it simple, so even
  `SELECT base FROM t ORDER BY ARRAY_TRANSFORM(a, x -> x * base)[1]` would fail.
- The vendored validator's `ORDER BY` expansion (`OrderExpressionExpander`) leaves a lambda alone,
  as Calcite's `Expander` already does for the select list, `GROUP BY` and `HAVING`. Otherwise it
  would replace a simple body name that matches a select-list alias with the aliased expression and
  rebuild the lambda as an ordinary call, which fails with
  "Unexpected error in type inference logic … This is a bug." Validation qualifies a body's column
  names in place (`base` becomes `t.base`), so with the first change only a parameter is still a
  simple name there, as `x` in `SELECT v AS x FROM q ORDER BY ARRAY_TRANSFORM(arr, x -> x * 2)[1]`.

A body name that `ORDER BY` would read as a select-list alias is accepted only where the alias
stands for the column of that name (as `base` in that query, behind `SELECT *`, or repeated);
otherwise the two readings differ, and the name is rejected rather than silently read as either.
In `SELECT base * 2 AS base FROM t ORDER BY ARRAY_TRANSFORM(a, x -> x * base)[1]` the body's `base`
is the column, while a plain `ORDER BY base` would be the alias; with `SELECT base AS y`, a body
naming `y` names no column at all. Both fail with
"A lambda body in ORDER BY cannot use the select-list alias '…'", and the message suggests the
aliased expression or the column qualified with its table alias (`t.base`). Substituting the
aliased expression is not an option, since a lambda parameter could capture one of its names. A
lambda parameter shadows an alias of its name, as it shadows a column, and an alias outside the
body still works, as the initial value in `ORDER BY ARRAY_REDUCE(a, y, (acc, x) -> acc + x)`.
Pinned by `HigherOrderFunctionsBatchITCase#testLambdaInOrderBy`.

**Capturing across a sub-query boundary (correlation).** A lambda evaluated inside a sub-query may
close over a column of the *enclosing* query, as `t.a` in
`SELECT (SELECT ARRAY_TRANSFORM(a, x -> x + t.a)[1]) FROM t`. At conversion time such a reference
is a *correlated* field access (`$cor.a`), so capture lifting cannot lift it until decorrelation
rewrites it into an input reference of the decorrelated row. On the pinned Calcite 1.41.0,
decorrelation's visitors do not descend into a `RexLambda` body and its shuttles discard a
rewritten body, so the correlation would leak into the plan. Three steps fix this. The two in
vendored Calcite classes are `FLINK MODIFICATION` blocks, most of which the Calcite upgrades delete
(see [*Calcite fork*](#calcite-fork-what-is-upstream-what-is-residual)). Which names inside such a
sub-query refer to a lambda parameter is described under [*Known limitations*](#known-limitations).
Pinned by `HigherOrderFunctionsBatchITCase#testOuterColumnCaptureFromLambdaInScalarSubquery`.

- `org.apache.calcite.sql2rel.RelDecorrelator` is patched to descend into and rewrite lambda
  bodies: its discovery visitor, `DecorrelateRexShuttle` and `RemoveCorrelationRexShuttle` each
  gain a `visitLambda` that rebuilds the lambda from a rewritten body. It also keeps a higher-order
  call's result type, which decorrelation cannot change, when an operand changes; re-deriving it
  for a rewritten lambda operand gives `ANY`, which later fails type conversion.
- The rewrite leaves the capture, now an input reference, un-lifted *inside* the lambda, so a pass
  after decorrelation in Flink's own `FlinkDecorrelateProgram` runs capture lifting again, bottom-up
  into lambda bodies, and restores the closed-lambda invariant; it does nothing for closed lambdas.
- Two or more distinct outer columns captured by one lambda enter as separate correlation
  variables, which `SqlToRelConverter.getCorrelationUse` merges through
  `DeduplicateCorrelateVariables`. Its `RexShuttle` again discards a rewritten lambda body on
  1.41.0, so a secondary variable would survive inside the body and no `Correlate` would form; a
  lambda-aware remap in `getCorrelationUse` finishes the de-duplication inside lambda bodies. As a
  safety net, the leftover-correlation check in `FlinkDecorrelateProgram` also descends into
  lambdas, so an escaping correlation is a clean planning error, not a code-generation crash.

**Lambda-body determinism.** A call is only as deterministic as its lambda body:
`ARRAY_TRANSFORM(a, x -> RAND())` is non-deterministic. Calcite's `RexVisitorImpl.visitLambda`
returns without descending into the body, so the stock `RexUtil.isDeterministic` would treat such a
call as deterministic and expose it to unsafe common-subexpression elimination, predicate
push-down, or result caching. A minimal `FLINK MODIFICATION` patch to the vendored
`org.apache.calcite.rex.RexUtil` visits the lambda body when computing determinism. CALCITE-6242
fixes this upstream in 1.43.0 by making `RexVisitorImpl` recurse when `deep`, which deletes the
patch. Two of Flink's own traversals bypass it, so both descend into `RexLambda` bodies themselves
and, being Flink code, stay after the 1.43.0 upgrade:

- `ShortcutUtils.isDeterministicThroughProgram` resolves `RexLocalRef`s against the enclosing
  `RexProgram`'s expression list, falling back to the patched entry point only without a program.
  `ExprCodeGenerator` consults it before caching a local reference's generated value. Because
  `RexProgramBuilder` collapses structurally identical expressions (lambda parameter *names* are
  not compared), without lambda descent
  `SELECT ARRAY_TRANSFORM(a, x -> nd(x)), ARRAY_TRANSFORM(a, y -> nd(y))` would evaluate the body
  once and return the same array for both columns, while `SELECT nd(1), nd(1)` evaluates twice.
- `FlinkRexUtil.getNonDeterministicCallName` is the entry point of the non-deterministic-update
  (NDU) analysis in `StreamNonDeterministicUpdatePlanVisitor`; without lambda descent, a
  non-deterministic body in a projection or condition would pass the streaming check unreported.

Pinned by `HigherOrderFunctionDeterminismTest`, `NonDeterministicUpdateAnalyzerTest`, and
`HigherOrderFunctionsBatchITCase#testNonDeterministicLambdaBodyIsEvaluatedPerCall` /
`#testNonDeterministicLambdaBodyInFilterAndProjection`.

**`OVER` windows.** On both surfaces, a lifted `OVER` window must not stay in the same projection
as the call. `ProjectToWindowRule` splits a projection that contains a window into a windowed and a
non-windowed part on its flattened `RexProgram`, in which a lambda is an expression of its own, so
the lambda would become a meaningless output field of the non-windowed part (a lambda is only an
argument of the call that applies it). `ExtractLambdaOverWindowRule`, registered in the **batch and
stream** default rewrite rule sets immediately before `ProjectToWindowRule`, therefore moves such
windows into a projection **below** the call and leaves an input reference behind, as if the user
had written the window in a sub-query. A lifted sub-query needs no such rule, because the existing
sub-query rewrite handles a `RexSubQuery` operand.

**Reserved capture names.** A lifted capture parameter is named with the reserved `cap$` prefix
(`HigherOrderFunctionUtil.CAPTURE_PARAM_PREFIX`), and these names are visible in plans. The planner
recognizes a capture parameter by its index (`isCaptureParameter`), never by its name; the
reservation keeps plans unambiguous to read. Because SQL users choose lambda parameter names, the
validator rejects names that begin with the prefix (`FlinkCalciteSqlValidator.validateLambda`,
with the message in `ExtraCalciteResource`). The SQL parser treats `$` as an identifier letter, so
the check applies to an unquoted `cap$x` as well as to a back-quoted one. The Table API DSL
generates its names (`$lambdaParam$N`); the public `UnresolvedLambdaExpression` constructor accepts
any name, which is harmless: such a name can only make a plan harder to read.

**Unique parameter names.** A lambda's own parameter names must be **unique**: both surfaces bind
by name, so a repeated name would let the declared arity disagree with the `FUNCTION` type.
`FlinkCalciteSqlValidator.validateLambda` and the `UnresolvedLambdaExpression` constructor reject
it before resolving the body. Shadowing an *enclosing* lambda's parameter is still allowed.

#### Examples

Given a table `t(arr ARRAY<INT>, base INT, factors ARRAY<INT>)` with one row
`([1, 2, 3], 10, [100, 200])`:

```sql
-- Close over a scalar column: base is a per-row capture, evaluated once per row.
SELECT ARRAY_TRANSFORM(arr, x -> x + base) FROM t;
-- [11, 12, 13]

-- Close over a column inside a predicate.
SELECT ARRAY_FILTER(arr, x -> x < base) FROM t;
-- [1, 2, 3]

-- Nested lambda closing over the ENCLOSING lambda's parameter (f), evaluated once per
-- outer element, and over an outer column (base), evaluated once per row.
SELECT ARRAY_TRANSFORM(factors, f -> ARRAY_TRANSFORM(arr, x -> x * f + base)) FROM t;
-- [[110, 210, 310], [210, 410, 610]]
--   f = 100 -> [1*100+10, 2*100+10, 3*100+10]
--   f = 200 -> [1*200+10, 2*200+10, 3*200+10]

-- An own parameter shadows an enclosing one of the same name (inner x wins).
SELECT ARRAY_TRANSFORM(arr, x -> ARRAY_REDUCE(arr, 0, (acc, x) -> acc + x)) FROM t;
-- [6, 6, 6]   -- inner x iterates arr; the outer x is shadowed and unused
```

A capture is evaluated at the loop level of the scope that binds it: `base` once per row, and `f`,
a trailing operand of the inner call, once per outer element.

### Table API (`flink-table-api-java`, `flink-table-common`)

- An `UnresolvedLambdaExpression` (parameter names plus body) and a resolved `LambdaExpression`
  whose parameters are `LocalReferenceExpression`s and whose output type is a `FunctionType`.
  `LambdaExpression` is `@PublicEvolving` and exposes its parameters through
  `getParameters(): List<LocalReferenceExpression>`, so `LocalReferenceExpression` is promoted to
  `@PublicEvolving` (see [*Compatibility*](#compatibility-deprecation-and-migration-plan)).
- The four functions are the same bridged built-ins as in SQL
  ([*SQL planner*](#sql-planner-flink-table-planner)), so the resolver validates against the same
  strategies.
- The shared strategy makes every argument rule hold on **both** paths, which still differ: SQL
  reaches it through Calcite's validator, the Table API through expression resolution, and each
  derives the lambda's parameter types itself. A rule enforced on one path only would fail at
  runtime or type a call differently on the other, so `HigherOrderFunctionValidationParityTest`
  checks both (see [*Test plan*](#test-plan)). Type merging uses one function on both
  (`LogicalTypeMerging.findCommonType`, see [*`ARRAY_REDUCE` accumulator
  typing*](#array_reduce-accumulator-typing)), and both render a rejection's expected signature
  from the strategy's `getExpectedSignatures`.
- The expression resolver (`ResolveCallByArgumentsRule`) binds lambda parameter types from the
  preceding arguments, the only ones resolved when it reaches the lambda (see
  [§5](#5-declaring-a-lambda-argument-lambdainputtypestrategy-lambdainfo-callcontext)). The body's
  scope holds the lambda's parameters, those of enclosing lambdas, **and** the input columns of the
  surrounding query. Inner parameters shadow outer ones, and parameters shadow columns of the same
  name (`FieldReferenceLookup`). Both kinds of capture are lifted out of the lambda (see
  [*Lambda captures*](#lambda-captures)), and a lambda argument is never wrapped in an implicit
  `CAST`. The scope inherits registered over-windows (`$("base").sum().over($("w"))`). A function
  called by name in a body (`call("my_udf", x)`) is looked up as outside one, because
  `LookupCallResolver` descends into the body that `UnresolvedLambdaExpression` otherwise hides.
- The resolver rejects, with the same wording as SQL, table functions, asynchronous scalar
  functions and Python scalar functions in a body, and an aggregate or `OVER` window over a lambda
  parameter. It also rejects a lambda passed to a user-defined function (the layer-1 restriction).
- Aggregate extraction (`OperationExpressionsUtils`, `OperationTreeBuilder`) descends into lambda
  bodies, which `UnresolvedLambdaExpression` hides from generic traversals. So
  `a.arrayTransform(x -> x.plus($("base").sum()))` means the same as the SQL form: the aggregate is
  hoisted out of the unresolved projection into the enclosing aggregation, and the body captures
  the resulting column, as the SQL path arranges in `convertLambda`. An aggregate over a lambda
  *parameter* has no such reading and is rejected.
- `ExpressionConverter` converts `LambdaExpression` to `RexLambda`. Parameters become
  `RexLambdaRef`s indexed in declaration order, offset by the enclosing lambdas' parameter count so
  nested parameters stay distinguishable, and `liftCaptures` renumbers them. The higher-order call
  is converted like any call (`FunctionDefinitionConvertRule`, `BridgingSqlFunction`,
  `RelBuilder#call`); `ExpressionConverter` then lifts its lambdas' captures, keeping the type
  derived through `TypeInferenceReturnInference`, for which `OperatorBindingCallContext`
  synthesizes the `FUNCTION` argument type and withholds the body (see
  [§5](#5-declaring-a-lambda-argument-lambdainputtypestrategy-lambdainfo-callcontext)).
- DSL methods (`arrayTransform`, `arrayFilter`, `arrayReduce`, `arrayZipWith`) accept a
  host-language lambda through `HigherOrderFunctionCalls`, which generates the parameter names.

### Python (`flink-python`)

- Python `Expression` methods (`array_transform`, `array_filter`, `array_reduce`,
  `array_zip_with`) map a Python callable to the Java lambda expression through the existing
  gateway, and are documented in `expressions.rst`. Python never exposes `FUNCTION` as a
  `DataType`: a lambda is a host-language callable, so no Python type mapping can mishandle it.
- A Python scalar function is rejected inside a lambda body on both surfaces, but can still consume
  or produce the array a higher-order call works on (see [*Known limitations*](#known-limitations)).

### The generated function object

This is the part of the parent FLIP's "User-defined functions" section that layer 1 implements. A
built-in and (from layer 3) a user-defined function get their lambda through the same code path.

- **One place compiles a lambda body.** `BridgingFunctionGenUtil.generateLambdaFunctionObject`
  compiles it through the `RexLambda → RexNodeExpression` bridge into an `ExpressionEvaluator`
  implementing `LambdaFunctionFactory`: `bindLambda(Object[] captures)` unpacks the captures once
  and returns an anonymous functional-interface object whose `apply` casts its arguments and calls
  the typed `eval` directly. No `MethodHandle` means no `Object[]` allocation or adaptation **per
  application**, only one capture array (empty without captures) per function object.
- **The operand is the representation.** The `FUNCTION` operand is a runtime `eval` operand passed
  through without conversion, unlike every other operand. Its type, synthesized by
  `OperatorBindingCallContext` because a `RexLambda` carries only its body type, excludes the lifted
  capture parameters, so the function sees the lambda's declared arity and conversion class. The
  captures stay visible only in the `LambdaInfo` parameter fields used by code generation.
- **Captures are stripped and bound.** `BridgingSqlFunctionCallGen` strips the trailing capture
  operands from the call used for the function's own type inference, evaluates them at the call
  site and binds them behind the function object. An enclosing lambda parameter is not
  loop-invariant, so each value is bound to a fresh local per iteration before the object is
  (re)built. Trailing operands are partitioned per lambda operand from left to right, so several
  lambda arguments work the same way; no layer-1 built-in has more than one, layer 3 does.
- **Where the evaluator lives.** It is opened once in the enclosing generated class: the operator,
  or one of three classes without a runtime context (an enclosing lambda's evaluator, a watermark
  generator pushed into a source, and `ExpressionReducer`, the planner's constant-reduction and
  partition-pruning function). The last two open it with a `FunctionContext` of their own, as for a
  user-defined function; otherwise a lambda in a pushed-down watermark would fail at job start, and
  one in a partition predicate during planning.
- **The evaluator hands its context on.** `openLambdaFactory(FunctionContext, UdfMetrics[])` opens
  it with an `OpenContext` carrying the enclosing class's `FunctionContext` and, with
  `table.exec.udf-metric-enabled`, its `UdfMetrics` handles. A function or nested evaluator in the
  body thus sees the job parameters, task information and operator metric group at any depth. A
  lambda's evaluator never registers a handle: it has no runtime context, and a second one would
  collide with the operator's. A call in a body feeds the enclosing class's handle for its function.
- **How the lambda object contract is made true.** The object is an anonymous conversion-class
  instance **constructed at the call site** after its captures are evaluated into locals, so the
  [*contract*](#the-lambda-object-contract)'s lifetime, identity and capture rules hold by
  construction. Its evaluator is a **single instance per call site**, opened in the enclosing
  class's `open()`, held in a `transient` field and closed as a reusable **close** statement of that
  class; its per-instance state (reused buffers, converters) makes the object single-threaded.
- **Representation.** The body is compiled in the representation that the *receiving function*
  declared for the argument: internal for the built-ins. The lambda itself is
  representation-neutral, and `LambdaInfo` describes it as written.
- **Coercing the body.** When a function needs the body to produce a type of its own choosing, as
  `ARRAY_REDUCE` does for its accumulator, the coercion is a `CAST` wrapped around the body before
  compilation. It folds into the compiled body instead of costing a conversion per application.
- **Mechanics.** Loop placement, the deep copy, method splitting, nested class names and the cost of
  the ABI are in [*A3. Code-generation mechanics*](#a3-code-generation-mechanics).

### Compiled plan (`flink-table-planner`)

- `RexNodeJsonSerializer`/`Deserializer` handle `RexLambda` and `RexLambdaRef`. A lambda is
  serialized as its parameter references (name, index, type) and body. The `RexLambda` carries no
  `FUNCTION` type, so none is serialized: `RexBuilder.makeLambdaCall` rebuilds it with its body's
  type, and `OperatorBindingCallContext` derives the `FUNCTION` type from the parameter list where
  type inference needs it (see [*Design overview*](#design-overview)).
  - **What a plan pins, and what it recomputes.** Reading a plan derives none of a lambda's types
    again: each parameter's index, name and type, the body's result type, the higher-order call's
    result type and the node's output type are written into the plan. Only a lambda operand's
    `FUNCTION` type is derived, and it carries only the arity, which the parameter list pins. A
    change to how a type is *derived* can change new plans, but cannot silently retype an old one.
  - **A serialized lambda is always closed, and its numbering is a documented convention.**
    `RexLambdaRef` is positionally ambiguous only between conversion and capture lifting (see
    [*Residual after both upstream fixes*](#residual-after-both-upstream-fixes)). Before
    serialization, `liftCaptures` lifts and renumbers every lambda and `checkClosed` asserts the
    result, so the format needs no nested or scope-qualified reference and no IR or serde change.
    The numbering of [*Lambda captures*](#lambda-captures) (captures from `1000000` upward, with
    `cap$` names) is visible in a compiled plan and part of its contract.
  - **Restoring re-validates the invariant.** A plan is external input that another version may
    have written, so `RexNodeJsonDeserializer` runs `checkClosed` on every lambda it reads. It also
    checks that each body reference agrees with the name and type declared for its position, and
    that the capture parameters form an unbroken run from the reserved base. A changed numbering
    convention then fails the restore with a clear error instead of binding the wrong parameter.
- `RestoreTestBase` (stream, `CalcRestoreTest`) and `BatchRestoreTestBase` (batch,
  `CalcBatchRestoreTest`) cover `Calc` nodes containing each of the four functions, plus a capturing
  built-in lambda (`calc-array-transform`, `calc-array-filter`, `calc-array-reduce`,
  `calc-array-zip-with`, `calc-array-transform-capture`, in `CalcTestPrograms`), whose lifted `cap$`
  parameters and trailing operands are serialized and restored with it.
  - **What these resources show and do not show.** Layer 1 commits 5 batch plans, 5 stream plans
    and 5 stream savepoints, written *and* read by this version. They show that such a plan survives
    serialization, deserialization and a savepoint restore unchanged. They are **not** evidence of
    restoring a *released* version's plan or state; like every other `ExecNode` version-1 restore
    resource, they become that once a release contains them and the next one restores them.

## Step 0: narrowing the generic RAW guard in `leastRestrictive`

Commit `825bdcd0896` (`[FLINK-31207][table-planner] Narrow the generic RAW guard in
leastRestrictive`): 4 production lines, 257 test lines, files `FlinkTypeFactory.java` and
`FlinkTypeFactoryTest.java`.

### The change

```java
// before
if (types.stream().anyMatch(t -> t.getSqlTypeName() == SqlTypeName.ANY)) {
    throw new TableException("Generic RAW types must have a common type information.");
}
// after
if (types.stream().anyMatch(t -> t instanceof GenericRelDataType)) {
    throw new TableException("Generic RAW types must have a common type information.");
}
```

`FlinkTypeFactory.leastRestrictive` decides whether Flink imposes its own RAW-type reasoning or
defers to Calcite. The old condition does not match its own message: Flink's encapsulated RAW type
is `GenericRelDataType`, and every `GenericRelDataType` is `SqlTypeName.ANY`, but not every `ANY` is
a `GenericRelDataType`. Calcite's contract is the opposite of the old guard:
`SqlTypeFactoryImpl.leastRestrictiveSqlType` states *"if any of the inputs are ANY, the output is
ANY"*. `ANY` is the top type, and a least-restrictive computation over it succeeds.

### Why lambdas need it

**The trigger is the unresolved lambda *parameter*, not the `FUNCTION` type.** In the first
validation pass of a body, a lambda parameter has the plain `SqlTypeName.ANY` placeholder Calcite
uses for every not-yet-known type. The old guard rejects that `ANY` with a RAW-type message before
the second pass can bind the parameter. `FunctionRelDataType` is `SqlTypeName.OTHER` and never
matched the old guard. The narrowing is **necessary**: with the old guard restored, the lambda IT
cases fail with an internal *"Unexpected error in type inference logic of function
'ARRAY_TRANSFORM'. This is a bug."*, not a clean validation error.

### Why it is safe

- **Behaviour-preserving for every RAW type.** Of the types Flink converts from a `LogicalType`,
  `GenericRelDataType` is the only one with `SqlTypeName.ANY`. `RawRelDataType`,
  `FunctionRelDataType` and `BitmapRelDataType` are `SqlTypeName.OTHER`, and
  `StructuredRelDataType` is `STRUCTURED`. A RAW type meeting an unrelated type still fails fast
  instead of silently producing an untyped `ANY` column.
- **The premise is pinned by tests:** `FlinkTypeFactoryTest#testEveryLogicalTypeRootIsAudited` and
  `#testOnlyGenericRawTypesMapToSqlTypeNameAny` convert a representative of **every**
  `LogicalTypeRoot` through `FlinkTypeFactory.createFieldTypeFromLogicalType` and fail the build if
  any root, including a future one, maps to `SqlTypeName.ANY`; the modern `RawType` maps to
  `RawRelDataType` (`OTHER`). They also pin the only `ANY` producer, the legacy
  `TypeInformationRawType`, to a `GenericRelDataType`.
- **Not a no-op for a Calcite-native basic `ANY`.** The old guard rejects such a call with
  *"Generic RAW types must have a common type information."*; the new one defers to Calcite's
  default `leastRestrictive`, which normally succeeds, so such a query can now plan and produce an
  `ANY`-typed column. No query that planned before changes its result, and a native `ANY` meeting
  a RAW type still fails with the same message (`#testLeastRestrictiveDefersForCalciteNativeAny`,
  `#testLeastRestrictiveStillFailsWhenNativeAnyMeetsRawType`). The RAW-type behaviour is pinned by
  `#testLeastRestrictiveFailsForDifferentRawTypes`, `#testLeastRestrictiveResolvesIdenticalRawTypes`
  and `#testLeastRestrictiveReturnsNullForIncompatibleNonRawTypes`.
- **No existing query reaches the difference.** Flink does create plain `ANY` types on master:
  `SqlFirstLastValueAggFunction` (`FIRST_VALUE`/`LAST_VALUE` operand inference),
  `SqlWindowTableFunction`, `SqlMLEvaluateTableFunction`, `SqlVectorSearchTableFunction`,
  `TypeInferenceOperandChecker#toParamType` for untyped table arguments, and `SqlItemOperator` for
  an `ANY`/`DYNAMIC_STAR` operand. Each produces it as *operand metadata* (`paramTypes`, the
  deprecated `getParameterTypes`/`getReturnType`, signature and named-argument handling) or derives
  it from an operand that is already `ANY`; none puts it into a result type that reaches
  `leastRestrictive`, so the old guard's over-reach is latent, not user-visible.
- **An unresolved dynamic parameter `?` is not affected.** `SqlValidatorImpl` types a node that is
  not yet inferred with `typeFactory.createUnknownType()`, which Flink already overrides to
  `UnknownSqlType extends BasicSqlType(SqlTypeName.NULL)` (an existing `FLINK MODIFICATION` in the
  vendored `SqlTypeFactoryImpl`). Dynamic parameters never reach this guard, before or after.
  Pinned by `#testUnknownTypeIsNotSqlTypeNameAny`.

### Measured blast radius

The old guard was restored on the full prototype branch and the `flink-table-planner` test corpus
re-run, to measure rather than argue that existing queries cannot see the narrowing:

| Suite | Tests run | Failures | Where |
| --- | ---: | ---: | --- |
| Unit tests (`default-test`) | 11,428 | **1** | `FlinkTypeFactoryTest#testLeastRestrictiveDefersForCalciteNativeAny`, the test that pins this change |
| `*ITCase` (208 of 226 classes reported; the run was aborted by an unrelated fork OOM) | 10,584 | **30** | lambda ITCases only |

No pre-existing test in the module that is unrelated to lambdas is sensitive to the guard. The
layer-1 lambda ITCases are `HigherOrderFunctionsITCase`, `HigherOrderFunctionsBatchITCase`,
`ArrayReduceAccumulatorOwnershipITCase` and `UdfMetricsITCase#testLambdaBodySharesOneHandle`; the
others on the full branch belong to layer 3. On the step-0 commit alone, with no lambdas present,
the ITCase corpus is green either way.

### How it relates to the lambda placeholder convention

Every other layer-1 change on a path every query goes through is conditioned on an unresolved lambda
placeholder or a `RexLambda`, through `SqlValidatorUtils.isUnresolvedLambdaParameter`, which is
exactly `type.getSqlTypeName() == SqlTypeName.ANY && !(type instanceof GenericRelDataType)`. Its
recursive wrapper `containsUnresolvedLambdaParameter` (which also matches a placeholder nested in a
collection or row type) gates `SqlCastFunction`, `TypeInferenceOperandChecker` (two sites),
`SqlMapConstructor` and `SqlArrayConstructor`. Three direct uses postpone a field lookup on a still
unbound record: `FlinkSqlLambdaScope.validateFieldPath` (checking a field path such as `x.f`),
`FlinkSqlNameMatcher.field` (resolving it) and `FlinkCalciteSqlValidator.deriveType` (a field of a
computed value, see [*Deferred field access inside a body*](#sql-planner-flink-table-planner)).
"Plain `ANY` means unbound lambda parameter" is thus a branch-wide convention, and a reviewer should
check the step-0 audit that keeps it sound, not the call sites: `GenericRelDataType` must remain the
only type converted from a `LogicalType` with `SqlTypeName.ANY`. The `leastRestrictive` guard is
the predicate's complement (it fires *on* `GenericRelDataType`), so the same audit protects both.

### Why it is its own commit, but not its own JIRA

It is the only layer-1 change on a path every query goes through that **cannot** be conditioned on
a lambda: when the guard runs, there may be no lambda in scope yet. As its own commit it is a small
diff (one changed condition), a clean `git bisect` point, and what lets the rest of layer 1 claim
that *every remaining* change on such a path is conditioned on a lambda. It is not a standalone bug:
the over-reach is latent, so a JIRA would have no user-visible bug for a release note, and a
backport would change a release branch for no benefit. Its compatibility note belongs in
FLINK-31207's release notes, since the difference is observable only once lambdas exist. No
configuration switch is proposed (see
[*Delivery*](#delivery-how-layer-0--1-is-cut-sized-and-reviewed)).

## Known limitations

- **A lambda is applied through a function object, not inlined into the caller's loop**
  ([*The generated function object*](#the-generated-function-object)). The cost is the
  `Object`-typed ABI, not the call: boxing in and out of `apply` costs a primitive element type 32
  bytes per element. A built-in runs at 1.08×–1.31× of the rejected inlined loop for `ARRAY<INT>`
  with a trivial body, at 1.03×–1.14× with an expensive one, and at 0.96×–1.07× for `ARRAY<STRING>`
  ([A3.3](#a33-what-the-lambda-abi-costs)); the remaining headroom is larger than that gap and
  reachable without inlining ([A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)).
  The shared runtime loop's `apply` site turns megamorphic, at a measured cost that levels off. Not
  measured: method size for a large body, and the JIT behaviour of deeply nested lambdas. A body
  far beyond `table.generated-code.max-length` still splits and compiles
  (`HigherOrderFunctionsBatchITCase#testLargeLambdaBodyIsSplitIntoSeparateMethods`).
- **`FUNCTION` is not a storable type.** It cannot be a column, a persisted return type or a state
  type, and it cannot be nested inside one of those. Every one of those checks is recursive.
- **Lambdas support at most four parameters.** The bound is a *representation* limit: a
  `FUNCTION(n)` converts to `FunctionDataN` internally (and to `FunctionN` externally, for users
  from layer 3 on), and those families stop at 4. Zero parameters is supported, because Calcite's
  grammar (`SimpleIdentifierOrListOrEmpty`) already accepts `()` and nothing downstream needs a
  lower bound.
  - Both binding paths (`ResolveCallByArgumentsRule` for the Table API,
    `TypeInferenceOperandChecker` for SQL) check the types returned by
    `getExpectedLambdaParameterTypes(...)` with the same `@Internal` `LambdaTypeValidation` helper,
    so the two cannot drift apart, and reject more than four with "A lambda argument must have
    between 0 and 4 parameters, but *n* parameter types were derived. …".
  - `DataTypes.FUNCTION(n)` and the public `FunctionType` constructor apply the same bound with the
    same wording ("…but a FUNCTION type with *n* parameters was requested. …"), so the public API
    cannot construct the type in a state whose conversion class does not exist.
  - A lambda with lifted captures can have more than four parameters, and two internal paths
    construct such a type: `FlinkTypeFactory.toLogicalType`, converting a Calcite `FunctionSqlType`
    whose field count includes the `cap$` parameters, and `LogicalTypeParser`, reading back the
    `asSerializableString()` form (`FUNCTION(5)`) that every logical type must round-trip through.
    Both use the `@Internal` factory `FunctionType.ofUncheckedArity(int)`, which skips only the
    convertibility check and still rejects a negative count. `OperatorBindingCallContext` also
    calls it, after filtering out the lifted capture parameters by their reserved index rather than
    their `cap$` name. A compiled plan never stores the `FUNCTION` type, so it is no such path.
  - Four is one above the widest built-in the full feature delivers (three lambda parameters, a
    later layer; layer 1's widest takes two). Raising it is **source-compatible and
    additive**: the arity-to-conversion-class mapping is a lookup table indexed by arity, so a new
    arity means one new interface per family plus table entries. Nothing in the planner, code
    generation or compiled-plan format depends on arity (a prototype at arity 22 changed none of
    them; see [*Rejected alternatives*](#rejected-alternatives)). The Python API has one small
    adapter per arity its DSL methods use (`_LambdaFunction1`, `_LambdaFunction2`).
- **A lambda body is a scalar expression over the row it is evaluated in.** It runs once per
  element and may reference its own parameters, those of enclosing lambdas, and any value the
  enclosing query produces for the current row: a column, an aggregate, an `OVER` window or a
  sub-query. It may not evaluate such a row-level construct over a lambda **parameter**, and three
  kinds of function cannot appear in it at all:
  - **Not expressible: aggregating over a lambda parameter.** An aggregate folds a set of *rows*,
    while a lambda parameter is an array element within a single row, so
    `ARRAY_TRANSFORM(a, x -> SUM(x))` has no meaning. Folding an array's own elements is
    `ARRAY_REDUCE(a, 0, (acc, x) -> acc + x)`. The same applies to an `OVER` window partitioned or
    ordered by a parameter, and to a sub-query correlated on one (`x -> x IN (SELECT …)`). Each is
    rejected during validation with its own message, for example "Aggregate functions over a
    lambda parameter are not supported in the body of a lambda expression. …" ("OVER windows
    over …", "Subqueries over …"). SQL names the offending parameter; the Table API does not,
    because its lambda parameters have generated names that would mean nothing to the reader. A
    parameter of a lambda nested *inside* the construct is bound within it, so
    `x -> x + SUM(ARRAY_REDUCE(a, 0, (acc, y) -> acc + y))` is accepted.
  - **Not expressible: table functions.** They produce rows, not a value. They are always rejected:
    "Table functions are not supported in the body of a lambda expression".
  - **Not expressible: asynchronous scalar functions.** An `AsyncScalarFunction` completes a future
    after its `eval` returns, while a lambda body must produce its value in place, and the call
    cannot be hoisted out of a per-element body. It is rejected in every Calcite version, whether
    or not it references the parameter.
  - **Not expressible: Python scalar functions.** `PythonCalcSplitRule` moves a projection's Python
    calls into a separate operator that hands whole rows to the Python worker, which works once per
    row but not per element. Such a call is rejected on both surfaces, whether or not it references
    the parameter.

  Outside a lambda body, asynchronous and Python functions still combine with a higher-order call:
  they can produce its input or consume its result. Everything else is supported on both surfaces
  (a sub-query has no Table API spelling).
  `ARRAY_TRANSFORM(a, x -> x + SUM(base)) … GROUP BY a` evaluates `SUM(base)` once per group, and
  the body sees the result. The same holds for `SUM(base) OVER ()` and for a scalar sub-query,
  plain or correlated, including inside a nested lambda. The construct resolves to a reference
  into the enclosing row (the `Aggregate` output, or a projection below the call for an `OVER`
  window), which capture lifting hoists out of the lambda like any other capture, giving the plan
  of the hand-written workaround:

  ```sql
  SELECT ARRAY_TRANSFORM(a, x -> x + s) FROM (SELECT a, SUM(base) AS s FROM t GROUP BY a)
  ```

  The grouping rules still apply to the rest of the expression: without a `GROUP BY`,
  `SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t` is rejected because the aggregate is then
  global and `a` is not grouped, as for `SELECT a, SUM(base) FROM t`. This places Flink at the
  permissive end among engines. Trino excludes the whole class ("Subqueries are not supported …
  Aggregations are not supported"). Spark 3.5.3 evaluates outer-column aggregates, `OVER` windows
  and (correlated) scalar sub-queries in a lambda body, as Flink does, but reports
  `transform(a, x -> SUM(x))` as a missing `GROUP BY` and, once one is added, fails at runtime with
  a `NullPointerException`, where Flink rejects the call cleanly (repro in the parent FLIP's
  appendix A4).
- **Inside a sub-query in a lambda body, an unqualified name that matches a lambda parameter is
  that parameter.** The check that rejects a sub-query over a lambda parameter
  (`FlinkCalciteSqlValidator`, `findLambdaParameterReference`) is syntactic and errs only towards
  rejection. It follows the sub-query's own declarations only where that is unambiguous:
  - A name that is only declared is not a reference: an `AS` alias and its column list, a `WITH`
    item's name and column list, and a table name in `FROM`.
  - A qualified name `q.c` is not a reference if `q` is a table alias of a `SELECT` of the
    sub-query that encloses it, because the validator and the converter resolve it innermost scope
    first. So `x -> x + (SELECT MAX(q.x) FROM (VALUES (10), (20)) AS q(x))` and
    `x -> x + (SELECT MAX(x.base) FROM t AS x)` are accepted, as are a join of such aliases and a
    `WITH` item named like the parameter.
  - Any other name whose first component is a parameter is a reference, including an unqualified
    name that is also a column of the sub-query:
    `x -> x + (SELECT MAX(x) FROM (VALUES (10), (20)) AS q(x))` fails with "Subqueries over a
    lambda parameter are not supported …". The message
    (`unsupportedSubqueryOverLambdaParameterInLambdaBody`) says that such a name always refers to
    the lambda parameter, and suggests qualifying the sub-query's own column with its table alias
    (`q.x`) or renaming the parameter.

  This is a rule, not a gap: resolved by scope, `x` would mean both the element just outside the
  sub-query and a column inside it, which a reader cannot tell apart, and accepting it later would
  only accept more queries. Where scoping is subtle, the walk stays strict. A derived table's alias
  is not visible outside it, so in `(SELECT MAX(q.v) + x.v FROM (SELECT v FROM … AS x(v)) AS q)`
  the `x.v` is the parameter's field and is rejected. Items of `LATERAL`, `UNNEST` and other calls
  in `FROM` are walked as expressions, so a reference there to a sibling alias named like the
  parameter is rejected too. A lambda nested in such a sub-query sees the sub-query's alias, and
  `FlinkSqlLambdaScope.isParameter` stops at the sub-query, so `x.base` there stays subject to the
  sub-query's `GROUP BY`. Aggregates and `OVER` windows outside a sub-query open no scope, so there
  a matching name is the parameter. `HigherOrderFunctionsBatchITCase#testSubQueryInLambdaBody`
  pins each case except the walk of `LATERAL`, `UNNEST` and other calls in `FROM`, which no test
  covers yet. The user documentation states the rule.
- **A lambda body in `ORDER BY` does not see the aliases of the select list.** A body name that
  `ORDER BY` would read as an alias for anything but the column of that name is rejected with "A
  lambda body in ORDER BY cannot use the select-list alias '…'". The workaround is the aliased
  expression or the column qualified with its table alias (see *Names in a lambda body in
  `ORDER BY`* under [*Lambda captures*](#lambda-captures)).
- **A column merged by a `USING` or `NATURAL` join must be qualified in a lambda body.** Calcite's
  `Expander` rewrites such a column into `COALESCE(t1.base, t2.base)` before names are resolved,
  but leaves a lambda alone, not knowing its parameters. So
  `SELECT ARRAY_TRANSFORM(a, x -> x * base) FROM t1 JOIN t2 USING (base)` fails with "Column 'base'
  is ambiguous", as does such a name in an aggregate argument or a nested lambda (in `WHERE`,
  `HAVING` and `ORDER BY` it is ambiguous even outside a lambda). Write `t1.base` (inner or left
  join), `t2.base` (right join), `COALESCE(t1.base, t2.base)` (full join) or a derived table. This
  is a validation error, never a wrong result, and accepting the name later is additive
  ([*Future work*](#future-work)). Pinned by
  `HigherOrderFunctionsBatchITCase#testCommonJoinColumnInLambdaBody`; the user documentation states
  the rule.
- **Function references are not lambdas.** A lambda argument must be written as a lambda,
  `ARRAY_TRANSFORM(a, x -> my_udf(x))`. A bare `my_udf` at a lambda position is an ordinary
  identifier and is reported as an unresolved column, as in Spark 3.5.3, Trino and ksqlDB.
  Eta-expansion is a separate, additive feature ([*Future work*](#future-work)).
- **Only built-in functions accept a lambda argument in layer 1.** A user-defined function that
  declares one is rejected during validation on both surfaces until layer 3 removes the check (see
  [*What a `+1` on layer 0 + 1 authorizes*](#what-a-1-on-layer-0--1-authorizes)). A user-defined
  function *can* be called inside a lambda body, and can consume or produce the array.
- **A lambda's parameter types come only from the arguments before it**
  ([§5](#5-declaring-a-lambda-argument-lambdainputtypestrategy-lambdainfo-callcontext)). In the
  Table API, a strategy that reads a later argument fails with an index error or with "the lambda
  parameter types cannot be derived from the preceding argument types"; in SQL it happens to work,
  but the contract does not promise it. No layer-1 user can observe this, because only the four
  built-ins can declare a lambda, and each writes it after the arguments it reads. A function
  therefore cannot take its lambda first, as ClickHouse's `arrayMap(func, arr)` does. Lifting the
  rule is compatible ([*Future work*](#future-work)).
- **The optimizer sees less of a lambda body than of an ordinary expression.** On the pinned
  Calcite 1.41.0, two optimizations stop at a lambda; both are missed optimizations, not wrong
  results. A constant sub-expression of a body (the `1 + 2` in `x -> x > 1 + 2`) is evaluated per
  element, because the expression-reduction rules do not look inside a lambda before CALCITE-7497
  (1.42.0). Flink registers Calcite's own `ReduceExpressionsRule` instances, and its reducer
  already compiles lambdas, so folding should arrive with that upgrade without a Flink change. A
  conjunct containing a higher-order call is never offered to `SupportsFilterPushDown` or to any
  other path that hands a connector or catalog an `Expression`, because
  `RexNodeToExpressionConverter` declines a lambda. The conjunct stays in the `Calc` above the
  scan; the other conjuncts are still offered (`testLambdaPredicateIsNotOfferedToSource`).
  Partition pruning is unaffected: the planner evaluates a predicate over partition columns only
  against the partitions the catalog lists, skipping `listPartitionsByFilter`, so a lambda there
  does prune.
- **Plans show the lifted form.** `EXPLAIN` and a compiled plan show a capturing lambda after
  capture lifting, for example `ARRAY_TRANSFORM(arr, (x, cap$1) -> (x + cap$1), base)`. The `cap$`
  numbering is part of the plan format ([*Compiled plan*](#compiled-plan-flink-table-planner)); why
  it is visible and frozen is under [*Rejected alternatives*](#rejected-alternatives).
- **`MULTISET[...]` cannot be used in a lambda body.** Calcite validates a multiset constructor in a
  scope of its own, which resolves a name as a column of a table, never as a lambda parameter, so
  `ARRAY_TRANSFORM(a, x -> MULTISET[x])` fails with "Column 'x' not found in any table". This is
  not worth changing on its own: Flink does not support the multiset value constructor at all, and
  `SELECT MULTISET[1, 2, 3]` fails to plan outside a lambda too, as does `MULTISET[1]` in a body.
- **An `ARRAY_REDUCE` accumulator is widened at most once, losslessly, and within its type family**
  (see [*`ARRAY_REDUCE` accumulator typing*](#array_reduce-accumulator-typing)). A reducer that
  leaves the family (an `INT` accumulator with a `DOUBLE` body) or changes type again after the
  widening needs a cast, and the error's hint says which. For decimals, Flink's addition keeps at
  most 38 digits and at that cap gives up scale, down to 6, to keep integer digits, so a
  `DECIMAL(38, s)` sum is stable only for `s ≤ 6`. For a larger scale every step would round,
  which is not a lossless widening; casting the reducer's result
  (`CAST(acc + x AS DECIMAL(38, 8))`) makes the rounding explicit. Conversely, an *assignable*
  result is accepted, including where `findCommonType` itself gives up scale at the 38-digit cap: a
  `DECIMAL(38, 4)` result fits a `DECIMAL(38, 2)` accumulator and is rounded to scale 2, as it
  would be in a `CASE` or a `UNION` of the two types.
- **In SQL, `FLOAT + DOUBLE` is a `FLOAT`.** Calcite's arithmetic types it as the `FLOAT` operand,
  while the Table API's `plus` gives `DOUBLE`. So `ARRAY_REDUCE(a, CAST(0 AS FLOAT), (acc, x) ->
  acc + x)` over a `DOUBLE` array accumulates in `FLOAT` in SQL and in `DOUBLE` in the Table API.
  This is Calcite's arithmetic, not the accumulator rule, and applies outside a lambda too. A
  `CAST(acc AS DOUBLE)` in the body makes both surfaces widen to `DOUBLE`.

## Compatibility, deprecation, and migration plan

1. **Four new reserved built-in names.** The change is additive for queries that do not use
   `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE` or `ARRAY_ZIP_WITH`, and nothing is
   deprecated. But an unqualified call to a temporary catalog function or a catalog function of
   one of these names may resolve differently after the upgrade. Flink resolves an unqualified
   reference in the order *temporary system → built-in system → temporary catalog → catalog*
   (`FunctionCatalog#resolveAmbiguousFunctionReference`), so the call can bind to the new built-in,
   or fail with a new signature-validation error if its arguments do not match. The migration
   options, each pinned for all four names by
   `FunctionCatalogTest#testBuiltInHigherOrderFunctionNameCollision`, are to rename the function;
   to call it by a fully qualified identifier (`catalog.database.name`), which
   `FunctionCatalog#resolvePreciseFunctionReference` resolves as *temporary → catalog* without
   consulting built-in system functions; or to register a temporary system function of that name,
   which has the highest priority and overrides the built-in for the session.
2. **The `leastRestrictive` guard narrowing** is the one change that affects every query, and it is
   confined to one case: a Calcite-native basic `ANY` that previously threw now defers to Calcite.
   Behaviour is unchanged for every RAW type and for dynamic parameters, and no existing query can
   reach the difference ([*Step 0*](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive)).
3. **New `LogicalTypeRoot.FUNCTION` constant.** `LogicalTypeVisitor.visit(FunctionType)` is a
   source-compatible default method. Per the exhaustive-switch audit
   ([appendix A2](#a2-logicaltyperootfunction-exhaustive-switch-audit-site-by-site)), the type is
   handled on the lambda planning path, rejected with a clear error at the five choke points and
   the two entry guards, and unreachable elsewhere behind throwing defaults. The regenerated SQL
   Gateway REST OpenAPI specs (`rest_v{1..4}_sql_gateway.yml`) gain one `LogicalTypeRoot` enum
   value each, an additive, wire-compatible change.
   - **The new enum constant is binary-additive, but not source-compatible in one narrow case.**
     Compiled third-party code keeps working: no existing constant's signature or ordinal changes,
     and code that reaches an unknown constant takes its `default` branch. On *recompilation*, a
     `switch` **expression** over `LogicalTypeRoot` that lists every constant without a `default`
     fails to compile. This comes with any constant added to a `@PublicEvolving` enum
     (`LogicalTypeRoot` gained `DESCRIPTOR`, `VARIANT` and `BITMAP` the same way), and the fix is
     one `default` branch, which such code needs anyway to stay forward-compatible.
4. **`CallContext#getLambdaArgument(pos)`** is a source-compatible default method. An existing
   implementation of the `@PublicEvolving` `CallContext` keeps compiling and answers "no lambda",
   which is correct for a call that has none. Every framework context that wraps another one
   overrides it to delegate ([*Type system*](#type-system-flink-table-common)).
5. **`LocalReferenceExpression` is promoted from `@Internal` to `@PublicEvolving`.** The new
   `@PublicEvolving` `LambdaExpression` exposes it through
   `getParameters(): List<LocalReferenceExpression>`, and no `@PublicEvolving` API may leak an
   `@Internal` type. This widens the public API in a source-compatible way: no existing member is
   removed or changed. It is verified by the architecture tests
   (`PUBLIC_EVOLVING_API_METHODS_USE_ONLY_PUBLIC_EVOLVING_API_TYPES`). What is committed is kept as
   small as the leak requires. The class remains framework-constructed (its constructor is
   package-private, and its only factory is in the `@Internal` `ApiExpressionUtils`), so the
   guarantee covers reading a parameter's `getName()` and its inherited `ResolvedExpression`
   members, not constructing one. For the same reason, `LambdaExpression`'s own constructor is
   `@Internal`, following the precedent of `CallExpression`, whose constructor and built-in
   `permanent(...)` factory are `@Internal`.
6. **`FUNCTION` nullability is canonicalized.** Nullability means nothing for the `FUNCTION` type,
   since a lambda is never a runtime value, so it is canonicalized to nullable at the `FunctionType`
   boundary: `copy(boolean)` ignores the requested nullability, and `FunctionRelDataType` is always
   nullable and treats `createWithNullability` as a no-op. Every construction path
   (`DataTypes.FUNCTION(n)`, `.notNull()`, `copy(false)`, a parsed `FUNCTION(n) NOT NULL`) therefore
   gives the same logical type, serializable string and `FunctionRelDataType` digest, and a
   nullability bit can never split two otherwise identical lambda arities during coercion or
   inference. The nullability of the lambda's parameter and result types still matters; it lives on
   the `LambdaInfo` from `CallContext#getLambdaArgument(pos)`.
7. **`FUNCTION` becomes a keyword of the type-string parser.** `LogicalTypeParser` matches keywords
   case-insensitively, and neither a field name nor a part of a user-defined type name may be an
   unquoted keyword. So a hand-written type *string* with an unquoted `function` now fails:
   - as a field name (`@DataTypeHint("ROW<function INT>")`, `DataTypes.of("ROW<function INT>")`)
     or a part of a user-defined type name (`DataTypes.of("cat.db.function")`), with
     "<IDENTIFIER> expected but was <KEYWORD>";
   - as a whole user-defined type name, `DataTypes.of("function")`, because it now reads as a
     `FUNCTION` type without its parameter count.

   Every other type keyword behaves the same (`ROW<date INT>`, `ROW<map INT>`, `cat.db.date`), and
   the fix is to quote the name (``ROW<`function` INT>``, ``cat.db.`function` ``, `` `function` ``).
   Persisted types are not affected, because `asSerializableString()` always quotes field names and
   every part of a type name. Nor is SQL DDL, whose parser already reserves `FUNCTION` and so
   rejected `ROW<function INT>` before. Like every other type keyword, `FUNCTION` is not
   context-sensitive. `LogicalTypeParserTest` pins the failing and the quoted forms.
8. **`LambdaInputTypeStrategy` is added without default methods** in layer 1, so an implementation
   overrides all four methods. That is no compatibility concern, because in layer 1 only built-ins
   implement it. Adding default methods later (layer 3) is source- and binary-compatible.
9. **Release notes.** Entries for the four SQL functions, the Table API and PyFlink methods, the
   `FUNCTION` type, the type-string keyword of item 7 (with its quoting workaround) and the step-0
   behaviour change go on layer 1's JIRA subtask; the release manager consolidates them.

## Delivery: how layer 0 + 1 is cut, sized, and reviewed

### Where layer 0 + 1 sits in the whole feature

The prototype branch `flink-31207-higher-order-functions-and-lambdas` implements and tests the full
feature in four commits: step 0 plus one per functional layer. This branch
(`flink-31207-higher-order-functions-and-lambdas-layer01`) carries the first two, one commit each.
Layer 1 also closes gaps a later layer would otherwise close, and its figures include them: the
entry guards, the function context in a lambda body (also in a pushed-down watermark), a sub-query
check that follows the sub-query's own declarations and table aliases, lossless `ARRAY_REDUCE`
widening, and declaration-order lambda parameter types on the SQL path.

| # | Commit | What a user can do that they could not before | main / test / docs |
|---|---|---|---|
| **0** | `825bdcd0896` — narrow the generic RAW guard in `leastRestrictive` | Nothing on its own; no existing query can reach the old guard's over-reach | 4 / 257 / 0 |
| **1** | the layer-1 commit (child of `825bdcd0896`) — lambdas and array higher-order functions | Write a lambda and apply the four array functions to it in SQL, the Java Table API and PyFlink, with captures, nesting, `OVER`/sub-query bodies, compiled plans and savepoint restore | 7,661 / 7,793 / 377 |
| 2 | *(follow-up)* further built-in higher-order functions | More built-ins on the same three surfaces; no new mechanism | 929 / 2,121 / 76 |
| 3 | *(follow-up)* lambda arguments for user-defined functions | Declare a lambda argument on their own scalar, table or process table function | 380 / 6,334 / 595 |

Measured on the branch, each commit against its parent: production lines are Java, Scala and Python
under `src/main` and `pyflink`; test lines all of `src/test` and `pyflink/**/tests`; documentation
all of `docs/` and `flink-python/docs`. Layer 1 touches 158 files, plus this document.

**Why the mechanism and the array functions are one layer.** A lambda needs the type, the front end,
capture lifting, code generation, plan serde and Table API expression resolution, and is reachable
and testable end to end (result-checking IT cases, restore tests, compiled plans) only through a
function that accepts one. A mechanism-only layer would ship public API (`DataTypes.FUNCTION`,
`LambdaInputTypeStrategy`, `LambdaExpression`) without end-to-end tests and could not be released.
The array functions are the smallest family that exercises the mechanism, on both surfaces.

### What layer 0 + 1 satisfies

1. **It compiles and verifies on its own.** It needs no later layer to be correct and is not a
   partial refactor that a follow-up completes. On the branch, each commit builds main *and* test
   sources of the modules it touches, Spotless and Checkstyle clean. Before merging, the pull
   request must also pass `mvn clean verify` on those modules and on all of `flink-table`, Spotless,
   Checkstyle and ArchUnit clean, rebased on current `master`; that gate is still to be passed.
2. **It is functionally complete.** Everything it makes reachable is validated, code-generated,
   serializable in a plan, covered by batch *and* streaming IT cases, registered in the function
   catalog, documented, and in the release notes. No function parses but rejects a captured column,
   works in SQL but is missing from `SHOW FUNCTIONS`, or lacks its restore test.
3. **It is releasable.** Flink can ship after layer 1 with lambdas and array higher-order functions
   in SQL, the Java Table API and PyFlink. Layers 2 and 3 never change what layer 1 made public;
   layer 3's removal of the restriction to built-in functions only widens what is accepted.
4. **It is a slice of the reviewed branch, not a rewrite.** Layer 1 is the prototype's layer-1
   commit plus the gap fixes above. Layers 2 and 3 apply on top of it (verified by cherry-pick) and
   pass all tests. Layer 2 conflicts only where tests sit side by side; layer 3 needs three changes:
   - Its `StaticArgument.checkLambdaType` accepts a non-optional top-level `FUNCTION` static
     argument, which layer 1 rejects, and `SystemTypeInference.checkLambdaArgs` then requires a
     process table function with a `LambdaInputTypeStrategy`; the nested rejection is the same in
     both.
   - It deletes `UserDefinedFunctionLambdaArgumentTest`; the rebase keeps the literal and the nested
     and optional static-argument cases, with layer 3's messages.
   - Three of its assertions, for a strategy that returns `Optional.empty()`, expect the signature
     error described under *Binding failures are validation errors on both surfaces*
     ([*SQL planner*](#sql-planner-flink-table-planner)) instead of "does not accept a lambda
     expression".
5. **It is ordered for review.** Each commit is a buildable, self-consistent tree, so `git bisect`
   and per-commit review both work. They are not squashed on merge.

### Review sub-units

At 7,661 production lines, "reviewers can follow the commit" is not enough. The branch keeps layer 1
as one commit, but for merge it is proposed as **five named sub-units after the step-0 commit**,
each its own pull request under layer 1's JIRA subtask. They merge in order into one feature branch
that merges to `master` as a unit, so no release can contain the mechanism without the functions.

| Sub-unit | Contents | Reviewable on its own because |
|---|---|---|
| **1a — Guard narrowing** (step 0) | `FlinkTypeFactory.leastRestrictive` keyed off `GenericRelDataType`, with the `LogicalTypeRoot` audit that keeps `GenericRelDataType` the only `ANY`-typed `RelDataType` converted from a `LogicalType` | A four-line change in one method, pinned by unit tests that call `leastRestrictive` directly. It depends on nothing and is the only piece not conditioned on a lambda |
| **1b — Type** | `LogicalTypeRoot.FUNCTION`, `FunctionType`, `DataTypes.FUNCTION`, the `FunctionDataN` and `FunctionN` families, the `LogicalTypeParser`/`asSerializableString` round-trip, the materialization guards and the exhaustive-`switch` audit | Mostly in `flink-table-common`. The rest is `FunctionN` in `flink-core-api` and two of the materialization guards: `InternalSerializers` in `flink-table-type-utils` and `DefaultSchemaResolver` in `flink-table-api-java`, plus the `FUNCTION` case of the planner's `CodeGenUtils.boxedTypeTermForType`. Its whole contract is round-tripping and rejection, both unit-testable, with the planner touched by one `match` case only |
| **1c — SQL front end** | `FlinkSqlLambdaScope`, cumulative `RexLambdaRef` indices, lambda-body validation, the `FlinkTypeFactory`/`FunctionRelDataType` bridge, decorrelation, `ExtractLambdaOverWindowRule` | Depends on 1a–1b. Reviewable as "does a lambda validate and convert to a correct `RexNode`", with plan tests and no runtime |
| **1d — Capture lifting, code generation and plan serde** | Capture lifting, the `LambdaInputTypeStrategy` contract with its restriction to built-in functions, the `RexLambda → Expression` bridge, the generated function object, `RexLambda` compiled-plan serde | Depends on 1c. Reviewable as "does a validated `RexLambda` become a correct executable object, and does it round-trip through a compiled plan" |
| **1e — Table API** | `UnresolvedLambdaExpression`/`LambdaExpression`, expression-resolver support (enclosing lambda parameters and over-windows in a nested lambda's scope), lambda-aware aggregate extraction and lambda-body validation, `ExpressionConverter` (`LambdaExpression → RexLambda`) | Depends on 1b and 1d. Reviewable as "does a Table API lambda resolve to the `RexLambda` the SQL front end produces", which 1f's IT cases then check on both surfaces |
| **1f — Array functions** | `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE`, `ARRAY_ZIP_WITH` as runtime classes with their definitions and type strategies, their DSL methods and PyFlink `array_*` bindings, and the end-to-end evidence for 1b–1e: batch and streaming IT cases, restore and compiled-plan tests, the SQL/Table API validation parity test | Depends on 1d and 1e. Reviewable function by function against [*Public interfaces → 3*](#3-built-in-higher-order-functions), with the mechanism already reviewed |

The boundaries are ones the design already has, so the split costs only six pull requests instead of
one. No sub-unit is releasable on its own, hence the joint merge to `master`. A maintainer who
prefers one layer-1 pull request gets one with the same boundaries, step 0 still its own commit.

### Blast radius, and why there is no feature flag

Layer 1 touches three paths that every SQL query goes through:

- the SQL front end and decorrelation
- type derivation for `ARRAY[...]`, `MAP[...]` and `CAST` (`SqlCastFunction`'s small guarded change)
- determinism analysis: `RexUtil.isDeterministic` and its two Flink-side companions,
  `ShortcutUtils.isDeterministicThroughProgram` and `FlinkRexUtil.getNonDeterministicCallName`

Every change on those paths is conditioned on an unresolved lambda placeholder or a `RexLambda`, so
a query without lambdas takes the existing behaviour by construction and never reaches the new code.
`ExtractLambdaOverWindowRule` is not on such a path at all: its match predicate requires a
`RexLambda`, so it never fires on a plan without lambdas.

Layer 1 also touches paths every Table API program goes through: expression resolution
(`ExpressionResolver`, `ResolveCallByArgumentsRule`, `LookupCallResolver`), aggregate extraction
(`OperationExpressionsUtils`, `OperationTreeBuilder`) and schema resolution
(`DefaultSchemaResolver`). They are conditioned the same way: on an `UnresolvedLambdaExpression` or
`LambdaExpression`, or, for schema resolution, on a `FUNCTION` column, which no schema could contain
before. A visitor that gains a lambda branch hands every other expression to its previous default.

`leastRestrictive` is **the one exception**, which is why step 0 is its own commit;
[*Step 0*](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive) specifies, pins and measures
it. Every other layer-1 change on such a path is conditioned on a lambda, so *not using the
functions is the off switch*, and no configuration kill switch is proposed. A flag would add a
second, less-tested path through the same validator. The existing planner and IT suites are the
regression evidence for this claim, and the pull request has to be green on them.

### How to review

This document is the complete layer 0 + 1 design; a layer-1 reviewer can read it alone. The parent
FLIP (`FLIP-XXXX-higher-order-functions-and-lambdas.md`) designs the whole feature, and the
prototype branch shows it holds together. The sections map onto the sub-units:
[*Type system*](#type-system-flink-table-common) for 1b,
[*SQL planner*](#sql-planner-flink-table-planner) for 1c, [*Lambda captures*](#lambda-captures),
[*The generated function object*](#the-generated-function-object) and
[*Compiled plan*](#compiled-plan-flink-table-planner) for 1d,
[*Table API*](#table-api-flink-table-api-java-flink-table-common) for 1e, and
[*Public interfaces → 3*](#3-built-in-higher-order-functions) and [*Python*](#python-flink-python)
for 1f.

## Test plan

Everything below checks computed results, not plans, against SQL and, where the Table API can spell
it, the Table API. Only three semantics cases are SQL-only, because they depend on parameter names a
user chooses (two shadowing cases, and a reducer whose parameters are named `(x, acc)`): the typed
Table API methods generate their own names, and the spelling with explicit names is tested
separately. The built-in *execution* semantics run in batch *and* in streaming
(`HigherOrderFunctionsBatchITCase`, `HigherOrderFunctionsITCase`), including `NULL` and empty arrays
and `NULL` elements. The mode-independent areas (aggregates, `OVER` windows, sub-queries and
correlation in a body, views, `FUNCTION`-type rejection, non-determinism, and code splitting of the
body) are checked once, in the batch class; four of its tests create a streaming environment because
the feature under test needs one (named state entries, an `OVER` window on the Table API, and the
two watermark tests). The materialized table, a `CONTINUOUS` one, is checked in the SQL gateway
(`MaterializedTableStatementITCase`). Layer 1 contains 7,793 test lines.

### Commitments

- **Built-in semantics.** All four functions on both surfaces over columns and literals: `NULL` and
  empty arrays, `NULL` elements, element and accumulator type changes, nesting without captures, and
  `ARRAY_ZIP_WITH` over arrays of different lengths, also with `NOT NULL` elements (nullable lambda
  parameters, §5). For `ARRAY_REDUCE`: a body assignable to but not identical with the accumulator,
  every row of the [widening table](#array_reduce-accumulator-typing), a reducer that keeps its
  type, and parameters named `(x, acc)`.
- **Captures and nesting.** Outer-column captures for each function, and inner lambdas capturing an
  *enclosing* lambda's parameter: two levels deep, combined with an outer-column capture, and with
  name shadowing, also on the Table API (`testLambdaParameterShadowingOnTableApi`).
- **Captures outside a projection** (see [*Lambda captures*](#lambda-captures)): join conditions
  capturing either side, also `LEFT`, nested, in an equi-join operand and `LATERAL`
  (`testLambdaCaptureInJoinCondition`); computed columns and `callSql`
  (`testLambdaInStandaloneSqlExpression`); watermarks with and without push-down
  (`testLambdaInWatermarkExpression`), with a UDF in the body reporting to the source's metric group
  (`testUserDefinedFunctionInPushedDownWatermarkLambdaUsesSourceMetricGroup`); partition pruning
  (`testLambdaInPartitionPredicate`); and a lambda conjunct withheld from a source's filter
  push-down while the others are offered (`testLambdaPredicateIsNotOfferedToSource`).
- **Field access on a row-typed parameter.** `r.n` and `r.nested.v`, also nested and under
  `GROUP BY`, a parameter shadowing a table alias, and rejection of an unknown field and of an
  aggregate over a field (`testFieldAccessOnRowLambdaParameter`).
  [Deferred field access](#sql-planner-flink-table-planner), where an unknown field is still
  "Unknown field" and `MULTISET[x]` keeps its documented error
  (`testFieldAccessInNestedLambdaBody`).
- **Correlation across a sub-query boundary** (see [*Lambda captures*](#lambda-captures)), executed
  end to end: nested, mixed local and correlated, and two distinct correlated captures, with
  unaffected controls (`testOuterColumnCaptureFromLambdaInScalarSubquery`).
- **A capturing lambda inside an aggregate, a group key or a sort key.** A `GROUP BY` key
  (`testColumnCaptureInGroupBy`), and `SUM`, `MAX`, `COUNT(DISTINCT …)`, `FILTER (WHERE …)`,
  `HAVING` and `ORDER BY` aggregates, also over join columns, in an enclosing lambda's body and in
  an aggregating sub-query (`testColumnCaptureInAggregateArgument`).
- **A lambda in `ORDER BY`.** Each [name rule](#lambda-captures), with `DESC`, `LIMIT`, `DISTINCT`,
  a repeated key, a star over a sub-query, a nested lambda and a group key (`testLambdaInOrderBy`).
- **A merged join column in a body.** The ambiguous unqualified `USING` or `NATURAL` join column and
  each workaround of [*Known limitations*](#known-limitations) (`testCommonJoinColumnInLambdaBody`).
- **Validation and error messages.** A lambda outside a higher-order function
  (`testLambdaOutsideOfHigherOrderFunctionIsRejected`); `FUNCTION` as a column type (directly,
  nested, as metadata, computed, from a catalog, through CTAS), return type, accumulator type or
  named state entry (`testFunctionTypeRejectedAsReturnType`, `…AsAccumulatorType`,
  `…AsNamedStateEntry`); casts to and from `FUNCTION`, duplicate parameter names, the reserved
  `cap$` prefix, a wrong parameter count (`testLambdaParameterCountMismatchIsRejectedOnTableApi`), a
  function name as a lambda (`testFunctionReferenceAsLambdaIsRejected`), and the signature errors of
  all four built-ins (`testInvalidArgumentsAreRejectedWithTheFunctionSignature`).
- **Layer-1 restriction.** A lambda passed to a user-defined scalar or aggregate function is
  rejected on both surfaces with the "only supported by built-in functions" message
  (`UserDefinedFunctionLambdaArgumentTest`; layer 3 deletes the restriction cases with the check and
  keeps the entry-guard cases). The same class pins
  the two entry guards: a `FUNCTION`-typed static argument (an `eval` parameter, an optional
  `@ArgumentHint`, a process table function argument, a nested `ARRAY<FUNCTION(1)>`) called with
  `NULL`, a lambda or no argument (`testFunctionTypedStaticArgumentIsRejected`), and a
  `FUNCTION`-typed literal (`testFunctionTypedLiteralIsRejected`).
- **Row-level constructs in a body** (see [*Known limitations*](#known-limitations)). Aggregates
  over outer columns under `GROUP BY` (both key orders, and inside a nested lambda), `COUNT(*)`,
  `OVER` windows with and without a partition, and plain and correlated scalar sub-queries, also on
  the Table API where they have a spelling; the rejections over a lambda **parameter**, and each
  sub-query case for a name that matches one, down to "not being grouped"
  (`testSubQueryInLambdaBody`). An aggregate over a *nested* lambda's parameters is accepted, and a
  hoisted aggregate keeps the grouping rules (`testAggregateInLambdaBodyWithoutGroupingIsRejected`).
- **Construct rejections in a body.** Table functions, asynchronous scalar functions and Python
  scalar functions, on both surfaces, also when the call does not use the lambda parameter
  (`testTableFunctionInLambdaBodyIsRejected`,
  `testAsynchronousScalarFunctionInLambdaBodyIsRejected`,
  `testPythonScalarFunctionInLambdaBodyIsRejected`).
- **User-defined scalar functions in a body.** On its own, with a capture and nested, on both
  surfaces (`testUserDefinedFunctionInLambdaBody`), and a higher-order call as a UDF argument
  (`testHigherOrderFunctionAsArgumentOfUserDefinedFunction`). The
  [lifecycle](#the-generated-function-object): opened and closed once
  (`testUserDefinedFunctionInLambdaBodyIsOpenedAndClosed`), with the operator's `FunctionContext`
  (`testUserDefinedFunctionInLambdaBodyIsOpenedWithOperatorContext`) and metric handle
  (`UdfMetricsITCase#testLambdaBodySharesOneHandle`), and an unchecked exception keeping its
  original root cause (`testLambdaBodyErrorPropagatesUnchanged`).
- **Views and materialized tables.** Both persist the *expanded* query and re-parse it on every
  read, so the expansion must leave a lambda parameter unqualified while qualifying captured
  columns, tables and catalog function names. Each case checks the persisted query *and* the rows,
  including shadowing, stacked views (`testViewOver…`) and a `CONTINUOUS` materialized table
  (`MaterializedTableStatementITCase#testCreateMaterializedTableWithHigherOrderFunctions`).
- **Value constructors and mutable results.** Each shape the deferral of `ARRAY[...]`, `MAP[...]`
  and `CAST` must cover (`testCollectionConstructorsOverLambdaParameter`), and the
  [no-aliasing guarantee](#3-built-in-higher-order-functions)
  (`testMutableLambdaResultsAreNotAliased`). `ARRAY_REDUCE`'s accumulator ownership: an empty
  array's initial accumulator buffered downstream, in batch with object reuse and in streaming, and
  `ARRAY` and `ROW` reducers reading the old accumulator in swapped order, which only the copy of
  each accumulator makes correct (`ArrayReduceAccumulatorOwnershipITCase`).
- **Type system and type inference.** The `LogicalTypeParser` round trip and the `function` keyword
  of [*Compatibility*](#compatibility-deprecation-and-migration-plan) item 7
  (`LogicalTypeParserTest`); a conversion class per arity, none outside the range, and a fallback
  internal class for a lifted-capture arity (`LogicalTypesTest`); the step-0 RAW guard, nullability
  canonicalization on every construction path, and the parameter type strategies; a wrapping
  `CallContext` reporting the lambda of the argument it was asked about
  (`SubsequenceInputTypeStrategyTest`); and `InternalSerializers` refusing a `FUNCTION`
  (`InternalSerializersTest`).
- **Function resolution** (`FunctionCatalogTest`): the four reserved names behave as in
  [*Compatibility*](#compatibility-deprecation-and-migration-plan) item 1.
- **Compiled plan.** Restore tests for the four functions and a capturing lambda, also as
  `BatchExecCalc` baselines (`CalcRestoreTest`, `CalcBatchRestoreTest`); `RexNode` serde with and
  without a lifted capture, and rejection of an invalid lambda (`RexNodeJsonSerdeTest`).
- **Closed-lambda invariant.** `HigherOrderFunctionUtil.checkClosed` after capture lifting: the
  positive case, a free outer column, an out-of-range reference, and a parameter list that is not
  0-based (`HigherOrderFunctionUtilTest`), a check the
  [plan reader](#compiled-plan-flink-table-planner) repeats.
- **The body stays out of type inference.** Re-deriving a converted call's return type gives the
  output type strategy the parameters and result type but no body
  (`TypeInferenceReturnInferenceTest`). Every executed case covers specialization, where `getBody()`
  throws without one.
- **Lambdas that differ only in their captures are not merged**
  (`testLambdasThatDifferOnlyInTheirCapturesAreNotMerged`).
- **SQL↔Table API parity harness** (`HigherOrderFunctionValidationParityTest`). Every built-in
  argument-rejection case as an equivalent SQL string and Table API expression, both rejected during
  validation, including the type pairs on which Calcite's `leastRestrictive` and Flink's
  `findCommonType` disagree. Where both surfaces word the error alike, it pins the message: every
  `ARRAY_REDUCE` accumulator mismatch with its hint (including the three that widening cannot fix),
  a non-lambda value in a lambda position, a non-array argument, and a refinement that returns too
  few or too many lambda parameter types (`testRefinementChangingTheLambdaArityIsRejected`).
- **Completeness, derived from the definitions, not from a list.** Four checks make an omission in a
  *new* higher-order built-in fail the build. Each finds them by asking every
  `BuiltInFunctionDefinition` whether its input type strategy declares a lambda argument
  (`LambdaBuiltInFunctions`; the flink-docs check applies its own copy of that predicate):
  - `BuiltInFunctionDefinitionsTest` requires a runtime class.
  - `HigherOrderFunctionCoverageTest` requires, checked independently, **both** an execution case
    with a result assertion in the semantics IT case *and* a rejection case in the parity harness.
  - `BaseExpressionsLambdaCompletenessTest` requires a same-named typed Table API method taking a
    Java lambda (`Function0` to `Function4`); `test_expression_completeness.py` covers PyFlink.
  - `SqlFunctionsDocsCompletenessITCase` requires the documentation entries.
- **Determinism.** A non-deterministic body makes the enclosing call non-deterministic and a
  deterministic one does not, through each entry point of [*Lambda captures*](#lambda-captures)
  (also through a `RexLocalRef`) and each place they are used (`HigherOrderFunctionDeterminismTest`,
  `NonDeterministicUpdateAnalyzerTest`, `testNonDeterministicLambdaBodyIsEvaluatedPerCall`,
  `testNonDeterministicLambdaBodyInFilterAndProjection`).
- **PyFlink end to end** (`test_higher_order_functions.py`): unary and binary lambda adapters, an
  outer-column capture, a nested lambda and a validation failure, all checked on collected results.
- **Large lambda bodies stay splittable** (`testLargeLambdaBodyIsSplitIntoSeparateMethods`). A body
  far beyond `table.generated-code.max-length` runs, with the same expression outside a lambda as
  the control. Unsplit, it would fail with `Code grows beyond 64 KB` and the misleading generic
  "this is a bug" message ([appendix A3](#a3-code-generation-mechanics)).

**Deferred to layer 3**, because only a user-defined function can observe them: the
[lambda object contract](#the-lambda-object-contract) tests (lazy evaluation, a fresh object per
evaluation, repeated and reordered application over one capture snapshot, catching an unchecked
failure from `apply`), multiple lambda arguments per call, the public `LambdaInputTypeStrategy`
extension point, and function-kind scoping. The cases of the full branch's
`UserDefinedHigherOrderFunctionITCase` that need only a built-in host or a regular UDF (`FUNCTION`
return and state guards, construct rejections, the Table API arity check, UDF lifecycle and error
propagation) are tested here, and layer 3 can drop its copies when rebased.

### Test inventory

| Module | Test | Lines |
|---|---|---:|
| `flink-table-planner` | `HigherOrderFunctionsBatchITCase` (batch semantics and the mode-independent areas) | 2,956 |
| | `HigherOrderFunctionsITCase` (streaming semantics, `BuiltInFunctionTestBase` specs) | 609 |
| | `FlinkTypeFactoryTest` (step 0 and the `FunctionType` bridge) | 319 |
| | `HigherOrderFunctionUtilTest` (capture lifting, `checkClosed`) | 225 |
| | `HigherOrderFunctionValidationParityTest` | 384 |
| | `UserDefinedFunctionLambdaArgumentTest` (layer-1 restriction, entry guards) | 281 |
| | `HigherOrderFunctionDeterminismTest` | 158 |
| | `TypeInferenceReturnInferenceTest` (no lambda body during return-type inference) | 134 |
| | `CalcTestPrograms` + `CalcRestoreTest` / `CalcBatchRestoreTest` (5 programs, batch and stream plans, stream savepoints) | 132 |
| | `RexNodeJsonSerdeTest` | 117 |
| | `ArrayReduceAccumulatorOwnershipITCase` | 140 |
| | `HigherOrderFunctionCoverageTest` | 95 |
| | `UdfMetricsITCase` (a function in a lambda body shares the operator's metric) | 27 |
| | `NonDeterministicUpdateAnalyzerTest` | 25 |
| `flink-table-common` | `LogicalTypesTest`, `SubsequenceInputTypeStrategyTest`, `DataTypesTest`, `BuiltInFunctionDefinitionsTest`, `LogicalTypeCastsTest`, `LogicalTypeParserTest`, `LogicalTypeCastAvoidanceTest` | 460 |
| `flink-table-api-java` | `BaseExpressionsLambdaCompletenessTest`, `FunctionCatalogTest`, `SchemaResolutionTest` | 190 |
| `flink-table-type-utils` | `InternalSerializersTest` | 42 |
| `flink-sql-gateway` | `MaterializedTableStatementITCase` | 106 |
| `flink-docs` | `SqlFunctionsDocsCompletenessITCase` | 153 |
| `flink-python` | `test_higher_order_functions.py`, `test_expression.py` | 144 |

## Documentation

User-facing docs ship in the same commit as their surface, in English and Chinese (377 lines):

- `docs/content{,.zh}/docs/sql/functions/built-in-functions.md`: a **Higher-Order Functions**
  section (lambda syntax, scoping, captures, the rules of [*Known limitations*](#known-limitations),
  Java and Python Table API examples). Per-function entries are in
  `docs/data/sql_functions{,_zh}.yml` under the new `higherorder` category.
- `docs/content{,.zh}/docs/sql/reference/data-types.md`: a **`FUNCTION`** entry, a planning-only
  type of arity 0–4 (`Function0`–`Function4`) that cannot be declared in SQL, stored, or used
  from Python.
- The PyFlink expression methods in `flink-python/docs/reference/pyflink.table/expressions.rst`.
- The lambda object contract, in the `LambdaInputTypeStrategy` Javadoc. The **Lambda Arguments**
  section of `udfs.md` is layer 3's.

## Rejected alternatives

The parent FLIP records further rejected alternatives that belong to layers 2 and 3; they are left
out. Each entry below is a decision that layer 0 + 1 makes and the later layers inherit.

- **Keeping the `FUNCTION` type planner-internal (no public type).** This is viable only for a
  SQL-only build, as a prototype confirms (about 6 files, no public type): the planner's
  `FunctionRelDataType` is already `@Internal`, and a compiled plan never serializes the `FUNCTION`
  type ([*Design overview*](#design-overview)). It is rejected because a lambda crosses public
  contracts in modules that must not depend on `flink-table-planner`: Table API resolution and type
  inference in layer 1, and user-defined function type inference in layer 3, for which an internal
  type would have to be reworked into a public one. The new root touches every exhaustive `switch`,
  so the audit in [*Proposed changes → Type system*](#type-system-flink-table-common) makes every
  site handle it, reject it with a clear message at one of the five choke points or two entry
  guards, or keep it unreachable behind a throwing default; it never silently gives a wrong result.

  **What the public type costs.** The internal alternative commits Flink to nothing outside
  `flink-table-planner`. The public type commits it to:
  1. a new `LogicalTypeRoot` constant, which is switched over exhaustively across the whole table
     stack and is effectively irreversible once released;
  2. a serializable string form `FUNCTION(n)` in `LogicalTypeParser` and
     `LogicalType#asSerializableString`, a parser and serializer surface that must keep
     round-tripping in every future release;
  3. a `LogicalTypeVisitor#visit` default method that every existing visitor inherits;
  4. the ordinary `@PublicEvolving` obligations on `FunctionType`, `DataTypes.FUNCTION(int)` and the
     `FunctionDataN` family. These obligations are why the constructor validates its arity and why
     the arity-to-conversion-class mapping is kept off the type's public surface.

  These are paid *once*, now, rather than as a superset during a later public rework, but they are
  permanent in a way the internal type would not be.

- **An explicit `isCapture` marker on `RexLambdaRef` instead of a sentinel index base.** Lifted
  captures are numbered from `HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE = 1_000_000` and
  named `cap$N`. A `boolean isCapture` field would need no magic number, but `RexLambdaRef#equals`
  compares only *index and type* ([*Lambda captures*](#lambda-captures)). A flag outside that
  identity would let a capturing lambda be merged with a structurally identical non-capturing one;
  a flag inside it would change the equality contract of a vendored Calcite class
  ([*A6*](#a6-vendoring-cost)). The index needs no Calcite change. The cost is that index and name
  are **visible in plans**, verbatim in the committed restore plans
  (`calc-array-transform-capture.json`, batch and stream), so `1_000_000` and `cap$` are frozen in
  the plan format: old and new versions both read them, but renaming needs a plan migration.

- **Encoding lambdas as strings**, passing the body as a string literal that the function parses,
  e.g. `ARRAY_TRANSFORM(a, 'x -> x + 1')`. Calcite 1.34+ parses `x -> expr` natively, so this saves
  no parser work. A string body is opaque to validation (types, column resolution, determinism) and
  to the optimizer (expression reduction, which a lambda body gains with CALCITE-7497, capture
  lifting, sub-query decorrelation), invisible in `EXPLAIN`, and impossible to write in the Table
  API or PyFlink without duplicating the SQL parser; its dialect would become a compatibility
  surface.

- **Upgrading Calcite, or upstreaming the fixes first, instead of carrying the modifications.** This
  is rejected as a *precondition*, not as a direction. Ten of the 18 blocks that layer 1 adds to
  vendored Calcite classes are already scheduled for deletion on upgrades in flight (6 at 1.42.0
  with CALCITE-7497, 4 at 1.43.0 with CALCITE-6242). The remaining eight are either Flink-specific
  (deferred operand binding, Flink's own lambda scope `FlinkSqlLambdaScope`), or they sit in private
  methods and private or package-private nested classes, so an upstream patch would first have to
  *introduce* extension points. Waiting would couple a Flink feature to a Calcite release cycle, a
  Flink-wide Calcite upgrade and an upstream API design, for an unbounded period. Instead, each
  block records the Calcite issue that removes it, or why it survives both upgrades, and
  [*A5*](#a5-why-the-calcite-changes-cannot-be-applied-through-hooks-or-subclasses) records why no
  hook or subclass would do. Accepting this debt is a maintainer decision; this entry states the
  case rather than settling it.

- **Modelling a lambda as an argument *kind* rather than an argument *type***: a
  `StaticArgumentTrait.LAMBDA` plus a semantics accessor and no `LogicalTypeRoot`, as for `TABLE`
  and `MODEL` (`StaticArgument.model(...)` carries no `DataType`). Flink gives a type root to an
  argument that is a *value in a scalar operand slot*, as `SYMBOL` and `DESCRIPTOR` are; a `TABLE`
  (a relational input) or `MODEL` (a catalog reference) never is. A `RexLambda` *is* a `RexNode`
  operand, and the function receives a `FunctionData0`…`FunctionData4` object in `eval()`, as a
  process table function receives a `ColumnList` for a `DESCRIPTOR`. As a kind, the `eval()`
  parameter class could not be derived from the argument's conversion class.

- **A fully parameterized `FUNCTION` type** carrying the lambda's parameter types and result type
  (`FunctionType(argumentTypes, resultType)`, serialized as `FUNCTION<(t0, ...) -> tr>`). Rejected
  in favour of the parameter-count-only type, which follows both existing helper roots and keeps the
  audited surface at `DESCRIPTOR`'s level. Nothing consumes the parameter types, and the result type
  is consumed only by the built-in input and result type strategies, which read it from
  `CallContext#getLambdaArgument(pos)`, as a process table function reads a `DESCRIPTOR`'s column
  list. The payload in the type would cost:
  - a `FunctionRelDataType` that canonicalizes nullability and recomputes digests, so that two
    identical signatures do not split during coercion (`SYMBOL` and `DESCRIPTOR` map to a plain
    `SqlTypeName` and need no custom `RelDataType`);
  - a new `->` token and a nested-type grammar production in `LogicalTypeParser`, on top of the
    keyword and required-precision production that the count-only form needs anyway;
  - interaction with `leastRestrictive` over the inner types.

- **Bare `TRANSFORM` / `FILTER` / `REDUCE` names.** `TRANSFORM` collides with Hive's
  script-transform syntax and `FILTER` with the SQL aggregate `FILTER (WHERE …)` clause, while the
  `ARRAY_*` prefix matches Flink's existing array functions
  ([*Naming in other engines*](#naming-in-other-engines)).

- **`OperandTypes.ARRAY_FUNCTION` for `ARRAY_TRANSFORM`.** Despite its name, it is the `ARRAY[…]`
  constructor's same-type checker, so Calcite computes `leastRestrictive(ARRAY, FUNCTION)` and
  fails an assertion.

- **Deleting the RAW common-type guard** to make lambda-body validation pass. This silently turns
  genuine RAW mismatches into an untyped `ANY` column. The guard is narrowed instead
  ([*Step 0*](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive)).

- **Committing to a wide `FunctionN` family now.** jOOQ and jOOλ ship `Function1`…`Function22`
  and `Function23` respectively, and Calcite's grammar puts no bound on lambda arity. A prototype at
  **arity 22** shows that the planner, the code generation, `LambdaFunctionFactory`,
  `UnresolvedLambdaExpression`, capture lifting, the Table API and the compiled-plan format are
  **already arity-generic**: `BridgingFunctionGenUtil` builds `apply` from a parameter count and
  implements whatever interface the conversion class names. The prototype changed none of them, and
  the full test suite passed. PyFlink has one small adapter per arity its DSL methods use. A wider
  bound therefore costs only the `@PublicEvolving` types: one external `FunctionN` and one internal
  `FunctionDataN` per arity, i.e. 46 permanent interfaces at arity 22 against the 10 that layer 1
  adds, each with architecture-test and documentation obligations, for functions that do not exist.
  Adding an arity later costs no more than adding it now. Four is one above the widest built-in the
  full feature delivers ([*Known limitations*](#known-limitations)).

- **Keeping `Function`, `BiFunction` and `TriFunction` as the conversion classes at arities one to
  three**, with `FunctionN` only at zero and four, where no interface with an `apply()` method
  exists. This yields a list a reader has to memorise (`Function0`, `Function`, `BiFunction`,
  `TriFunction`, `Function4`), with the arity readable from the name at only two of five positions,
  and the mixture leaks into the documentation, the error messages, the bridging table and
  `getConversionClass()`. Nothing is being migrated, since the `FUNCTION` type and the `FunctionN`
  family ship together, and nothing is lost: `Function`/`BiFunction`/`TriFunction` remain accepted,
  declarable and bridgeable conversions. Only the name the framework picks by itself changes.

- **Inverting the family at arity three** (`TriFunction extends Function3` rather than the
  reverse), which is possible, unlike for the JDK-owned `Function` and `BiFunction`. It would make
  every existing `TriFunction` in Flink a `Function3`, including the `...WithException` family and
  user code with no connection to SQL, coupling a general-purpose `flink-core-api` utility to the
  table module's lambda family for no gain. Accepting `TriFunction` as an equivalent conversion
  gives lambda arguments the same result without touching its supertypes.

- **Handing the function the compiled `ExpressionEvaluator` directly** instead of a function
  object. This is simpler for non-capturing lambdas but cannot support closures: captured values
  vary per row, and the receiving function's fixed `eval` signature has no channel for them. The
  function object has a stable arity, binds the per-row captures behind it and leaves the function
  in control of its loop. The evaluator's `LambdaFunctionFactory` implementation generates it, and
  its `apply` calls the typed `eval` directly: no reflective indirection, no new codegen machinery.

- **Generating the built-ins' loops inline in the planner**, in a
  `codegen/calls/HigherOrderFunctionCallGen.scala` next to the existing `SearchOperatorGen` and
  `Json*CallGen` generators, rather than as `BuiltInScalarFunction`s in `flink-table-runtime`. The
  case for inlining is per-element cost: argument and result conversions, and a
  `MethodHandle.invoke` through an `Object`-typed `apply`. Neither is intrinsic to the runtime form.
  A lambda argument has an internal representation (`FunctionDataN`), so a built-in reading
  `ArrayData` needs no conversions, and the generated `apply` calls the evaluator's typed `eval`
  directly, which alone improves the lambda ABI by 2.2×–4.7×. What remains is the boxing that the
  `Object`-typed ABI forces around a nearly free virtual call: on an isolated per-element harness,
  a built-in runs at 1.03×–1.31× of the inlined loop for `ARRAY<INT>` and at 0.96×–1.07× for
  `ARRAY<STRING>` ([A3.3](#a33-what-the-lambda-abi-costs)). The inlined prototype boxed every
  element too; the real straight-line form, 4×–5× faster than either, is reachable from the runtime
  class ([A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)).

  Inlining costs, per function, a Calcite operator, an operand type checker, an
  `ExpressionConverter` mapping and a code-generation method, plus lambda-binding state
  (`RexLambdaRef` scoping, capture evaluation) in `ExprCodeGenerator` that every new higher-order
  function would have to extend. The runtime form costs what *any* scalar function costs: a
  definition entry, a type strategy and a runtime class. It also leaves one lambda-execution stack:
  a built-in and (from layer 3) a user-defined function given the same body compile to the same
  code, so nesting, captures, copying of buffered results, null semantics and error messages
  cannot drift apart. No shape is slower for a reason only inlining can fix, so no inlined fast
  path, dispatch or flag is kept beside it. Should a future shape need one, the runtime class is
  the executable reference semantics, and equivalence is the existing suite run with inlining on
  and off.

- **Deriving `ARRAY_REDUCE`'s assignability and widening rule from Calcite's `leastRestrictive`**
  on the SQL side. The Table API must use `LogicalTypeMerging.findCommonType`, and the two disagree
  on several pairs where only the latter follows Flink's type system. Calcite treats `FLOAT` as the
  widest approximate type, correct for the SQL standard but not for Flink's 4-byte `FLOAT`, so it
  would accept a `DOUBLE` reducer body for a `FLOAT` accumulator and silently narrow the result,
  where `findCommonType` widens the accumulator to `DOUBLE`. It also merges pairs that Flink cannot
  cast between (e.g. `INT` into `TIME`), which then fail during code generation rather than
  validation. One merging function on both surfaces is what makes the parity harness meaningful.

- **Other ways to type `ARRAY_REDUCE`'s accumulator**, against the rule in [*`ARRAY_REDUCE`
  accumulator typing*](#array_reduce-accumulator-typing):
  - *The initial value's type, exactly, and a cast for anything else* rejects the most natural
    folds (`''`, `0` over decimals, `ARRAY[0]` over nullable elements), with no obvious cast.
  - *Always a broad accumulator type* (`STRING`, `DECIMAL(38, s)`) changes the result type of folds
    that need no widening: `ARRAY_REDUCE(a, 'zz', (acc, x) -> acc)` would no longer be a `CHAR(2)`.
  - *Iterating the widening to a fixed point* chases a type that grows every step (`CHAR(0)`,
    `VARCHAR(1)`, `VARCHAR(2)`, …, or a decimal product's precision and scale): it needs an
    arbitrary bound, and its result depends on how many rounds ran. Widening once to the bound the
    fold converges to gets the same result in one step.
  - *Widening across families* (`INT` to `DOUBLE`, `DATE` to `TIMESTAMP`) would make an exact
    accumulator approximate, or change a value's meaning, without the user asking for it.

## Future work

- **Layers 2 and 3.** The next deliveries, specified in the parent FLIP and summarized under
  [*Delivery*](#delivery-how-layer-0--1-is-cut-sized-and-reviewed). Layer 2 adds further built-in
  higher-order functions on the same mechanism. Layer 3 lifts the "built-in functions only" check,
  adds the function-kind guards and process table function support, and documents lambda arguments
  in the UDF guide. Neither changes anything that layer 1 introduces.
- **A standing JMH benchmark for higher-order functions.** The lambda ABI's cost has been measured
  once, on an isolated harness ([appendix A3.3](#a33-what-the-lambda-abi-costs)). A
  `flink-benchmarks` harness, like the scheduler and state backend benchmarks, would guard it
  against regression ([appendix A3.4](#a34-the-benchmark-protocol)). A JIRA will be filed under
  FLINK-31207. Nothing in the design depends on the outcome.
- **Primitive specialization of the higher-order runtime classes.** The 4×–5× headroom, and all of
  the per-element garbage, lies in the boxing that the `Object`-typed ABI forces at the evaluator's
  boundary and in the `Object[]`/`GenericArrayData` result assembly; the hooks to remove it exist
  ([appendix A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)). It is not proposed:
  on a sixteen-element array the saving is around 90 ns per record, against a record's
  microsecond-scale journey through an operator chain; the fast path covers only `NOT NULL`
  primitive element types unless the interface gains a null channel; and `ARRAY_REDUCE`'s two
  independently typed parameters multiply out the interface matrix. The trigger is evidence: a real
  job showing higher-order element throughput or boxing-related allocation as a hot spot.
- **More array higher-order functions.** What other engines offer beyond this set is mostly plain
  new functions: the quantifiers (`any_match` / `all_match` / `none_match` in Trino, `exists` /
  `forall` in Spark), with an `ARRAY_EXISTS` the obvious first, and a comparator-lambda overload of
  the existing `ARRAY_SORT` (Spark, Trino). Two shapes are not plain:
  - An **index-aware** lambda (`(x, i) -> …`, which Spark's `transform` and `filter` accept next
    to `x -> …`) is an overload on the lambda's arity, but `getExpectedLambdaParameterTypes` does
    not see the arity the lambda was written with. It needs that information on the `CallContext`
    (an additive change to a `@PublicEvolving` interface) or a function name of its own.
  - A **finish function** for `ARRAY_REDUCE` (Spark's `aggregate(…, finish)`, Trino's
    `reduce(…, outputFunction)`) is a second lambda in the same call, which the mechanism already
    handles. It is a convenience only, since applying the finishing expression to
    `ARRAY_REDUCE`'s result is equivalent.

  A new higher-order built-in needs a `BuiltInFunctionDefinitions` entry naming a runtime class, a
  declarative input and output type strategy, the runtime class, tests, the Table API and PyFlink
  methods, and two documentation entries; no Calcite operator, operand checker,
  `ExpressionConverter` mapping, code generator or restore artifact. The completeness tests derive
  the set of higher-order built-ins from the definitions, so an omission fails the build
  ([*Test plan*](#test-plan)). As the family grows, consider extracting a shared element-loop
  skeleton for the array iterators that differ only in the per-element action and result assembly.
- **Function references at a lambda position (eta-expansion).** `ARRAY_TRANSFORM(a, my_udf)` as
  shorthand for `ARRAY_TRANSFORM(a, x -> my_udf(x))`. It is additive and needs no change to the
  `FUNCTION` type or the runtime representation, only a resolution step (see
  [*Known limitations*](#known-limitations)).
- **Lambda predicates for connectors.** If `RexNodeToExpressionConverter` translated a `RexLambda`
  back into a `LambdaExpression`, a conjunct with a higher-order call could reach
  `SupportsFilterPushDown`, to be accepted or declined like any other predicate. This is additive,
  since a source that does not recognize the call declines it. Its one design question is whether a
  source should see the lifted form (`cap$` parameters with trailing operands) or a closure
  reconstructed from it.
- **Shrinking the Calcite fork on the upgrades.** Work that this design schedules; what survives
  both upgrades is under [*Calcite fork*](#calcite-fork-what-is-upstream-what-is-residual).
  - The **1.42.0** upgrade deletes six vendored blocks and the `visitLambda` override of
    `HigherOrderFunctionUtil`'s capture-lifting shuttle, and allows the post-decorrelation re-lift
    in `FlinkDecorrelateProgram` to be reconsidered.
  - The **1.43.0** upgrade deletes four more blocks, `FlinkSqlLambdaScope.fullyQualify` and
    `resolveColumn` (enabling `SqlConformance.allowLambdaClosure()` instead), and
    `FlinkSqlToRelConverter`. It allows the leftover-correlation check to be reconsidered, and must
    settle the parameter-shadowing difference.
- **Lambda parameter types from later arguments.** The rule that a strategy reads only the
  arguments before the lambda ([*Known limitations*](#known-limitations)) can be widened so that a
  strategy may also read the non-lambda arguments that follow it, allowing lambda-first signatures
  such as ClickHouse's `arrayMap(func, arr)`. The Table API resolver would resolve every non-lambda
  argument first and the lambdas second, from left to right, as SQL already binds them; both
  surfaces would show a lambda that is not bound yet in the same way; and the Javadoc would state
  the wider rule. A strategy written for the narrow rule sees a superset of what it reads today, so
  this is purely additive. It is tied to no layer, since a layer-3 function can put its lambda last.
  Open question: whether a lambda may depend on a later lambda (proposed: no).
- **A merged `USING` column in a lambda body.** After the parameter lookup, Flink would recognize
  an unqualified `USING` or `NATURAL` join column ([*Known limitations*](#known-limitations)) and
  rewrite it into the `COALESCE` the select list gets, built in `FlinkSqlLambdaScope` or
  `FlinkCalciteSqlValidator` because the select list's version is in the private
  `SqlValidatorImpl.Expander`. It is additive, changes no plan format, and is tied to no layer.
- **Items that depend on layer 3.** The parent FLIP also lists future work that only makes sense
  once user-defined functions accept lambdas: a host-language lambda factory for user-defined calls,
  named lambda arguments for scalar and table functions, lambda arguments for aggregate functions,
  and captures, `onTimer` access and optional lambda arguments for process table functions. They are
  not repeated here.

## Calcite fork: what is upstream, what is residual

Calcite's lambda support (CALCITE-3679) targets *self-contained* lambdas, whose body references
only the lambda's own parameters. The closures and nesting of layer 1 need changes inside Calcite's
validator, converter and decorrelator; step 0 touches no Calcite file. Two upstream issues remove
most of them, in **different releases**, so the fork shrinks in two steps:

- [CALCITE-7497](https://issues.apache.org/jira/browse/CALCITE-7497) ("Support constant folding in
  Lambda expressions"; Closed/Fixed, fixVersion **1.42.0, released**; commit
  [`fa951db4b900`](https://github.com/apache/calcite/commit/fa951db4b900f16a74d087688096e98fb7dadf1e)).
  Its `RexShuttle.java` hunk (+6 / −2) makes `visitLambda` rebuild the lambda if the body changed,
  unconditionally (no conformance flag or rule):

  ```java
  @Override public RexNode visitLambda(RexLambda lambda) {
    RexNode oldBody = lambda.getExpression();
    RexNode newBody = oldBody.accept(this);
    if (newBody == oldBody) {
      return lambda;
    }
    return new RexLambda(lambda.getParameters(), newBody);
  }
  ```

  Each rewriting shuttle overrides a leaf `visitXxx` and relies on the base class to reassemble the
  tree, so this covers the first three rows of the table below.
- [CALCITE-6242](https://issues.apache.org/jira/browse/CALCITE-6242) ("Enhance lambda closure";
  Resolved/Fixed, fixVersion **1.43.0, unreleased**; commit `81d93213e6d2`) implements much of the
  *validation and conversion* side, from the same starting point
  (`Param 'T2.V' not found in lambda expression`).

**Delivery baseline.** Flink pins Calcite **1.41.0**, and layer 1 ships the **1.41.0 column** of
the table below; a Calcite upgrade is its own FLIP-sized change. Apart from imports and class
Javadoc, every change to upstream code sits in a `FLINK MODIFICATION` block that names the JIRA key
and version subsuming it or, if it survives 1.43.0, says why.

### Classification against the three upstream states

"carried" means Flink must vendor the change; "upstream" means the released or planned Calcite
does it and the Flink block becomes a deletion. Rows are logical changes, some of them in
Flink-owned files rather than in vendored copies.

| Flink change | 1.41.0 (pinned, shipped) | 1.42.0 (released) | 1.43.0 (planned) | Upstream equivalent |
| --- | --- | --- | --- | --- |
| `RelDecorrelator.DecorrelateRexShuttle.visitLambda` rebuilds the rewritten body | carried | **upstream** | upstream | CALCITE-7497 `RexShuttle.visitLambda` |
| `RelDecorrelator.RemoveCorrelationRexShuttle.visitLambda` rebuilds the rewritten body | carried | **upstream** | upstream | CALCITE-7497, same base-class fix |
| `SqlToRelConverter.getCorrelationUse`: lambda-aware correlation de-duplication | carried | **upstream** | upstream | CALCITE-7497, via `DeduplicateCorrelateVariables`'s shuttle |
| `FlinkSqlLambdaScope.fullyQualify` delegates to the parent scope | carried | carried | **upstream** | CALCITE-6242, gated on the new `SqlConformance.allowLambdaClosure()` |
| `FlinkSqlLambdaScope.resolveColumn` delegates to the parent scope | carried | carried | **upstream** | CALCITE-6242 |
| `SqlValidatorImpl.registerFrom` registers lambda operands under the lambda scope | carried | carried | **upstream** | CALCITE-6242 (upstream registers only operand 1, the body) |
| `SqlToRelConverter.convertLambda` inherits the enclosing `nameToNodeMap` (enclosing lambda parameters, and the columns of a stand-alone expression) | carried | carried | **upstream** | CALCITE-6242 |
| `SqlToRelConverter.Blackboard.lookupExp` falls through instead of asserting | carried | carried | **upstream** | CALCITE-6242 |
| `RexUtil.isDeterministic` descends into `RexLambda` bodies | carried | carried | **upstream** | CALCITE-6242 makes `RexVisitorImpl.visitLambda` recurse when `deep` |
| `RelDecorrelator.CorelMapBuilder`'s visitor descends into lambda bodies | carried | carried | **upstream** | CALCITE-6242, same mechanism (`RexVisitorImpl(true)`) |
| `FlinkSqlToRelConverter.createJoin` / `.createAsofJoin` lift join-condition captures before push-down | carried | carried | **upstream** | CALCITE-7497 (re-indexing shuttles) + CALCITE-6242 (`RelOptUtil.InputFinder` descent) |
| `FlinkSqlLambdaScope.isParameter` walks enclosing lambda scopes | carried | carried | carried | none (residual 1) |
| `FlinkCalciteSqlValidator.validateParameterizedExpression` / `.validateCall` scope a lambda in a stand-alone expression | carried | carried | carried | none (residual 2) |
| `SqlToRelConverter` cross-query-boundary capture (`isSameQueryLambdaCapture`) | carried | carried | carried | none (residual 3) |
| `SqlToRelConverter.convertLambda` roots the body at the post-aggregation row | carried | carried | carried | none (residual 4) |
| `SqlToRelConverter` cumulative nested-parameter indices and renumbering | carried | carried | carried | none (residual 5) |
| `RelDecorrelator` preserves a higher-order call's return type | carried | carried | carried | none (residual 6) |
| `SqlValidatorImpl.OrderExpressionExpander` leaves a lambda alone | carried | carried | carried | none (residual 7) |
| `SqlCastFunction` accepts a placeholder while a lambda parameter is unbound | carried | carried | carried | none (specific to Flink's deferred operand binding) |

Two CALCITE-6242 decisions, verified against Calcite `main`, matter at the 1.43.0 upgrade: Flink's
conformance must enable `SqlConformance.allowLambdaClosure()`, and upstream **rejects** a lambda
parameter shadowing an enclosing one (`RESOURCE.duplicateLambdaParameter`), which layer 1 allows,
documents and tests; that difference is to be raised upstream rather than adopted.

### Residual after both upstream fixes

These changes survive 1.43.0, ranked by value to a future upgrade. Residuals 2, 3, 5, 6 and 7 are
described from Flink's side under [*Lambda captures*](#lambda-captures), 1 and 4 under
[*SQL planner*](#sql-planner-flink-table-planner).

1. **`AggChecker` reports an enclosing lambda parameter as ungrouped.**
   `SqlLambdaScope.isParameter` matches only the scope's own parameters, upstream included, so
   `ARRAY_TRANSFORM(a, e -> ARRAY_TRANSFORM(a, x -> x + e)) ... GROUP BY a` reports `e` as not
   grouped. `FlinkSqlLambdaScope.isParameter` walks the enclosing lambda scopes of the same query,
   not past a sub-query, whose own table aliases come first. The most valuable residual to upstream:
   a plain bug, a one-method fix in a public non-final class, the natural follow-up to CALCITE-6242.
2. **A lambda in a stand-alone expression gets no scope.** `validateParameterizedExpression`
   (computed columns, watermarks, `callSql`) registers no lambda scope, and `ParameterScope`
   returns itself from `getOperandScope`, so `ARRAY_TRANSFORM(arr, x -> x + 1)` fails there with
   "Unknown identifier 'x'", also on Calcite `main`. A plain bug; the upstream fix is the same two
   small changes inside the validator.
3. **Capturing across a query boundary loses its correlation.** CALCITE-6242's `lookupExp` sets
   `isParent = false` for *any* resolution from a lambda scope that has inputs, which is wrong for a
   lambda in a scalar sub-query capturing an outer-query column: that must keep Calcite's correlated
   `DeferredLookup` / `RexCorrelVariable` path. A bug in CALCITE-6242 as committed; Flink's
   `isSameQueryLambdaCapture` requires the resolved ancestor to be the enclosing query block.
4. **A lambda body is converted against the pre-aggregation row.** `convertLambda` leaves the root,
   `AggConverter` and registered sub-queries of the lambda `Blackboard` unset, upstream included,
   so a captured group key resolves against the pre-aggregation row and an aggregate in the body is
   never replaced by its registered reference ([*SQL planner*](#sql-planner-flink-table-planner)).
5. **Nested `RexLambdaRef`s cannot be told apart in the IR.** `RexLambdaRef` is *positional*, so a
   free reference to an enclosing lambda's parameter looks like the inner lambda's first parameter.
   CALCITE-6242 inherits the enclosing `nameToNodeMap` but does not renumber; layer 1 gives nested
   parameters distinct indices during conversion, then lifts the captures and renumbers. The fix, a
   scope-qualified `RexLambdaRef` such as a `(depth, index)` pair, belongs upstream but touches the
   IR, serialization and plan digests, so it is the least self-contained of these.
6. **A higher-order call's result type does not change under decorrelation.** `RelDecorrelator`
   re-derives a call's return type whenever an operand changed, which for a rewritten lambda yields
   `ANY`, although the correlated reference is replaced by an input reference of the same type.
7. **The `ORDER BY` expansion rebuilds a lambda.** `SqlValidatorImpl.OrderExpressionExpander` lacks
   `Expander`'s `case LAMBDA`, so it replaces a body name matching a select-list alias with the
   aliased expression and copies the lambda as a plain `SqlBasicCall`, which fails type inference
   with an internal error. Calcite `main` has the same gap and no JIRA issue yet; it is a plain bug,
   worth filing. The vendored block leaves a lambda alone; the scope side is Flink-owned
   ([*Names in a lambda body in `ORDER BY`*](#lambda-captures)).

**Flink-side companions** live in `org.apache.flink.*` and are not upstream candidates:

- **1.42.0** (CALCITE-7497) deletes the `visitLambda` override of `HigherOrderFunctionUtil`'s
  capture-lifting shuttle; the one in `FlinkDecorrelateProgram`'s post-decorrelation re-lift, a
  `RexShuttle`, can then be reconsidered.
- **1.43.0** (CALCITE-6242) deletes `FlinkSqlToRelConverter`; the `visitLambda` override of the
  leftover-correlation check, a deep `RexVisitorImpl`, can then be reconsidered.
- Unlike the vendored blocks, neither `FlinkDecorrelateProgram` override has to be removed.

**A Calcite bug worked around in Flink-owned code.** `SqlLambda.SqlLambdaOperator#deriveType`
orders a lambda's parameter types by hash, not declaration; `CallBindingCallContext` reads them by
name ([*SQL planner*](#sql-planner-flink-table-planner)), so Flink vendors no `SqlLambda`. The
upstream fix, iterating the declared parameters and looking each one up, is worth filing.

**Flink's own visitors.** Independently of CALCITE-6242, a Flink `RexVisitorImpl` / `RexShuttle`
skips lambda bodies unless it opts in, as the leftover-correlation check,
`ShortcutUtils.isDeterministicThroughProgram` and `FlinkRexUtil.getNonDeterministicCallName` do
(*Lambda-body determinism* under [*Lambda captures*](#lambda-captures)). The others are audited:
a lambda reaching them is closed, so the input-reference, correlation, nested-field and
time-indicator finders see every capture in the operands; or a body cannot contain what they look
for (`AsyncCalcSplitRule` and `PythonCalcSplitRule` are why a body rejects asynchronous and Python
functions). Calcite's traversals reach an unlifted lambda only in join-condition push-down, hence
`FlinkSqlToRelConverter`. `RexNodeToExpressionConverter` declines a lambda by design.

**Also not proposed upstream:** the closed-`RexLambda` model, which capture lifting accommodates
without an IR change; and a public operand checker for "array plus a lambda over its element"
(Calcite's `OperandTypes.EXISTS` hard-codes `EXISTS` in its signature message and cannot defer
while the array type is unresolved), which a Flink-side checker covers.

### Why the changes cannot be applied through hooks or subclasses, and what they cost

Everything that works outside a vendored copy lives in Flink-owned code, with no rebase cost:

- `FlinkSqlLambdaScope`, a subclass of the public, non-final `SqlLambdaScope`.
- `FlinkCalciteSqlValidator.validateLambda`: the `cap$` prefix, duplicate-name and body rules.
- `FlinkSqlToRelConverter.createJoin` / `.createAsofJoin`: join-condition capture lifting.
- `validateParameterizedExpression` / `validateCall`: a lambda in a stand-alone expression.

**Layer 1 copies no new Calcite class.** The rest has no reachable extension point in
`calcite-core-1.41.0`
([appendix A5](#a5-why-the-calcite-changes-cannot-be-applied-through-hooks-or-subclasses)), so
layer 1 patches five Calcite classes that Flink **already vendors**: **18** new `FLINK MODIFICATION`
blocks beside **36** existing ones (the cost argument uses the 18, not the 54), of which ten are
scheduled deletions (6 at **1.42.0**, 4 at **1.43.0**; per file in
[appendix A6](#a6-vendoring-cost)). The Flink-owned `ExtraCalciteResource` under
`org/apache/calcite/**` gains six uncounted validator messages: reserved parameter name, duplicate
parameter name, unsupported construct in a body, construct over a lambda parameter, sub-query over
a lambda parameter, and select-list alias in an `ORDER BY` lambda body.

## Appendix (non-normative)

Supporting evidence for reviewers; **nothing here is normative**. The numbering follows the
parent FLIP; its A1 (the unabridged test enumeration), A4 (the Spark bug cited under
[*Known limitations*](#known-limitations)) and A7 (process table function examples) are omitted.

### A2. `LogicalTypeRoot.FUNCTION`: exhaustive-switch audit (site by site)

The principle, choke points and entry guards are in
[*Type system*](#type-system-flink-table-common). Every exhaustive `switch` over `LogicalTypeRoot`
falls into one of four categories:

1. **Explicitly handled: the lambda planning path.**
   - `FlinkTypeFactory`: `FunctionType` converts to `FunctionRelDataType`, a new `@Internal`
     `AbstractSqlType` (`SqlTypeName.OTHER`) that carries the parameter count and renders it in
     plan digests. The reverse direction also accepts Calcite's `FunctionSqlType`, which the
     validator produces for a SQL lambda and which keeps the parameter and result types.
   - `FunctionType` picks its conversion classes (see [*Public interfaces*](#public-interfaces))
     by parameter count, the only reason the count is part of the type. A lambda with lifted
     captures has an arity outside the tables and falls back to `java.util.function.Function` and
     `FunctionData1`, which `supportsInputConversion` and `supportsOutputConversion` reject. Neither
     reaches a function: `OperatorBindingCallContext` excludes the captures from the `FUNCTION` type
     that type inference and `eval()` see. In layer 1 only built-ins receive a lambda, in the
     internal form; the external classes become reachable for users in layer 3.
   - `CodeGenUtils.boxedTypeTermForType` maps `FUNCTION` to `java.util.function.Function`. Its
     `match` has no default case, so the type needs a case of its own.
   - `SqlTypeFactoryImpl` (already vendored) needs no change: it handles `FunctionSqlType`.
   - `LogicalTypeVisitor.visit(FunctionType)`: a **source-compatible default method**, so existing
     visitors still compile. No visitor overrides it, so a `FunctionType` reaches each visitor's
     `visit(LogicalType)` fallback.
2. **Explicitly rejected with a clear message: the five choke points** (four materialization guards
   plus the cast guard).
   - `InternalSerializers` throws a `ValidationException` ("The FUNCTION data type is a helper type
     for lambda arguments …") for every runtime materialization of a value
     (`InternalSerializersTest#testFunctionTypeCannotBeMaterialized`).
   - `DefaultSchemaResolver` keeps the type out of a catalog, whether through `CREATE TABLE`, a
     `TableDescriptor` or CTAS, and out of a schema that a catalog hands back
     (`SchemaResolutionTest#testFunctionTypeRejectedAsColumnType`,
     `HigherOrderFunctionsBatchITCase#testFunctionTypeRejectedAsSchemaColumn` and
     `#testFunctionTypeRejectedOnCatalogRoundTrip`). SQL DDL has no syntax for the type anyway.
   - `TypeInferenceUtil#inferOutputType` (a **return** type) and `#inferStateInfo` (a **state**
     type: an aggregate accumulator or a process table function's named state entry) reject it
     before any serializer or state code is generated; reflective extraction already rejects a
     `FUNCTION` state as non-composite. Pinned by
     `HigherOrderFunctionsBatchITCase#testFunctionTypeRejectedAsReturnType`,
     `#testFunctionTypeRejectedAsAccumulatorType` and `#testFunctionTypeRejectedAsNamedStateEntry`.
   - `LogicalTypeCasts` rejects every cast to or from the type except the identity, and lambda
     arguments are never implicitly cast anyway (`LogicalTypeCastsTest`,
     `LogicalTypeCastAvoidanceTest`).
   - **Entry guards.** The `StaticArgument` constructor rejects a `FUNCTION` type in a static
     signature, top-level ("Lambda arguments are currently only supported by built-in functions,
     which declare them in their input type strategy …") or nested ("… must not be nested in
     another data type"); otherwise a `NULL` or omitted argument would fail internally in code
     generation or in the `FUNCTION`-to-`RelDataType` conversion. A specialized
     `BuiltInScalarFunction` does not trip it ([*Type system*](#type-system-flink-table-common)).
     `ValueLiteralExpression` rejects a `FUNCTION` literal (`nullOf(DataTypes.FUNCTION(1))`), which
     would reach a built-in's type inference without `LambdaInfo`. The two guards are pinned by
     `UserDefinedFunctionLambdaArgumentTest#testFunctionTypedStaticArgumentIsRejected` and
     `#testFunctionTypedLiteralIsRejected`. Layer 3 accepts a non-optional top-level `FUNCTION` in
     `StaticArgument.checkLambdaType`, and `SystemTypeInference.checkLambdaArgs` then requires a
     process table function whose input type strategy implements `LambdaInputTypeStrategy`; the
     nested part stays.
3. **Unreachable data and serde paths, guarded by throwing (or harmless) defaults.** No other
   exhaustive `switch(LogicalTypeRoot)` lists `FUNCTION`, among them `LogicalTypeMerging`,
   `LogicalTypeChecks`, the `RowData` / `ArrayData` field getters and setters, `ToVariantConverter`
   (the writer behind `CAST` to `VARIANT`), and both the compiled-plan and the SQL-gateway REST
   `LogicalTypeJsonSerializer`s. The cast guard runs before the rule that casts a constructed type
   to `VARIANT` leaf by leaf, so no `FUNCTION`, top-level or nested, reaches `ToVariantConverter`.
   Most sites end in a throwing `default` (`UnsupportedOperationException`,
   `IllegalArgumentException`, `TableException`); a few same-root predicates
   (`TypeCheckUtils.isReference`, `LogicalTypeChecks.areTypesOfSameRootComparable`) return a
   harmless `true`. Category 2 makes `FUNCTION` unreachable here, and a future leak onto a data
   path would fail loudly. `leastRestrictive` is the one deliberate exception
   ([step 0](#step-0-narrowing-the-generic-raw-guard-in-leastrestrictive)).
4. **The type is never serialized in compiled plans**, neither as `FunctionType` nor as
   `FunctionRelDataType` ([*Compiled plan*](#compiled-plan-flink-table-planner)), so
   `LogicalTypeJsonSerializer` / `RelDataTypeJsonSerializer` need no `FUNCTION` case.
   - Python never surfaces `FUNCTION` as a `DataType`, so it has no type-mapping entry.
   - `LogicalTypeParser` reads `FUNCTION(n)` with a required precision (`parseRequiredPrecision`).
   - The wrapper nullability is canonicalized on the way back (see
     [*Compatibility*](#compatibility-deprecation-and-migration-plan)).

### A3. Code-generation mechanics

The body states the guarantees these mechanics produce; this section records why the generated code
has that shape. The figures apply to layer 1 as is: its runtime classes are byte-for-byte the
measured ones, and its function-object generator differs from the full branch only in the visibility
of two methods (widened by layer 3 for process table functions).

#### A3.1 Where a built-in higher-order function's loop lives

- **In the runtime class, not in the planner.** Each of the four is a `BuiltInScalarFunction`
  (`ArrayTransformFunction`, `ArrayFilterFunction`, `ArrayReduceFunction`, `ArrayZipWithFunction`)
  that reads its array as `ArrayData` and receives its lambda as the matching `FunctionDataN`, so
  the loop runs on internal data and the lambda application crosses no conversion boundary. There
  are four classes because the loop shapes differ: map-and-collect (`ARRAY_TRANSFORM`),
  conditional-collect (`ARRAY_FILTER`), accumulator fold (`ARRAY_REDUCE`), and a walk over two
  inputs to the longer length with `NULL` padding (`ARRAY_ZIP_WITH`).
- **Every buffered body result is copied** with the result type's serializer, whether appended to
  the result or carried on as `ARRAY_REDUCE`'s accumulator: a mutable result (`ARRAY[...]`,
  `ROW(...)`) lives in a *reusable* `BinaryArrayData` / `BinaryRowData` member that the next
  iteration overwrites, so uncopied elements would all alias the last one. `ARRAY_FILTER` buffers
  source elements, which the element getter returns as fresh views, so it copies nothing. For an
  empty array, `ARRAY_REDUCE` returns the initial accumulator itself, which stays valid when a
  downstream operator buffers it (`ArrayReduceAccumulatorOwnershipITCase`).
- **The 64 KB per-method limit is handled by the structure, not by style.** A lambda body can exceed
  the JVM's limit on its own, and `flink-table-code-splitter` only descends into `if` / `else` /
  `while` blocks (`BlockStatementGrouper`), so a loop generated by the planner would have to emit
  the body as a `while`. Here the body sits at the top of its own evaluator class, split like the
  same expression outside a lambda, and the splitter never sees the hand-written loop
  (`testLargeLambdaBodyIsSplitIntoSeparateMethods`, [*Test plan*](#test-plan)).

#### A3.2 The generated function object for a lambda

The behaviour is specified in [*The generated function object*](#the-generated-function-object);
this section records how.

- **Opening without a runtime context.** An evaluator is opened with the context that
  `CodeGeneratorContext#functionContextCode` produces. The watermark generator pushed into a source
  and the planner's constant-reduction and partition-pruning function have no runtime context; both
  already override how they open a user-defined function, and override `functionContextCode`
  likewise, with a `FunctionContext` built from the class loader and the open context.
  `WatermarkGeneratorCodeGeneratorFunctionContextWrapper` gains a constructor taking both and still
  exposes the source's watermark-context metric group
  (`HigherOrderFunctionsBatchITCase#testUserDefinedFunctionInPushedDownWatermarkLambdaUsesSourceMetricGroup`).
- **The context and the metric handles travel with the evaluator**, in the private `OpenContext` of
  `DefaultExpressionEvaluator.openLambdaFactory(FunctionContext, UdfMetrics[])`.
  - The generated code reads the context back with `DefaultExpressionEvaluator.functionContextOf`
    wherever it opens a user-defined function or a nested evaluator, so the evaluator's
    code-generator context overrides only `functionContextCode`, not `addReusableFunction`.
  - With `table.exec.udf-metric-enabled`, that context registers no handle: it records the function
    name and reads the handle at that position from the carrier (`udfMetricsOf`). The call site
    registers the recorded names in the *enclosing* context before the `openLambdaFactory`
    statement, and a nested evaluator's names propagate outward hop by hop to the operator: one
    handle per function name and operator (`UdfMetricsITCase#testLambdaBodySharesOneHandle`).
  - The public `open(FunctionContext)` of a `SpecializedFunction.ExpressionEvaluator` passes the
    given context and no handles; `udfMetricsOf` then registers one in that context's metric group.
- **The body-type coercion ignores nullability.** A nullable body is only ever handed to a nullable
  parameter, so a coercing `CAST` would only restate the body's own type.
- **Nested evaluators need distinct class names.** A nested evaluator is compiled with the enclosing
  evaluator's (Janino) class loader as parent, so its `CodeGeneratorContext` takes the enclosing
  context as parent to share the name counter, and nothing else.

#### A3.3 What the lambda ABI costs

These measurements compare the function-object ABI with the two rejected designs, an `apply`
through a `MethodHandle` and loops inlined by the planner
([*Rejected alternatives*](#rejected-alternatives)); the isolated harness backs the claims.

- **The invocation ABI.** Janino lacks the signature polymorphism of `MethodHandle#invoke`, so an
  `apply` holding a handle must call `invokeWithArguments`, with a fresh `Object[]` of boxed
  arguments (captures included) and an `asType` adaptation on **every** application. A test-only
  evaluator implementing the interface directly was **2.2×–4.7×** faster in a single-parallelism
  batch pipeline (five alternating repetitions per mode, only the invocation differing): 3.63×,
  4.70× and 3.27× for 16-, 256- and 1,024-element arrays with a trivial body, 4.18× for an expensive
  body, and 2.17× for a `STRING` result. JFR put 15.2% of CPU samples and 9.5% of sampled
  allocation weight on `invokeWithArguments` (`BoundMethodHandle$Species_L`, `Class[]` adapter
  churn), diluted by two other modes.

**The isolated measurement.** A JMH harness (JMH 1.37, JDK 17, Apple M1 Pro, two forks, five
one-second iterations after three of warmup, `-prof gc`) runs the shipped
`ArrayTransformFunction.eval` loop against the rejected prototype's loop, transcribed verbatim from
`HigherOrderFunctionCallGen.generateArrayTransform`, over the same `BinaryArrayData`, with `apply`
reproduced as the ABI it is: an interface call, a cast per user parameter, and a typed `eval`. Both
produce the identical `GenericArrayData`, so everything downstream is common and excluded. Per
element, for `ARRAY_TRANSFORM(a, x -> …)` over `ARRAY<INT>`, as time and allocation:

| body | length | shipped runtime class | inlined prototype | inlined, presized `Object[]` | inlined, straight to binary | shipped / prototype |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| trivial | 16 | 7.40 ns / 38 B | 6.83 ns / 38 B | 4.20 ns / 22 B | 2.02 ns / 2 B | 1.08× |
| trivial | 256 | 7.50 ns / 36 B | 5.73 ns / 38 B | 4.03 ns / 20 B | 1.49 ns / 0 B | 1.31× |
| trivial | 1,024 | 7.21 ns / 36 B | 5.52 ns / 35 B | 3.57 ns / 20 B | 1.44 ns / 0 B | 1.30× |
| trivial | 16,384 | 6.01 ns / 36 B | 5.03 ns / 36 B | 3.60 ns / 20 B | 1.42 ns / 0 B | 1.19× |
| expensive | 16 | 8.59 ns / 38 B | 8.36 ns / 38 B | 6.05 ns / 22 B | 4.63 ns / 2 B | 1.03× |
| expensive | 256 | 8.27 ns / 36 B | 7.33 ns / 38 B | 5.95 ns / 20 B | 4.10 ns / 0 B | 1.13× |
| expensive | 1,024 | 8.13 ns / 36 B | 7.15 ns / 35 B | 6.33 ns / 20 B | 4.05 ns / 0 B | 1.14× |
| expensive | 16,384 | 7.34 ns / 36 B | 6.51 ns / 36 B | 5.80 ns / 20 B | 4.01 ns / 0 B | 1.13× |

`ARRAY<STRING>` shows no difference: 0.96×–1.07× across the same lengths and both bodies, at
68–82 ns and 520–590 bytes per element, as the body and the copy of its result dominate everything
the ABI does. The pipeline's `STRING` figures (2.17× for the ABI, 0.57× against the inlined
built-in) measured a string copy, not an invocation: the inlined built-in copied each result into a
`BinaryStringData`, while the prototype's external path stored a `String` reference. The presized
and straight-to-binary columns are controls
([A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)).

- **What the comparison says.** Against the rejected design, the band is 1.03×–1.31×: at most 1.8 ns
  per element on the cheapest shape a higher-order function has, under 1 ns once the body costs
  anything, and nothing measurable when the element type is not a primitive. It does not support
  inlining. The inlined
  prototype boxed each element into a `java.util.ArrayList` and passed `toArray()` to a
  `GenericArrayData`; the shipped form presizes an `Object[]` from `array.size()`, so it wins on the
  output side and loses only on the input side. There the difference is boxing, not dispatch:
  36 bytes per element against the presized control's 20 is one extra `Integer`, which
  `elementGetter.getElementOrNull` must create before `FunctionDataN#apply` can accept the argument.
- **One runtime loop is shared by every call site.** `ArrayTransformFunction.eval` is a single
  JIT-compiled method, so its `lambda.apply` site sees one generated class per distinct lambda in
  the TaskManager, where an inlined loop would have a private site per query. At 256 elements,
  driving 1/2/3/5 distinct lambda classes through it costs 6.84/7.36/7.82/7.53 ns per element
  against a flat 5.7 ns for a private loop: real, capped by the megamorphic plateau, and about 1 ns
  per element. Primitive specialization would remove it by splitting the site per element type.

#### A3.4 The benchmark protocol

The standing benchmark ([*Future work*](#future-work)) guards the
[A3.3](#a33-what-the-lambda-abi-costs) figures; each point notes what the measurement covered.

- **Compare four variants over the same collection:**
  - **(A)** the built-in `ARRAY_TRANSFORM(a, x -> x + 1)`;
  - **(B)** a user-defined higher-order function with the same signature applying its lambda per
    element (available from layer 3);
  - **(C)** the same arithmetic without a lambda, as the control;
  - **(D)** an assembly that boxes nothing and writes the result through a `BinaryArrayWriter`, as
    the headroom bound ([A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)).

  A and B are the same generated code and should coincide: the property most worth guarding, and the
  one not yet checked (the measurement covered A, C and D).
- **Report per element, not per record**, with the allocation rate: the per-element byte counts are
  the more stable signal and separate the variants where times overlap at short lengths.
- **Sweep four dimensions**, since one 1,000-element `INT` case cannot separate a fixed
  per-application overhead from a body-proportional one: length, from a few elements (per-call
  overhead dominates) to tens of thousands (allocation and cache dominate); body cost, trivial
  against expensive; result shape, a scalar against a constructed `ARRAY` / `ROW` (conversion and
  copying; not yet measured); element type, a boxed primitive against `STRING` or a nested type.
- **Measure on an isolated evaluator harness, not end to end**, with variance across repetitions: a
  pipeline-level point estimate cannot separate 0.95× from 1.04×.
- **Drive the shared runtime loop with several distinct lambda classes** in at least one
  configuration, since every TaskManager call site shares its type profile (covered at one length).

#### A3.5 The headroom, and how it comes out without inlining

This is the reasoning behind the follow-up in [*Future work*](#future-work), not a proposal; it also
explains the control columns in [A3.3](#a33-what-the-lambda-abi-costs). The shipped loop runs at
7.21 ns and 36 bytes per element; writing straight into binary takes 1.44 ns and nothing. The 4×–5×
gap has nothing to do with where the loop is generated and lies in three places, none intrinsic:

- **The input side.** `elementGetter.getElementOrNull(array, i)` returns `Object`, so a primitive
  element is boxed before `FunctionDataN#apply` can accept it: 16 of the 36 bytes.
- **The ABI itself.** The evaluator's `eval` signature is derived from each argument's
  `getConversionClass`, which for internal data is `LogicalTypeUtils#toInternalConversionClass`,
  boxed by definition. Inside the generated `eval`, `argMappingCode` converts the boxed argument to
  `primitiveTypeTermForType` with a separate `$0IsNull` flag, the body computes on the primitive,
  and `genToExternalConverterAll` boxes the result again. **The body is already primitive; the
  boxing is bookkeeping at the boundary on both sides of it.**
- **The output side.** The result box, the `Object[]` slot and the `GenericArrayData` around them,
  plus a `resultElementSerializer.copy` per element that does nothing for immutable element types.

Both hooks needed to remove this exist. A `BuiltInScalarFunction` is specialized per call site:
`ArrayTransformFunction`'s constructor already inspects `getArgumentDataTypes()` to build its
element getter, and could pick a primitive loop and a `BinaryArrayWriter` there, without the
planner. And the lambda's interface is whatever the `FUNCTION` type's conversion class names,
through `LambdaFactorySpec(paramFields.size, lambdaDataType.getConversionClass)`, so a built-in can
nominate an `@Internal` primitive interface without touching `FunctionDataN` or the public
`FunctionN`.

The evaluator generator is already primitive-aware (`externalResultCasting` handles a primitive
`externalResultClass` through `primitiveToWrapper`, so `eval(int)` returning `int` can be generated
as the code stands). The single blocker is `generateBindLambda`: its `apply(Object arg0, …)` takes
one `Object` per user-visible lambda parameter and returns `Object`, so it cannot implement a
primitive interface method, and `LambdaFactorySpec` would have to carry the target signature, not
only the interface. Two things make this a follow-up rather than part of this design:

- **Nullability is the design question**, not the machinery: internal conversion classes are boxed
  so that a null has a representation. Either the fast path is restricted to `NOT NULL` element and
  body types (array literals qualify, many real sources do not), or the interface gains a null
  channel mirroring code generation's `(resultTerm, nullTerm)` pair, making the function object
  stateful on a path that must survive nesting and re-entrancy.
- **The matrix does not stay small.** An interface per arity and primitive type suits one-parameter
  iterators; `ARRAY_REDUCE`'s `(acc, element)` multiplies out two independently typed ones.

The payoff splits about evenly. At 1,024 elements with a trivial body, removing the input box, the
interface call, the cast and the `copy` is worth 3.64 ns and 16 bytes per element, and removing the
result box, the `Object[]` and the `GenericArrayData` is worth 2.13 ns and 20 bytes. A half-measure
buys about half. Correctness needs no new gate: the runtime class is the executable reference
semantics, so a specialization is an internal swap that must leave every test result unchanged.

### A5. Why the Calcite changes cannot be applied through hooks or subclasses

**What is reachable from outside**, verified like the table below against `calcite-core-1.41.0`:

- `SqlLambdaScope` is public and non-final, and its three resolution methods (`fullyQualify`,
  `resolveColumn`, `isParameter`) are public. `DelegatingScope.parent` / `.validator` are
  `protected final`, and `DelegatingScope.getParent()` and `SqlLambdaScope.getParameterTypes()`
  are public. Hence `FlinkSqlLambdaScope`.
- `SqlValidatorImpl.validateLambda`, `.getLambdaScope` and `.newValidationError` are public. Hence
  the `FlinkCalciteSqlValidator` override.
- `SqlToRelConverter.createJoin`, `.createAsofJoin`, its `Blackboard` type and its `rexBuilder`
  field are protected. Hence `FlinkSqlToRelConverter`.
- `SqlValidatorImpl.validateParameterizedExpression` and `.validateCall` are public, and the
  six-argument `registerQuery` is protected. Hence the stand-alone-expression overrides in
  `FlinkCalciteSqlValidator`, which register an expression's lambdas themselves rather than
  through the private `validateScopedExpression`.

**What is not**, change by change:

| Change | Blocker |
| --- | --- |
| Create `FlinkSqlLambdaScope` instead of `SqlLambdaScope`; register lambda operands under the lambda scope | `SqlValidatorImpl.registerFrom` and `.registerOperandSubQueries` are **private**, and there is no factory or SPI for creating scopes |
| `convertLambda` blackboard mirroring and cumulative parameter indices; `Blackboard.lookupExp` resolution | `SqlToRelConverter.convertLambda` is **private**; `Blackboard.lookupExp` is package-private and called only from the private `convertIdentifier` |
| Lambda-aware correlation de-duplication | `SqlToRelConverter.getCorrelationUse` is **private**, and `DeduplicateCorrelateVariables`'s shuttle is a **private static** nested class |
| `RelDecorrelator` lambda descent and type preservation | `DecorrelateRexShuttle` is a **private static** nested class and `RemoveCorrelationRexShuttle` a **private** inner class. `CorelMapBuilder` is **public static**, but its lambda descent sits in an anonymous `RexVisitorImpl` that it creates internally. `RelDecorrelator` instantiates all three itself, so a subclass of any of them would never be used. `RexShuttle.visitLambda`'s behaviour is fixed in the base class in 1.41.0. CALCITE-7497 changes it in 1.42.0, but only in the base class, so on the pinned version there is still no override point outside the copy |
| `RexUtil.isDeterministic` lambda descent | A **public static** method with an anonymous visitor, called from Calcite's own rules. A Flink-side alternative would not be consulted |
| `SqlCastFunction` accepts an unresolved lambda parameter | `canCastFrom` is **private**, and `SqlCastFunction` is instantiated from `SqlStdOperatorTable` |
| The `ORDER BY` expansion leaves a lambda alone | `OrderExpressionExpander` is a **package-private** inner class. The public `expandOrderExpr` that runs it could be overridden only by re-implementing the expander, i.e. by a second copy of the code |

### A6. Vendoring cost

The table counts the five patched classes, all **already vendored** in `flink-table-planner`. The
Flink-owned `ExtraCalciteResource`, the sixth file changed under `org/apache/calcite/**`, has no
rebase cost and is not counted.

Blocks are counted as matched `FLINK MODIFICATION` `BEGIN` / `END` pairs. Layer 1's blocks use the
standard spelling `// ----- FLINK MODIFICATION BEGIN -----`; four pre-existing ones read
`// FLINK MODIFICATION BEGIN`, without the dashes: three in `SqlValidatorImpl` (`registerNamespace`,
the `UPDATE` `SET` source-expression lookup of FLINK-40937, and `validateMeasure`) and one in
`SqlToRelConverter`. A grep for `FLINK MODIFICATION BEGIN` matches both spellings and reproduces the
"Total" column; a grep for `FLINK MODIFICATION` counts marker *lines* and roughly doubles the
answer. The counts are taken on this branch against the base commit `dab40d31eba`.

| File | Pre-existing blocks | Added in layer 1 | Total | Deleted at 1.42.0 | Deleted at 1.43.0 | Survives 1.43.0 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `SqlValidatorImpl` | 16 | 3 | 19 | 0 | 1 | 2 |
| `SqlToRelConverter` | 16 | 7 | 23 | 2 | 1 | 4 |
| `RelDecorrelator` | 4 | 6 | 10 | 4 | 1 | 1 |
| `RexUtil` | 0 | 1 | 1 | 0 | 1 | 0 |
| `SqlCastFunction` | 0 | 1 | 1 | 0 | 0 | 1 |
| **Total** | **36** | **18** | **54** | **6** | **4** | **8** |

**The cost argument uses the "Added in layer 1" column, 18**, since A6 is about the marginal cost of
this change, not the cumulative fork. The last three columns split those 18 blocks; each block's
comment states its own column. They are bookkeeping against the upstream fixes as the two JIRA
issues describe them, not the result of an upgrade that was run.

- **1.42.0 (CALCITE-7497), 6 blocks.** In `RelDecorrelator`: the two `visitLambda` overrides, plus
  the `rexBuilder` field and its initialisation, which exist only for the first of them. In
  `SqlToRelConverter`: the `getCorrelationUse` call site and its remap helper.
- **1.43.0 (CALCITE-6242), 4 blocks.** The operand registration in `SqlValidatorImpl.registerFrom`,
  the `Blackboard.lookupExp` fall-through, the `CorelMapBuilder` visitor and the `RexUtil`
  determinism descent.
- **Surviving, 8 blocks.** The `FlinkSqlLambdaScope` creation in `registerFrom` (upstream still
  creates a plain `SqlLambdaScope`), the `ORDER BY` expander (residual 7), the `SqlCastFunction`
  guard, and residuals 3 to 6. The first `convertLambda` block shrinks at 1.43.0, because the
  inherited `nameToNodeMap` moves upstream, but it keeps the cumulative indices (residual 5). The
  `SqlCastFunction` file is already heavily modified by Flink, without markers.

The marker style is **not** uniform, a fork-hygiene finding in its own right. `RexUtil`,
`SqlCastFunction` and `ExtraCalciteResource` had no markers; layer 1 adds them to the first two but
not to `ExtraCalciteResource`, whose six new validation messages are declarations in a Flink-owned
interface with no upstream text to fence off. Raw `BEGIN` / `END` counts do not balance in every
vendored file, because comments mentioning the markers match the same grep; the imbalances in
`SimpleCalciteSchema.java` (0/2) and `MultiJoin.java` (5/4) are pre-existing and unrelated. A
reversed spelling, `// BEGIN FLINK MODIFICATION`, occurs four times in files not touched here and
not counted: `SqlMapTypeNameSpec.java`, `LogicalTableScan.java` and `SqlMapValueConstructor.java`.
