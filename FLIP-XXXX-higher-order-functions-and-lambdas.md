# FLIP-XXXX: Higher-order functions and lambda expressions in Flink SQL and Table API

| | |
|---|---|
| **Status** | Under discussion |
| **Discussion thread** | https://lists.apache.org/thread/ky7kq96nv1wd7n1od8xytqcbkw44d7mk |
| **Vote thread** | — |
| **JIRA** | [FLINK-31207](https://issues.apache.org/jira/browse/FLINK-31207) |
| **Released** | — |

> **`FLIP-XXXX` is a placeholder.** The number is allocated by a committer and has to be substituted
> in the title *and* in this file's name before the FLIP can be circulated for a vote. It has not
> been invented here. This markdown file is a working copy: the deliverable is the page on the
> Apache Confluence wiki that the discussion and vote threads reference, and the two must be kept in
> sync.

> This proposal **supersedes** the earlier draft ([Higher-Order Functions in Flink SQL: TRANSFORM
> and ARRAY_FILTER](https://docs.google.com/document/d/144P06vspNDwU3nevEluPeOsWBQsaaQUhe8DouiEFqeQ/edit)). It specifies the complete design: lambda
> expressions, a `FUNCTION` type, built-in higher-order functions, Table API + Python support, lambda
> arguments for user-defined functions, and compiled-plan support.
>
> **What is complete, and what is incremental.** The *design* below is complete and is what a vote
> would authorize; a prototype branch implements and tests all of it, as evidence that it closes.
> The *code* is not offered as one merge: it lands as **four functional layers**, each its own merge
> unit that compiles, verifies and is releasable on its own, and each giving users strictly more
> than the one before — array functions in SQL, then map functions, then the Table API and Python,
> then lambda arguments for user-defined functions. The branch is already cut into exactly those
> four commits — behind one preparatory commit that layer 1's pull request opens with, five in
> all — so the layering is a reviewable fact rather than an intention. See
> [*Implementation → Delivery plan*](#delivery-plan).

## Motivation

Flink can build and inspect `ARRAY` values but cannot express element-wise computation inline. Today
this requires `UNNEST` + re-aggregation, which is verbose and changes the row structure of a query.
Every comparable engine solves this with *higher-order functions* driven by *lambda expressions* —
batch and interactive engines (Spark, Presto/Trino, Snowflake, Databricks) as well as streaming SQL
(ksqlDB).

Calcite added lambda support (CALCITE-3679) and Flink already vendors the corresponding
`RexLambda`/`FunctionSqlType` plumbing. This FLIP introduces lambdas as a first-class concept across
Flink's APIs and delivers a set of built-in array and map higher-order functions on top of them,
while making the mechanism extensible to user-defined functions.

```sql
SELECT
  ARRAY_TRANSFORM(vals, x -> x * 10),             -- [1,2,3]        -> [10,20,30]
  ARRAY_FILTER(vals, x -> x > 1),                 -- [1,2,3]        -> [2,3]
  ARRAY_REDUCE(vals, 0, (acc, x) -> acc + x),     -- [1,2,3], 0     -> 6
  ARRAY_ZIP_WITH(vals, ns, (x, y) -> x + y),      -- [1,2,3],[10,20,30] -> [11,22,33]
  MAP_FILTER(m, (k, v) -> v > 0),                 -- {a:1, b:-1}    -> {a:1}
  MAP_TRANSFORM_KEYS(m, (k, v) -> UPPER(k)),      -- {a:1, b:2}     -> {A:1, B:2}
  MAP_TRANSFORM_VALUES(m, (k, v) -> v * 10),      -- {a:1, b:2}     -> {a:10, b:20}
  MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> v1 + v2)    -- {a:1},{a:2,b:3}-> {a:3, b:NULL}
FROM my_table;
```

### Naming in other engines

Higher-order functions are well-established, so the names and shapes here follow existing engines
rather than invent new ones. The table maps each function proposed here to its counterpart elsewhere
(— = no direct equivalent):

| Flink | Spark / Databricks | Trino / Presto | Snowflake | ksqlDB |
|---|---|---|---|---|
| `ARRAY_TRANSFORM` | `transform` | `transform` | `TRANSFORM` | `TRANSFORM` |
| `ARRAY_FILTER` | `filter` | `filter` | `FILTER` | `FILTER` |
| `ARRAY_REDUCE` | `aggregate` (alias `reduce`) | `reduce` | `REDUCE` | `REDUCE` |
| `ARRAY_ZIP_WITH` | `zip_with` | `zip_with` | — | — |
| `MAP_FILTER` | `map_filter` | `map_filter` | — | `FILTER` (overloaded) |
| `MAP_TRANSFORM_KEYS` | `transform_keys` | `transform_keys` | — | `TRANSFORM` (combined) |
| `MAP_TRANSFORM_VALUES` | `transform_values` | `transform_values` | — | `TRANSFORM` (combined) |
| `MAP_ZIP_WITH` | `map_zip_with` | `map_zip_with` | — | — |

Two conventions differ across engines and motivate the choices here (see
[*Rejected Alternatives*](#rejected-alternatives)):

- **`ARRAY_*` prefix vs. bare array names.** Spark, Trino, Snowflake and ksqlDB name the array
  functions bare (`transform`, `filter`, `reduce`). Flink prefixes them `ARRAY_*` to stay consistent
  with its existing array built-ins and to avoid `TRANSFORM` colliding with Hive script-transform
  syntax and `FILTER` with the SQL `FILTER (WHERE …)` aggregate clause. The map functions already
  carry a `MAP_*` prefix in every engine that has them (Spark, Trino), which this FLIP follows.
- **Lambda syntax.** Spark, Trino, Snowflake and Flink (following Calcite) spell lambdas
  `x -> body`; ksqlDB uses `x => body`.

Notes on the equivalents:

-  **Snowflake** offers only the three array functions (`FILTER`/`TRANSFORM` since May 2024,
  `REDUCE` later that year), and has no `MAP` type — semi-structured data uses `OBJECT` — hence no
  map higher-order functions and no `*_ZIP_WITH`.
-  **ksqlDB** overloads `TRANSFORM`/`FILTER`/`REDUCE` across both arrays and maps, has no
  `zip_with`/`map_zip_with`, orders its reduce lambda accumulator-first like Flink's `(acc, x)`, and
  expresses a map key+value transform as a single `TRANSFORM(map, (k,v) => k2, (k,v) => v2)` call
  taking *two* lambdas — the combined form discussed and rejected in
  [*Rejected Alternatives*](#rejected-alternatives).
-  **Trino/Presto `reduce`** and Spark's `aggregate` additionally take an explicit output/finish
  function (`reduce(array, initialState, inputFunction(S,T,S), outputFunction(S,R))`); Flink's
  `ARRAY_REDUCE` omits the separate finish step, which is expressible by wrapping the call.
-  **Lambda arity.** No engine here allows more than three lambda parameters, but only two draw the
  line explicitly, and the split follows exactly whether a *user-defined* function may take a
  lambda. ksqlDB — the one other engine where it may — documents the bound ("Currently, ksqlDB
  supports up to three arguments in a single lambda function") and reaches it by the same
  construction as this FLIP, which is why this FLIP has a bound at all: a UDF may declare a lambda
  parameter only as one of a fixed set of functional interfaces, so the widest member of that set is
  the cap. The sets differ. ksqlDB borrows the JDK's and extends it by one, recognising `Function`,
  `BiFunction`, or its own `TriFunction` (`UdfUtil`, `FunctionLoaderUtils`); this FLIP declares a
  dedicated arity-named family instead, `org.apache.flink.util.function.Function0` through
  `Function4` (with `Function`, `BiFunction` and `TriFunction` accepted as equivalents at arities one
  to three), which is why Flink's bound lands one higher — see
  [*Known limitations*](#known-limitations) for that choice. In the engines
  where lambdas are built-in-only the bound never has to be stated, because each higher-order
  function fixes its own arity: Spark's grammar accepts `(a, b, c, d) -> …` and the widest built-in
  is `map_zip_with`'s `(key, value1, value2)`, with the cap surfacing only outside SQL — three
  `createLambda` overloads in `functions.scala`, and an explicit `1 <= arity <= 3` check in PySpark
  (`WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION`). Trino has no cap at all: every higher-order function
  supplies its own functional interface (`UnaryFunctionInterface`, `BinaryFunctionInterface`, or a
  nested one such as `MapZipWithFunction.MapZipWithLambda`), and those interfaces live in
  `trino-main` rather than the plugin SPI, so a new arity is a new interface and no plugin function
  can declare one. Snowflake states no general rule and documents arity per function ("The lambda
  expression must have only one argument" for `FILTER` and `TRANSFORM`, `(acc, value)` for
  `REDUCE`), and forbids the general case outright: lambdas "aren't supported as standalone objects.
  They must be specified as arguments to Snowflake higher-order functions." Flink is in ksqlDB's
  position because [lambda arguments are open to UDFs](#7-function-kinds-and-process-table-functions),
  which is why the bound is stated and checked here rather than implied; the reasoning, and why a
  fourth arity stays additive, are under [*Known limitations*](#known-limitations).

## Design overview

A **lambda** is an anonymous function `params -> body` that a higher-order function applies to
elements. It is not a storable data value: the function that receives it — built-in or user-defined
alike — gets a transient function object at runtime and applies it in its own loop. To carry a lambda
through validation, type inference, planning and the Table API expression tree, we introduce a
**`FUNCTION` type** that states how many parameters a lambda accepts.

The `FUNCTION` type is a *planning/API helper type*: it marks an operand as a lambda and fixes its
runtime representation. It is **not** a storable value type — it cannot be a table column, a
persisted function return type, or a state type. These restrictions are enforced with clear errors.

The type is deliberately kept minimal. It carries exactly what determines the runtime
representation — the parameter count, which selects the conversion class — and
nothing else. The lambda's parameter types and result type, which are needed for validation and
inference rather than for representation, are exposed through
`CallContext#getLambdaArgument(int)` as a `LambdaInfo`. This mirrors how Flink's two existing
argument-only helper roots work: `DESCRIPTOR` is a parameterless type whose column list is read
through `CallContext#getArgumentValue`, and `SYMBOL` is a parameterless type whose symbol is the
value. Keeping the payload out of the type is what keeps the new root's blast radius at
`DESCRIPTOR`'s level (see [*Rejected Alternatives*](#rejected-alternatives)).

### Which representation is public, and which is serialized

A lambda has three distinct representations, and only one of them is public — a different one from
the only one that is serialized:

| Representation | Lives in | Visibility | In a compiled plan |
|---|---|---|---|
| `FunctionType` / `DataTypes.FUNCTION(n)` (a `LogicalType`/`DataType`) | `flink-table-common` | **`@PublicEvolving`** | never serialized |
| `FunctionRelDataType` (a Calcite `RelDataType`) | `flink-table-planner` | `@Internal` | never serialized |
| `RexLambda` / `RexLambdaRef` (the planner IR node) | planner IR (Calcite) | not user-facing | **this is what is serialized** |

**Nothing about the `FUNCTION` *type* is serialized, in either representation.** A compiled plan
stores the `RexLambda` — parameter references plus body — and the lambda's type is recomputed on
deserialization via `RexBuilder.makeLambdaCall`, so no `FUNCTION` case exists in
`LogicalTypeJsonSerializer` / `RelDataTypeJsonSerializer`
([*Proposed Changes → Compiled plan*](#compiled-plan-flink-table-planner)). The planner-side type is
already internal, so plan serialization is *not* an argument for or against a public type.

What forces the first row to be public is neither planning nor serialization, but two public
contracts that live **outside** the planner, in the modules a Table API program and a UDF are
compiled against (`flink-table-api-java` and `flink-table-common`, neither of which depends on
`flink-table-planner`):

- `ResolvedExpression#getOutputDataType` is non-null by contract, so the resolved, `@PublicEvolving`
  `LambdaExpression` must return a real `DataType`.
- `CallContext#getArgumentDataTypes()` hands a function author's `InputTypeStrategy` a `DataType`
  for *every* argument position, the lambda one included; a `LambdaInputTypeStrategy` identifies
  it by `LogicalTypeRoot.FUNCTION`.

A planner-internal type is not reachable from either. See
[*Rejected Alternatives*](#rejected-alternatives).

## Public Interfaces

### What a `+1` authorizes

The sections below are the normative description. This list is the enumeration of the permanent
commitments they make, so a voter need not assemble it across sections.

**New `@PublicEvolving` types — fifteen.**

- `org.apache.flink.table.expressions.LambdaExpression` (`flink-table-api-java`)
- `org.apache.flink.table.expressions.UnresolvedLambdaExpression` (`flink-table-api-java`)
- `org.apache.flink.table.types.logical.FunctionType` (`flink-table-common`)
- `org.apache.flink.util.function.Function0`, `Function1`, `Function2`, `Function3`, `Function4`
  (`flink-core-api`) — the external representation of a lambda argument, one per arity, and the
  conversion classes of the `FUNCTION` type. One naming scheme spans the whole range, which no
  scheme built on `java.util.function` can do: it has no zero- or four-argument type. `Function1`,
  `Function2` and `Function3` extend `java.util.function.Function`, `java.util.function.BiFunction`
  and `org.apache.flink.util.function.TriFunction` and add nothing, and those three remain accepted
  conversions, so either spelling may be declared or bridged to. The family is
  what makes the arity bound a lookup table rather than a hard-coded set (see
  [*Known limitations*](#known-limitations)).
- `org.apache.flink.table.data.FunctionData0`…`FunctionData4` (`flink-table-common`) — the internal
  representation of a lambda argument, one per arity, and the internal conversion classes of the
  `FUNCTION` type. They sit beside `ArrayData`, `MapData` and `RowData` and are public for the same
  reason those are: a function that reads its other arguments internally receives its lambda
  internally too, and its `eval` signature has to be able to name the type. Each declares a single
  `apply` over internal data plus the arity-appropriate parameter count; nothing else is exposed.
- `org.apache.flink.table.types.inference.LambdaInfo` (`flink-table-common`)
- `org.apache.flink.table.types.inference.LambdaInputTypeStrategy` (`flink-table-common`) — a
  sub-interface of `InputTypeStrategy` with one abstract method,
  `getExpectedLambdaParameterTypes(CallContext, int)`; it implements the three inherited methods by
  default to accept any arguments unchanged, which makes it a functional interface. It is the
  contract by which the expression resolver and the planner bind lambda parameter types, and **the
  only way to declare a lambda argument**: a function implements it as its input type strategy. The built-in higher-order functions declare their signatures the same way, so the
  path a UDF author takes is the one the framework itself exercises.

**One promotion.** `org.apache.flink.table.expressions.LocalReferenceExpression` moves from
`@Internal` to `@PublicEvolving`, because `LambdaExpression.getParameters()` returns
`List<LocalReferenceExpression>` and no `@PublicEvolving` API may leak an `@Internal` type. The
committed surface is deliberately minimal: the class stays framework-constructed (its constructor is
package-private and its only factory lives in the `@Internal` `ApiExpressionUtils`), so the promotion
adds exactly one member beyond `ResolvedExpression` — `getName()`. No existing member is removed or
changed.

**No factory vocabulary.** An earlier draft additionally proposed a `@PublicEvolving`
`LambdaTypeStrategies` holder with ten factory methods (`lambda`, `elementOf`, `argumentOf`,
`keyOf`, `valueOf`, plus re-exports of `sequence`, `logical`, `explicit`, `ANY`) and a
`LambdaParameterTypeStrategy` functional interface. Both are dropped. The argument for them was that
`InputTypeStrategies` is `@Internal`, so without re-exports a UDF could not be declared without
internal API — but that premise is false: `LambdaInputTypeStrategy` is public and is what both
binding paths consult, so a function can declare any signature with public API alone. The factories
were a convenience layer, and a convenience layer whose shape has not been validated by usage is the
wrong thing to freeze in a vote. Adding factories later is additive; removing them is a break.
`TypeInference.Builder#inputTypeStrategy` plus `LambdaInputTypeStrategy` is a complete,
self-contained entry point, and no `@Internal` API is needed at any step. The `@Internal`
`InputTypeStrategies` holder — 38 factories, most of them unrelated to this FLIP — is deliberately
**not** promoted; committing to it wholesale is a much larger surface than this FLIP has analysed,
and is left to its own proposal.

**Additions to existing public types.**

- The enum constant `org.apache.flink.table.types.logical.LogicalTypeRoot.FUNCTION`
- The default method `LogicalTypeVisitor#visit(FunctionType)`
- The default method `CallContext#getLambdaArgument(int pos)`
- `org.apache.flink.table.api.DataTypes.FUNCTION(int parameterCount)`, which accepts a parameter
  count between zero and four and rejects any other with a validation error
- The constant `FunctionType.MAX_CONVERTIBLE_PARAMETER_COUNT`, whose value is `4`
- No new member, but a newly accepted declaration: `StaticArgument.scalar(name,
  DataTypes.FUNCTION(n), false)` declares the lambda argument of a process table function, where the
  factory previously rejected a `FUNCTION` type

**Eight SQL function names**, reserved as built-ins: `ARRAY_TRANSFORM`, `ARRAY_FILTER`,
`ARRAY_REDUCE`, `ARRAY_ZIP_WITH`, `MAP_FILTER`, `MAP_TRANSFORM_KEYS`, `MAP_TRANSFORM_VALUES`,
`MAP_ZIP_WITH`.

**Eight Table API methods**: `arrayTransform`, `arrayFilter`, `arrayReduce`, `arrayZipWith`,
`mapFilter`, `mapTransformKeys`, `mapTransformValues`, `mapZipWith` — and the **eight PyFlink**
equivalents `array_transform`, `array_filter`, `array_reduce`, `array_zip_with`, `map_filter`,
`map_transform_keys`, `map_transform_values`, `map_zip_with`.

**Two behavioural commitments**, which are as permanent as the signatures above:

- The **normative lambda-object contract** governing the `Function0`…`Function4` object
  handed to a UDF's `eval` — see [*The lambda object contract*](#the-lambda-object-contract).
- The **function-kind scoping** rule: a lambda argument may be declared by scalar, table and
  process table functions — the last with a self-contained lambda only — and is rejected for
  aggregate, table-aggregate and asynchronous hosts — see
  [*7. Function kinds and process table functions*](#7-function-kinds-and-process-table-functions).

**Lambda syntax itself** — `x -> expr` and `(x, y) -> expr` — valid only as an argument of a
higher-order function.

### 0. Exact signatures

Naming a type is not declaring its API. Every member listed above, written out:

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
@FunctionalInterface
public interface LambdaInputTypeStrategy extends InputTypeStrategy {
    Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos);

    // Defaults: accept any arguments and return their types unchanged (no validation).
    @Override default ArgumentCount getArgumentCount();                   // any
    @Override default Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure);            // unchanged
    @Override default List<Signature> getExpectedSignatures(
            FunctionDefinition definition);                              // f(*)
}

// flink-table-common — org.apache.flink.table.types.inference.CallContext
default Optional<LambdaInfo> getLambdaArgument(int pos);

// flink-core-api — org.apache.flink.util.function
// The FunctionN family. Every member is a conversion class of the FUNCTION type. Arities 1..3
// extend the existing JDK/Flink types and add nothing, so those types stay accepted as equivalent
// conversions and either spelling may be declared.
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
@PublicEvolving
public interface FunctionData0 { Object apply(); }
@PublicEvolving
public interface FunctionData1 { Object apply(Object arg0); }
@PublicEvolving
public interface FunctionData2 { Object apply(Object arg0, Object arg1); }
@PublicEvolving
public interface FunctionData3 { Object apply(Object arg0, Object arg1, Object arg2); }
@PublicEvolving
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
    /** Internal: the arity-indexed internal conversion class, or null above the bound. */
    @Internal
    public static Class<?> internalConversionClass(int parameterCount);
    public int getParameterCount();
}
```

`getParameterCount()` is the sole public accessor for the arity. Both conversion-class mappings —
external and internal — are arity-indexed lookup tables private to `FunctionType`, which is what
makes a further arity two table entries rather than a change spread across modules. Neither table
is exposed: `LogicalTypeUtils.toInternalConversionClass`, where every other logical type's internal
class is decided, delegates to the `@Internal` `FunctionType.internalConversionClass(int)` rather
than repeating the mapping, and that accessor is not part of this proposal's surface.

The eight Table API DSL methods on `BaseExpressions<InType, OutType>`, with their exact generics:

```java
public OutType arrayTransform(Function1<OutType, OutType> lambdaFunction);
public OutType arrayFilter(Function1<OutType, OutType> lambdaFunction);
public OutType arrayReduce(InType initialValue, Function2<OutType, OutType, OutType> lambdaFunction);
public OutType arrayZipWith(InType array, Function2<OutType, OutType, OutType> lambdaFunction);
public OutType mapFilter(Function2<OutType, OutType, OutType> lambdaFunction);
public OutType mapTransformKeys(Function2<OutType, OutType, OutType> lambdaFunction);
public OutType mapTransformValues(Function2<OutType, OutType, OutType> lambdaFunction);
public OutType mapZipWith(InType map, Function3<OutType, OutType, OutType, OutType> lambdaFunction);
```

The `FunctionN` family appears here in a second, unrelated role: these are *builder-side* interfaces,
applied once at call-construction time to build the body expression, and **not** the conversion
classes a UDF receives at runtime (those are described in
[§5](#5-lambda-arguments-for-user-defined-functions)). The family is used for both because one
arity-named scheme across the whole API is the point of having it — the alternative,
`Function`/`BiFunction`/`TriFunction` here and `FunctionN` there, would make the same call site read
two ways depending on which side of the API it sits on. Since `Function1`, `Function2` and
`Function3` extend the JDK/Flink types, a Java lambda, a method reference and a Scala closure all
target these parameters exactly as they would the supertypes.

### 1. Lambda expressions

**SQL** (Calcite syntax; Calcite 1.41):

```
x -> expression
(x, y) -> expression
```

**Table API** (Java) — the higher-order function methods accept a host-language lambda that
receives expression(s) for the parameter(s) and returns the body expression:

```java
$("vals").arrayTransform(x -> x.times(10));
$("vals").arrayFilter(x -> x.isGreater(1));
$("vals").arrayReduce(lit(0), (acc, x) -> acc.plus(x));
```

**Python Table API**:

```python
col("vals").array_transform(lambda x: x * 10)
col("vals").array_filter(lambda x: x > 1)
col("vals").array_reduce(lit(0), lambda acc, x: acc + x)
```

Lambdas are only valid as arguments of higher-order functions; a lambda used anywhere else is an
error — rejected by the parser in SQL, since the grammar admits `->` only in an argument position,
and by the expression resolver in the Table API.

**Expression API.** A lambda is also a first-class node of the public expression tree: the new
`@PublicEvolving` `UnresolvedLambdaExpression` and `LambdaExpression` (the latter a
`ResolvedExpression` with a `FUNCTION` output type) — `LambdaExpression` exposes its parameters via
`getParameters(): List<LocalReferenceExpression>`. This **widens the public API surface beyond the
new classes themselves**: `LocalReferenceExpression` is promoted from `@Internal` to
`@PublicEvolving`, a new API commitment that is called out here rather than left as a consequence —
see [*Compatibility*](#compatibility-deprecation-and-migration-plan) for why it is forced, what it
guarantees, and how it is verified.

### 2. The `FUNCTION` type

- New `LogicalTypeRoot.FUNCTION` and `FunctionType(parameterCount)`, a parameterless-payload helper
  type in the shape of `DescriptorType` and `SymbolType`, plus the
  `LogicalTypeVisitor.visit(FunctionType)` default method — all `@PublicEvolving`.
- New `DataTypes.FUNCTION(parameterCount)` factory.
- Serializable string form: `FUNCTION(n)`, where `n` is the parameter count. It round-trips through
  `LogicalTypeParser` like every other logical type: the parser gains a `FUNCTION` keyword and a
  production that reads the parameter count as a *required* precision (a bare `FUNCTION` names no
  particular type, and `asSerializableString()` always writes the count, so no serialized form
  depends on a default). That is the same shape as every other precision-carrying type — no new
  token *character*, no nested-type production and no `->` syntax, which is what a type carrying the
  lambda's parameter and result types would have cost (see
  [*Rejected Alternatives*](#rejected-alternatives)). (This is the *type*'s string form; nothing
  about the `FUNCTION` type is written into a compiled plan — see
  [*Design overview*](#design-overview).)
- The lambda's parameter types and result type are **not** part of the type; they are read from
  `CallContext#getLambdaArgument(pos)` (see [§5](#5-lambda-arguments-for-user-defined-functions)).
- Conversion classes, by parameter count: `Function0`…`Function4` externally (with `Function`,
  `BiFunction` and `TriFunction` accepted as equivalents at arities one to three) and the new
  `@PublicEvolving` `FunctionData0`…`FunctionData4` internally. This is where a lambda argument's
  representation lives (see [§5](#5-lambda-arguments-for-user-defined-functions)).
- Guards: not allowed as a table column type, a persisted function return type, or a state type;
  each rejected with a clear message.

### 3. Built-in higher-order functions

Eight new built-in system functions. Their names become reserved built-in system function names —
see [*Compatibility*](#compatibility-deprecation-and-migration-plan) for what that means for a
colliding user-defined function.

| Function | Signature | Result |
|---|---|---|
| `ARRAY_TRANSFORM` | `(ARRAY<E>, E -> R)` | `ARRAY<R>` |
| `ARRAY_FILTER` | `(ARRAY<E>, E -> BOOLEAN)` | `ARRAY<E>` |
| `ARRAY_REDUCE` | `(ARRAY<E>, A, (A, E) -> A)` | `A` |
| `ARRAY_ZIP_WITH` | `(ARRAY<E1>, ARRAY<E2>, (E1, E2) -> R)` | `ARRAY<R>` |
| `MAP_FILTER` | `(MAP<K,V>, (K, V) -> BOOLEAN)` | `MAP<K,V>` |
| `MAP_TRANSFORM_KEYS` | `(MAP<K,V>, (K, V) -> K2)` | `MAP<K2,V>` |
| `MAP_TRANSFORM_VALUES` | `(MAP<K,V>, (K, V) -> V2)` | `MAP<K,V2>` |
| `MAP_ZIP_WITH` | `(MAP<K1,V1>, MAP<K2,V2>, (K, V1, V2) -> R)` | `MAP<K,R>` where `K` is the common type of `K1` and `K2` |

**ARRAY_TRANSFORM** — new array of the lambda applied to each element; result element type `R` is the
lambda body type; order preserved.

**ARRAY_FILTER** — elements for which the predicate is `TRUE`; a `FALSE` or `NULL` predicate excludes
the element.

**ARRAY_REDUCE** — left fold: starts from the initial accumulator `A`, applies `(acc, element) -> acc`
across the array in order, and returns the final accumulator. The merge lambda has two parameters —
the accumulator (`A`, bound from the initial value) and the element (`E`) — and must return a value
**assignable to `A`**: implicit widening (e.g. `INT` → `BIGINT`) is allowed and each reducer result
is cast to `A`, but an incompatible or narrowing body type is rejected at validation on both surfaces.
Assignability means the **common type** of the body type and `A` is `A` itself, derived with Flink's
own type merging on both surfaces — so an `INT` body is accepted for a `DECIMAL(12,2)` accumulator
but rejected for a `DECIMAL(10,2)` one (an `INT` needs 10 integer digits, which would overflow).
The call result type is `A`, nullable if the array, the initial value, or the lambda body is nullable.
When the lambda body can produce `NULL`, the accumulator is inferred nullable via a monotonic
second binding pass applied identically on the SQL and Table API surfaces, so a reducer that branches
on `acc IS NULL` observes an accumulator that an earlier iteration set to `NULL`.

```sql
ARRAY_REDUCE(ARRAY[1, 2, 3], 0, (acc, x) -> acc + x)                 -- 6
ARRAY_REDUCE(ARRAY['a','b','c'], '', (acc, x) -> acc || x)           -- 'abc'
```

**ARRAY_ZIP_WITH** — combine two arrays element-wise: the two-parameter lambda receives the elements
at each position and its result becomes the element at that position. The shorter array is padded with
`NULL` to the length of the longer one, so the result length is `max(len1, len2)` and a padded
position passes `NULL` for that side. The result element type `R` is the lambda body type.

```sql
ARRAY_ZIP_WITH(ARRAY[1, 2], ARRAY[3, 4, 5], (x, y) -> COALESCE(x, 0) + y)   -- [4, 6, 5]
```

**MAP_FILTER** — entries `(k, v)` for which the predicate is `TRUE`; a `FALSE` or `NULL` predicate
excludes the entry. Keys and values are unchanged.

**MAP_TRANSFORM_KEYS** — new map with each key replaced by the lambda body; values unchanged. The
result key type `K2` is the lambda body type. If the transform produces a **duplicate key** the call
fails (map keys must be unique); a `NULL` key produced by the transform is likewise rejected. That
is a property of this function, not of Flink's `MAP` type, which permits `NULL` keys: an input map
with a `NULL` key is accepted here and passed to the lambda like any other entry, and `MAP_FILTER`,
`MAP_TRANSFORM_VALUES` and `MAP_ZIP_WITH` all preserve `NULL` keys. Keys are compared by SQL value
equality, as in `MAP_FROM_ENTRIES`: two `BINARY` keys with equal content are the same key. The
same holds for `MAP_ZIP_WITH`'s union of keys.

**MAP_TRANSFORM_VALUES** — new map with each value replaced by the lambda body; keys unchanged. The
result value type `V2` is the lambda body type.

**MAP_ZIP_WITH** — merge two maps over the **union of their keys**: for each key `k`, the three-parameter
lambda receives `k`, the value from the first map (`NULL` if `k` is absent there) and the value from the
second map (`NULL` if absent), and its result becomes the value for `k`. The two maps' key types are
generalized to their **common type** `K` (as for `MAP_UNION`, and matching Spark's `map_zip_with`),
so `MAP<INT,V1>` and `MAP<BIGINT,V2>` merge into `MAP<BIGINT,R>`; key types without a common type
(e.g. `INT` and `STRING`) are rejected. The result value type `R` is the lambda body type.

```sql
MAP_ZIP_WITH(MAP['a', 1], MAP['a', 2, 'b', 3], (k, v1, v2) -> COALESCE(v1,0) + COALESCE(v2,0))
-- {a: 3, b: 3}
```

NULL handling:

| Input | ARRAY_TRANSFORM | ARRAY_FILTER | ARRAY_REDUCE | ARRAY_ZIP_WITH | MAP_FILTER | MAP_TRANSFORM_KEYS | MAP_TRANSFORM_VALUES | MAP_ZIP_WITH |
|---|---|---|---|---|---|---|---|---|
| map/array `NULL` | `NULL` | `NULL` | `NULL` | `NULL` if **either** array is `NULL` | `NULL` | `NULL` | `NULL` | `NULL` if **either** map is `NULL` |
| element / entry with `NULL` value | body applied; `NULL`→`NULL` (kept) | predicate `NULL` → excluded | passed to merge lambda | padded/absent position → `NULL` passed for that side | predicate `NULL` → excluded | body applied to `(k, NULL)` | body applied to `(k, NULL)` | absent key → `NULL` passed for that side |
| empty | empty array | empty array | initial accumulator | empty array (both empty) | empty map | empty map | empty map | empty map |

**A mutable body result is never aliased.** A body that produces a mutable value (`ARRAY[...]`,
`ROW(...)`) yields **element-wise distinct** results: `ARRAY_TRANSFORM(a, x -> ARRAY[x])` returns
one array per element, never *N* references to the last one. This holds for every function that
buffers body results — `ARRAY_TRANSFORM`, `ARRAY_ZIP_WITH`, `MAP_TRANSFORM_KEYS`,
`MAP_TRANSFORM_VALUES`, `MAP_ZIP_WITH`, and `ARRAY_REDUCE`'s accumulator. It is a guarantee the
implementation makes rather than an incidental property of it (the mechanism is in
[*Proposed Changes → SQL planner*](#sql-planner-flink-table-planner)), and it is pinned by a test.

### 4. Table API / Python methods

New methods on the expression DSL (Java and Python): `arrayTransform` / `array_transform`,
`arrayFilter` / `array_filter`, `arrayReduce` / `array_reduce`, `arrayZipWith` / `array_zip_with`,
`mapFilter` / `map_filter`,
`mapTransformKeys` / `map_transform_keys`, `mapTransformValues` / `map_transform_values`, and
`mapZipWith` / `map_zip_with`, corresponding to the built-ins above. The map methods take a
host-language two-parameter lambda `(k, v) -> …` (three-parameter `(k, v1, v2) -> …` for
`mapZipWith`).

The methods are declared once, on `BaseExpressions`. **No Scala-specific API is added**, in line
with FLIP-265: the Scala DSL's `ImplicitExpressionOperations` extends `BaseExpressions`, so the eight
methods are inherited there and a Scala closure is adapted to the `Function1` / `Function2` /
`Function3` parameter by the compiler's SAM conversion — but that is inheritance, not a commitment.
Nothing in this FLIP is written, tested or documented against the Scala API, and the Scala Table API
is deprecated.

### 5. Lambda arguments for user-defined functions

A `ScalarFunction`, `TableFunction` or `ProcessTableFunction` may declare a lambda argument via the
type-inference stack (other function kinds may not — see
[§7](#7-function-kinds-and-process-table-functions), which also covers what a PTF declares in
addition). It does so by implementing the `@PublicEvolving` `LambdaInputTypeStrategy` as its input
type strategy:

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

The UDF receives the lambda as a first-class function object: the
`org.apache.flink.util.function.FunctionN` matching its arity, from `Function0` to `Function4` (the
`MAP_ZIP_WITH` merge lambda, for instance, is a `Function3`). At arities one to three the equivalent
`java.util.function.Function`, `java.util.function.BiFunction` and
`org.apache.flink.util.function.TriFunction` are accepted just as well, and at arity zero a plain
`java.util.function.Supplier` may be declared, since `Function0` extends it. The UDF
simply calls `apply(...)` in its own loop (so reduce/fold-shaped UDFs are
naturally expressible). A UDF may declare more than one lambda argument (each declared at its own
position by the strategy and received as its own function object); every lambda is lifted and
evaluated independently — `getExpectedLambdaParameterTypes` is asked once per lambda position. The UDF
does not implement `SpecializedFunction`; the framework compiles the lambda body and binds any
captured columns behind the object. New public surface: the `LambdaInputTypeStrategy` interface and a
`CallContext.getLambdaArgument(pos)` accessor returning an `Optional<LambdaInfo>` (the lambda's
parameter fields and result type, which are what an output-type strategy needs, plus its body once
the function is specialized):

The classes above are the external representation. Like every other argument, a lambda argument also
has an internal one, and it is the conversion class of its `FUNCTION` type: the new
`@PublicEvolving` `org.apache.flink.table.data.FunctionData0`…`FunctionData4`, declared by bridging
the lambda argument's data type in `inferInputTypes`. They are named after the logical type they
carry, following the existing convention of that package: `ArrayType`/`ArrayData`,
`MapType`/`MapData`, `RowType`/`RowData`, and now `FunctionType`/`FunctionDataN`. The arity suffix
mirrors the external `FunctionN` family, so the two line up member for member and the internal name
is derivable from the external one. The representation is a property of the *receiving function*,
not of the lambda — the values a function feeds its lambda are its own, so a function that reads an
`ARRAY` argument as `ArrayData` hands the lambda `StringData` rather than `String`. Built-in
higher-order functions are on the internal side and get it from the existing "internalize every
argument" step of `BuiltInScalarFunction`; a UDF is external by default and opts in. A function
whose declaration and `eval` method disagree is rejected while the query is planned, by the same
`eval`-signature check that covers every other argument.

`LambdaInfo` is phase-, not surface-dependent: `getParameterFields()` and `getReturnDataType()` are
always available, while `getBody()` is available **only during specialization**
(`SpecializedFunction#specialize`) and throws otherwise, with `hasBody()` to test for it. This holds
identically on the SQL and the Table API surface — SQL validation has no `Expression` body to offer,
so the Table API withholds it in that phase too rather than letting a function's type inference
depend on how the call was written. A function's *type inference* therefore derives from the
parameter and result types alone; the body is needed only by the framework's own code generation
(and by a function that wants to build its own `ExpressionEvaluator`), which runs after inference.

`CallContext#getLambdaArgument(pos)`, `LambdaInfo` (`getParameterFields()`,
`getParameterFieldsArray()`, `getReturnDataType()`, `getBody()`, `hasBody()`) and
`LambdaInputTypeStrategy` are `@PublicEvolving`.

`LambdaInputTypeStrategy` is the contract by which `ResolveCallByArgumentsRule` (Table API),
`TypeInferenceOperandChecker` (SQL) and `SystemTypeInference` bind a lambda's parameter types from
its sibling arguments before the body is resolved. All three dispatch on `instanceof
LambdaInputTypeStrategy`, so an `InputTypeStrategy` that does not implement it cannot host a lambda
at any position — which is precisely why it must be public. The built-in higher-order strategies
implement the same interface, so a UDF author takes the path the framework itself exercises. The
zero-to-four-parameter bound is part of that contract: a present list of derived parameter types
must contain at most four types, and both binding paths reject any larger size with a
validation error, so no strategy can bind a lambda arity that has no runtime representation. An
*empty* list is valid and means a zero-parameter lambda; a strategy that derives no types is
therefore declaring `() -> …`, not declining to bind.

No factory vocabulary is proposed alongside it. An earlier draft added a `@PublicEvolving`
`LambdaTypeStrategies` holder — `lambda(...)`, `elementOf`, `keyOf`, `valueOf`, `argumentOf`, plus
re-exports of `sequence`, `logical`, `explicit` and `ANY` — on the reasoning that
`InputTypeStrategies` is `@Internal` and a UDF author would otherwise have to call `@Internal` code
to use a `@PublicEvolving` API. That reasoning does not survive contact with the interface above:
implementing `LambdaInputTypeStrategy` needs no argument-strategy factory at all, because
`inferInputTypes` returns data types directly. A higher-order UDF is therefore declared end-to-end
from `LambdaInputTypeStrategy` plus `TypeInference.Builder`, with no `@Internal` type at any step,
and `InputTypeStrategies` is left byte-identical to its pre-FLIP state without being promoted.
Convenience factories can be added later without breaking anyone; freezing an unvalidated
vocabulary in a vote cannot be undone as cheaply.

`getExpectedLambdaParameterTypes` is the *cross-surface* contract, and it is the only abstract method
the interface adds: `TypeInferenceOperandChecker` (SQL) and `ResolveCallByArgumentsRule` (Table API)
call the same method the same way, so a user-supplied strategy binds the same parameter types in a
SQL query and in an equivalent Table API call. There is no separate declaration step that one
surface could consult and the other forget — implementing the interface *is* the declaration, and
every binding path tests for it with the same `instanceof` check. Two further steps are
deliberately **not** part of that contract, because both are things `ARRAY_REDUCE` needs and nothing
a user-supplied strategy should have to reason about:

- **Refining a parameter type once the body is known.** `ARRAY_REDUCE` widens the accumulator
  parameter to nullable once the reducer body turns out to be nullable — a single monotonic feedback
  pass. Both surfaces have to run it explicitly: the resolver asks the strategy after resolving the
  body once, and the SQL operand checker does the same after its body pass. (An earlier design got
  the SQL half for free from a bespoke operand checker's second inference pass; with the shared
  checker there is no such pass, so the step is now stated once and invoked from both sides — which
  is the stronger arrangement, since the two can no longer drift.)
- **Requiring a body result type.** The reducer body only has to be *assignable* to the accumulator,
  so the framework wraps it in a `CAST` to the accumulator type before compiling it. The target type
  cannot ride in the argument's `DataType` — `FunctionType` deliberately carries only the parameter
  count — so the strategy is asked for it directly.

Both hooks live on an `@Internal` `RefinableLambdaInputTypeStrategy extends LambdaInputTypeStrategy`,
implemented only by the built-in array strategy. A user-supplied strategy declares its parameter
types once, is never asked to refine them and never coerces a body, which is what keeps the two
surfaces equivalent; the resolver additionally rejects a refined list whose arity differs from the
declared one with a validation error rather than failing on an index.

```java
public class ArrayTransform extends ScalarFunction {
    // getTypeInference(...) as above
    public Integer[] eval(Integer[] array, Function<Integer, Integer> lambda) {
        Integer[] result = new Integer[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = lambda.apply(array[i]);   // may close over outer columns
        }
        return result;
    }
}
```

#### The lambda object contract

The function object is not an ordinary value: it is a handle on a framework-compiled expression that
is bound to one invocation of `eval` and to the thread evaluating it. The following is the normative
contract between the framework and a user-defined higher-order function. It is a **behavioural**
commitment rather than a type, and part of the public surface committed here just as the types
above are; it is stated on `LambdaInputTypeStrategy` and in the user documentation, and
pinned by the tests listed in [*Test Plan*](#test-plan).

| Aspect | Contract |
|---|---|
| **Lifetime** | The object is valid **only for the duration of the `eval` invocation it is passed to**. A function **must not** retain it — no storing it in a field, in state, or in any structure that outlives the call, no returning it, no passing it to another object that outlives the call, and no serializing it (it is a generated class with no serialized form). Behaviour after `eval` returns is undefined; the framework may reuse or invalidate anything behind the object. |
| **Identity** | A fresh object is created for **each evaluation of the enclosing call** — per row for a top-level call, per element when the call itself sits in a lambda body. Object identity therefore carries no meaning across rows and **must not** be used as a cache key. |
| **Captures** | Every captured value (outer column, enclosing lambda parameter, aggregate, `OVER` window, sub-query result) is evaluated **before** `eval` is entered and bound behind the object. All applications within one `eval` invocation therefore observe the same capture snapshot. |
| **Invocation count and order** | The function owns its loop. It **may** apply the object zero, one, or arbitrarily many times, in any order, with any values of the declared parameter types. The framework **never** applies the lambda on the function's behalf, so a body with a side effect or a failure is only reached if the function applies it. |
| **Arguments and result** | `apply` accepts and returns the conversion classes declared for the lambda argument. Arguments **may** be `null`, and a `null` result means SQL `NULL`; a function **must not** assume a non-null result. An external `FunctionN` receives external values. Its internal counterpart in the `FunctionDataN` family receives internal data structures, and a mutable result may be backed by evaluator-owned memory that the next application reuses. A function that retains or buffers such an internal result across another application **must deep-copy it first**. |
| **Nesting** | Applications of **distinct** objects nest to any depth: a body may host another higher-order call whose function receives its own object, and the inner application completes before the enclosing one resumes. An object is never re-entered from within its own body — SQL has no recursion — and a function **must not** construct such a call itself. |
| **Thread affinity** | The object **must** be applied only from the thread that invoked `eval`, and only while that invocation is on the stack. The compiled body is a single instance shared by every object built at that call site and holds mutable scratch state, so it is **not thread-safe**: applications **must not** overlap, and the object **must not** be handed to another thread, an executor, a parallel stream, or a `CompletableFuture` callback. |
| **Asynchronous invocation** | Not available: `AsyncScalarFunction` and `AsyncTableFunction` **may not** declare a lambda argument (see [§7](#7-function-kinds-and-process-table-functions)), which is what makes the thread-affinity rule enforceable — no lambda object can reach a completion callback. |
| **Exceptions** | An unchecked exception (`RuntimeException` or `Error`) raised while the body is evaluated propagates out of `apply` **unchanged**, exactly as it does from a built-in higher-order function given the same body — the two run the same generated code. A checked exception cannot be declared by the functional interface's `apply`; generated code wraps one in `FlinkRuntimeException` with the original exception as its cause. A function **should** let either form propagate, failing the task as any other function failure does. Catching it is allowed, but the application then yields no result and anything the body touched is left in an unspecified state; the enclosing call is not retried. |
| **`open` / `close`** | A lambda object exists per call only: **none** is available in `open(FunctionContext)` or `close()`, and a function **must not** try to obtain, retain, or apply one there. The compiled body is initialized when the enclosing operator opens and closed with it, independently of the function's own lifecycle; a function called *from* a body is instantiated, opened, and closed like one called anywhere else. |
| **Determinism** | The object is as deterministic as the body the caller wrote. A non-deterministic body makes the enclosing call non-deterministic (`RexUtil.isDeterministic` descends into the lambda), so a function **must not** assume that applying the same arguments twice yields the same result. |

The practical consequence is that a higher-order UDF stays a plain loop: it reads its own data, applies
the lambda while `eval` runs, and returns, with no lifecycle of its own to manage.

**Where the contract lives, and what is checked.** The normative text above ships as the Javadoc of
`LambdaInputTypeStrategy` — the interface a UDF author implements to declare the argument — and is
mirrored in the user documentation. Only the rules that are structural are enforced: the function
kinds that may declare a lambda argument, its arity, and its parameter types. The lifetime and
thread-affinity rules are **not** checked at runtime, and deliberately so — a per-application guard
would sit on the per-element path, and the case where a violation would be systematic rather than
deliberate (a lambda reaching a completion callback) is already excluded by disallowing the
asynchronous kinds. This makes the two rules untestable from the outside and easy to violate
unnoticed: a retained object stays technically callable after `eval` returns, and because the
compiled body is a single instance shared by every object built at that call site, a body that folds
to plain arithmetic can appear to tolerate concurrent applications while one that constructs a `ROW`
or an `ARRAY`, or calls another function, corrupts the reusable buffers behind it. Both are therefore
stated as undefined behaviour rather than as something a function may probe for.

### 6. Composition

Lambdas and higher-order calls are ordinary expressions, and compose in both directions without
special cases:

- a lambda **body** is a scalar expression, so any scalar function may be called in it — built-in or
  user-defined, including a function that is itself higher-order and takes its own lambda; and
- a higher-order **call** is a scalar expression, so it may appear wherever one may — nested in
  another lambda's body, as an argument of an ordinary function, and outside the projection (in
  `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, or as an aggregate's argument).

Nesting is therefore unbounded in depth and free in kind: built-in and user-defined higher-order
functions may host one another in any combination and any order. At every level the body may
reference its own parameters, the parameters of *all* enclosing lambdas (threaded hop by hop, inner
names shadowing enclosing ones), and any value the enclosing query produces for the current row.

The following composition surface is supported, using columns `a ARRAY<INT>`,
`a2d ARRAY<ARRAY<INT>>`, `a3d ARRAY<ARRAY<ARRAY<INT>>>`, `base INT` and user-defined functions
named `my_*`:

| Composition | Example |
|---|---|
| Ordinary function in a lambda body | `ARRAY_TRANSFORM(a, x -> my_add(x, base))` |
| Built-in higher-order function in a built-in body | `ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))` |
| User-defined higher-order function in a built-in body | `ARRAY_TRANSFORM(a2d, a -> my_array_transform(a, x -> x + a[1]))` |
| Built-in higher-order function in a user-defined body | `my_array_transform(a, e -> ARRAY_REDUCE(a, e, (acc, x) -> acc + x))` |
| User-defined higher-order function in a user-defined body | `my_array_transform(a, x -> my_array_reduce(a, x, (acc, y) -> acc + y))` |
| Alternating hosts, arbitrary depth | `my_nested_array_transform(a3d, a -> ARRAY_TRANSFORM(a, b -> my_array_transform(b, x -> ARRAY_REDUCE(b, x, (acc, y) -> acc + y + base + a[1][1]))))` |
| Several lambda arguments, each capturing separately, nested in a lambda | `ARRAY_TRANSFORM(a, e -> my_two_lambdas(e, x -> x + e, x -> x + base))` |
| Higher-order call as an argument of an ordinary function | `my_plus_one(ARRAY_REDUCE(a, 0, (acc, x) -> acc + x))` |
| Higher-order call outside the projection | `... WHERE ARRAY_REDUCE(a, 0, (acc, x) -> acc + x) > 5`, and the same call in `GROUP BY`, `HAVING`, `ORDER BY`, or under `SUM(...)` |
| Row-level construct over outer columns in a body | `ARRAY_TRANSFORM(a, x -> x + SUM(base)) ... GROUP BY g` |

A called function is not restricted in kind either: a user-defined function invoked from a lambda
body is instantiated, opened, and closed normally, so one with `open()`/`close()` and a
`FunctionContext` behaves as it does anywhere else. Each row above holds identically on the Table
API (a scalar sub-query is the one construct with no Table API spelling), and each is verified by
executing the query and asserting the computed result rather than the plan — see
[*Test Plan*](#test-plan).

What does not compose is the small set of cases listed in [*Known limitations*](#known-limitations),
each rejected during validation. The one that follows directly from the design here is that a lambda
is an **argument**, not a value: it is written only at an argument position that declares one and
cannot be stored, returned or passed on — a consequence of `FUNCTION` not being a storable type.

### 7. Function kinds, and process table functions

A function can host a lambda argument exactly when it applies the lambda object **synchronously,
inside the invocation that received it** — the precondition of the lifetime and thread-affinity
rules of [the lambda object contract](#the-lambda-object-contract). That is true of
`ScalarFunction` and `TableFunction`, whose call is compiled into the expression of the row it is
evaluated in, and of `ProcessTableFunction`, whose `eval` the generated runner invokes once per input
row; it is true of no other function kind:

| Function kind | Lambda argument |
|---|---|
| `ScalarFunction` (`SCALAR`) | **supported** |
| `TableFunction` (`TABLE`) | **supported** |
| `ProcessTableFunction` (`PROCESS_TABLE`) | **supported** — self-contained lambdas only, see [below](#process-table-functions-ptfs) |
| `AggregateFunction` / `TableAggregateFunction` | rejected |
| `AsyncScalarFunction` / `AsyncTableFunction` | rejected |
| any other kind (`OTHER` — e.g. a procedure or a declarative aggregate) | rejected |

The supported set is the allow-list `{SCALAR, TABLE, PROCESS_TABLE}` and every other kind is
rejected, so a kind added to `FunctionKind` later is rejected by default rather than admitted by
omission.

The rejection is a single check in `SystemTypeInference`, which both surfaces already route every
call through, so SQL and the Table API report it identically: *"Lambda arguments are not supported
for functions of kind '`AGGREGATE`'. Only scalar, table, and process table functions can declare a
lambda argument."* An aggregate is rejected rather than silently mis-planned for the same reason
`ARRAY_TRANSFORM(a, x -> SUM(x))` is — an aggregate's accumulator is not a row context in which a
per-element expression can be compiled (see [*Known limitations*](#known-limitations)). None of
this is a statement that the kinds could never host a lambda — Trino's `reduce_agg` is an aggregate
that takes two lambdas, so the shape is not meaningless — only that they do not here, and fail
loudly rather than at a random point in planning.

**Asynchronous functions may not accept lambdas.** `AsyncScalarFunction` and `AsyncTableFunction`
are rejected by the same check (*"…of kind '`ASYNC_SCALAR`'…"*, *"…of kind '`ASYNC_TABLE`'…"*), for
a reason that is a direct consequence of the lambda object contract in
[§5](#5-lambda-arguments-for-user-defined-functions) rather than an implementation accident. An
asynchronous `eval` receives a `CompletableFuture` and returns immediately; the work that would
apply the lambda happens afterwards, on whatever thread completes the future. Handing a lambda
object to a completion callback would therefore break the contract's lifetime rule and its
thread-affinity rule at once — and would do so silently, since neither is checkable at runtime.
Rejecting the declaration is what makes the thread-affinity rule enforceable at all: with the
asynchronous kinds excluded, no lambda object can reach a thread other than the one evaluating the
enclosing call.

Lifting this later is possible but is a *semantics* change, not a representation one: it would need a
defined threading contract for the body (which thread may evaluate it, and how a per-call-site
evaluator is made safe under concurrent completions — most plausibly one evaluator instance per
in-flight invocation). Because a lambda argument on an asynchronous kind fails today with a clear
error, no user program can come to depend on the current behaviour, so that work stays additive.
Asynchronous functions are unaffected in the other direction: a higher-order *call* is an ordinary
scalar expression and may be an argument of an asynchronous function, or wrap its result.

#### Process table functions (PTFs)

A PTF may declare a lambda argument, with **no public type or member of its own**: the declaration
reuses `StaticArgument.scalar` and `LambdaInputTypeStrategy`, and the representation reuses the `FUNCTION`
type, the `Function0`…`Function4` conversion classes and the closed `RexLambda`. Two properties of
PTFs shape it — the static signature, and the absence of an enclosing row — and the result is a
deliberately narrow, *self-contained* lambda.

```java
public class ScoreTransform extends ProcessTableFunction<Integer> {
    public void eval(Row input, Function<Integer, Integer> f) {
        collect(f.apply(input.getFieldAs("score")));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        // f's single parameter has the type of the "score" column
        final LambdaInputTypeStrategy scoreParameter = (call, pos) -> {
            if (pos != 1) {
                return Optional.empty();
            }
            final DataType table = call.getArgumentDataTypes().get(0);
            final int score = DataType.getFieldNames(table).indexOf("score");
            return Optional.of(List.of(DataType.getFieldDataTypes(table).get(score)));
        };
        return TypeInference.newBuilder()
            .staticArguments(
                StaticArgument.table("input_table", Row.class, false,
                    EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)),
                StaticArgument.scalar("f", DataTypes.FUNCTION(1), false))
            .inputTypeStrategy(scoreParameter)
            .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
            .build();
    }
}
```

```sql
SELECT * FROM ScoreTransform(input_table => TABLE t, f => x -> x * 10);
```

*Declaration.* A PTF must declare a **static signature** — anything else is rejected with "Function
requires a static signature that is not overloaded and doesn't contain varargs" — so the lambda is
listed in it as a scalar argument of type `FUNCTION(n)`. That fixes the lambda's position, its name
(so it can be passed as `f => x -> …`) and its arity. It cannot fix the parameter types, which are
derived *per call* from the sibling arguments — the column types of the table argument, the element
type of an array argument, … — exactly what `LambdaInputTypeStrategy#getExpectedLambdaParameterTypes`
expresses. A PTF therefore additionally implements `LambdaInputTypeStrategy` as its input type
strategy, which remains the one way of declaring a lambda argument for every kind. Since the static
signature already determines and validates the argument types, the strategy's `inferInputTypes` is a
validation layer only, as for every PTF, and must return the argument types unchanged — which is
exactly what the interface's default methods do. A PTF therefore writes the strategy as a lambda
that only derives the parameter types, as above, and never has to know about the system arguments
`uid` and `on_time` the framework appends. `SystemTypeInference` wraps it in a system input strategy
that keeps implementing `LambdaInputTypeStrategy`, so every binding path sees the lambda.
Reflective extraction recognizes a lambda parameter of `eval` — a `FunctionN`, one of its
`Function`/`BiFunction`/`TriFunction` aliases, or the internal `FunctionDataN` — as `FUNCTION(n)`,
ignoring its generics, so it yields the lambda's position and arity; it can never yield the
strategy, which is why a PTF taking a lambda overrides `getTypeInference()`.
Each misdeclaration is rejected where it is declared, with its own message:

- an optional lambda argument — an omitted optional argument is passed as a `NULL` literal, and
  there is no `NULL` lambda: *"Lambda argument '`f`' must not be optional."*;
- a `FUNCTION`-typed static argument of a scalar or table function, which declares its lambda
  through the strategy alone: *"Invalid data type '`FUNCTION(1)`' for argument '`f`'. Lambda
  arguments in a static signature are only supported for process table functions. Declare them with
  a `LambdaInputTypeStrategy` in `getTypeInference()` instead."* — of a kind that takes no lambda at
  all, the kind itself is rejected (see [*Known limitations*](#known-limitations));
- a PTF without a `LambdaInputTypeStrategy`, including one whose signature is extracted reflectively
  from its `eval` method: *"Lambda argument '`f`' requires an input type strategy that implements
  `LambdaInputTypeStrategy`, which derives the types of the lambda parameters. They cannot be
  extracted from the eval() method, so override getTypeInference() to declare the static arguments
  together with such a strategy."*;
- a strategy that derives a different arity than the declaration: *"Lambda argument '`f`' declares 2
  parameter(s) in the static signature, but its input type strategy derives 1."*;
- a `FUNCTION` *nested* in a constructed type (`ARRAY<FUNCTION(1)>`, a `ROW` field, …), for any
  kind: nested, it is not a lambda argument at all — no call can supply a lambda there — only an
  unmaterializable type, so it is reported as *"…cannot be materialized and must not be nested in
  another data type."*

*Scope.* A PTF is called in the `FROM` clause, and its scalar arguments are evaluated for the call
rather than per row of any table, so there is no enclosing row and no capture scope of the kind
[*Lambda captures*](#lambda-captures) defines. A PTF's lambda is therefore **self-contained**: its
body may reference its own parameters, literals, and the parameters of lambdas nested in it — a
higher-order call in the body captures the outer lambda's parameters exactly as anywhere else. In
the Table API a column reference in the body does not even resolve, as a PTF call has no input
field scope; in SQL a capture can only arise from a scalar sub-query, which is rejected with *"A
lambda argument of process table function '`f`' may only reference its own parameters, literals,
and the parameters of lambdas nested in its body."* Lifting would append the captured values as
trailing operands, but a PTF's operands map one to one to its static signature and its system
arguments are located from the end of the operand list, so there is no slot for them. A
self-contained lambda is the shape of Calcite's original lambda support (CALCITE-3679), and
admitting captures later is additive.

What the parameters are bound to is **the PTF's own decision**, not a new binding rule: its strategy
derives their types from the sibling arguments, and its `eval` applies the object to whatever values
it chooses — the current row, a value from its state, an accumulator and a column, as in a
per-partition fold. This is what an earlier draft deferred as an open semantics question (whether a
PTF lambda is a per-row or per-partition callback, and what it may do about state, timers,
`PARTITION BY` and changelogs); with the PTF applying the object itself, none of those needs an
answer from the framework.

*Runtime.* The generated `ProcessTableRunner` builds the lambda object in `callEval()`, immediately
before `eval` is invoked, through the same `BridgingFunctionGenUtil` path that builds it for a scalar
or table function, so the contract of [§5](#5-lambda-arguments-for-user-defined-functions) applies
unchanged: the object is valid for the `eval` invocation of one input row. It is never available in
`onTimer`, which receives no scalar arguments, and it cannot be put into state, since `FUNCTION` is
not a state type.

*SQL operand type.* Calcite matches the operands of a PTF call against its declared parameter types,
which requires an assignable type for the lambda parameter. `FunctionRelDataType` (`SqlTypeName.OTHER`)
has no assignment rules, so the declared SQL type of a `FUNCTION`-typed static argument is `ANY` —
the precedent is the untyped table argument. The lambda itself is validated and bound by
`TypeInferenceOperandChecker`, as for every other function.

**Why no dedicated declaration factory.** An earlier draft reserved a `StaticArgument.lambda(name,
...)` factory plus a `StaticArgumentTrait.LAMBDA` for this. It is not needed: `DESCRIPTOR` is the
precedent that a payload-free helper type is declared through `StaticArgument.scalar` — PTFs consume
it as `StaticArgument.scalar(on_time, DataTypes.DESCRIPTOR(), true)` — and `FUNCTION` follows that
route. A dedicated factory would add public surface without adding information, since the parameter
types it could carry cannot be static anyway.

**Higher-order calls around a PTF.** A higher-order *call* is an ordinary scalar expression, so it
works everywhere a PTF call can see one — as a PTF scalar argument, inside the query behind a PTF
table argument, and in a projection over the relation a PTF produces. All three are verified
end-to-end; e.g.

```sql
SELECT * FROM my_ptf(t => TABLE t, threshold => ARRAY_REDUCE(ARRAY[1,2,3], 0, (acc, x) -> acc + x));
```

The other two are in [appendix A7](#a7-further-ptf-composition-examples). The converse does not
hold, for the reason given in [*Known limitations*](#known-limitations): a PTF — like any table
function — produces rows rather than a value a per-element expression can consume, so it may not
appear in a lambda body.

## Proposed Changes

Everything below that is not listed in [*Public Interfaces*](#public-interfaces) is internal
(`FunctionRelDataType`, `HigherOrderFunctionUtil`, the code-generation and planner-rule changes)
and carries no compatibility promise.

### Type system (`flink-table-common`)
- `LogicalTypeRoot.FUNCTION`, `FunctionType(parameterCount)`,
  `LogicalTypeVisitor.visit(FunctionType)`, `DataTypes.FUNCTION(parameterCount)`, and guards
  preventing column/state/persisted use.
- `CallContext#getLambdaArgument(pos)` returning a `LambdaInfo` (parameter fields, result type, and
  — during specialization only — the body) is the channel for everything the type does not carry.
  It is added as a `default` returning
  `Optional.empty()`, so existing implementations of the `@PublicEvolving` `CallContext` keep
  compiling, and is overridden by the three contexts that back a real higher-order call — the Table
  API resolver's, `CallBindingCallContext` (SQL validation) and `OperatorBindingCallContext`
  (return-type inference and code generation) — so the built-in higher-order functions' input and
  result type strategies read the lambda's result type from there. The framework's *wrapping*
  contexts delegate rather than inherit that default, since inheriting it would answer "no lambda"
  for a call that has one: `CastCallContext` forwards to the context it adapts, and the split
  context of `SubsequenceInputTypeStrategy` forwards with the same position shift it applies to
  every other per-argument accessor (no built-in combines `compositeSequence()` with a lambda
  argument today, so that one is future-proofing rather than a live path). `UnknownCallContext` states the
  empty answer explicitly: there a lambda argument is unknown like every other argument, so a
  strategy that declares one fails to infer instead of deriving types from a lambda that context
  cannot describe.

#### `LogicalTypeRoot.FUNCTION` — exhaustive-switch audit

Adding a `LogicalTypeRoot` is a permanent, high-blast-radius change: it must be accounted for in
every exhaustive `switch` over the root, or a latent, un-removable bug results. Every such site was
audited. The design principle is that `FUNCTION` is a *planning-only* helper type that is
**intercepted before any internal data path** — it is never a column, field, state, or serialized
(internal) value. Its only runtime manifestation is a transient, *external* function object
(one of the `Function0`…`Function4` conversion classes) handed to a user-defined higher-order
function's `eval` at the call boundary, and that object is passed through without an internal serializer (see
[*User-defined functions*](#user-defined-functions)). Nothing about the type is serialized in a
compiled plan, in either representation (see [*Design overview*](#design-overview)), so no
`FUNCTION` case is needed — or added — in the type serializers.

**What the principle commits to.** Five choke points reject the type with a clear message, and
together they are what makes it unreachable everywhere else:

- `InternalSerializers` — every runtime materialization of a value, and so the last line of defence
  for *all* of "not a column / not a state type / not a persisted value".
- `DefaultSchemaResolver` — the declared type of any column, physical, metadata or computed, and
  also nested in a constructed type; a schema is persisted long before a serializer is built.
- `TypeInferenceUtil#inferOutputType` — a declared **return** type.
- `TypeInferenceUtil#inferStateInfo` — a declared **state** type.
- `LogicalTypeCasts` — every cast to or from the type, implicit and explicit; only the identity (a
  function type of the same arity) is compatible.

`leastRestrictive` is the one deliberate exception on the otherwise-unreachable paths: it is
*narrowed* (not opened) so that an unresolved lambda parameter (plain `ANY`) defers to a later
validation pass while a genuine generic-RAW mismatch still fails fast — locked in by a regression
test (see [*SQL planner*](#sql-planner-flink-table-planner)).

Why each guard sits where it does rather than one layer later, what the remaining `switch` sites do
with a stray `FUNCTION`, and the site-by-site inventory in four categories with its test names are
in [appendix A2](#a2-logicaltyperootfunction--exhaustive-switch-audit-site-by-site).

### SQL planner (`flink-table-planner`)
-   The eight functions need **no Calcite operator and no operand type checker of their own**. Each
   is a `BuiltInFunctionDefinition` in `flink-table-common` carrying a declarative input and output
   type strategy plus a runtime class, and the SQL surface reaches it through `BridgingSqlFunction`
   and the shared `TypeInferenceOperandChecker` — the same path every other bridged built-in takes.
   The rules below are therefore properties of the *type strategies*, stated once and enforced
   identically on both surfaces; the strategies are shared where the shapes coincide
   (`ArrayHigherOrderFunctionInputTypeStrategy`, `MapHigherOrderFunctionInputTypeStrategy`,
   `ArrayZipWithInputTypeStrategy`).
  -   `ARRAY_TRANSFORM` and `ARRAY_FILTER` bind the element type to the single lambda parameter.
     `ARRAY_TRANSFORM` allows any body type and returns `ARRAY<body-type>`; `ARRAY_FILTER` requires
     a `BOOLEAN` body and returns the input array type.
  -   `ARRAY_REDUCE` binds parameter 0 to the accumulator (initial-value) type
     and parameter 1 to the element type; return type is the accumulator type. The reducer body must
     be assignable to the accumulator, which is decided with `LogicalTypeMerging.findCommonType` —
     the same rule the Table API strategy uses — rather than with Calcite's `leastRestrictive`,
     which disagrees with Flink's own type system for a number of type pairs (see *Rejected
     Alternatives → Deriving `ARRAY_REDUCE`'s assignability rule from Calcite's
     `leastRestrictive`*). This is the canonical statement of the mechanism: `findCommonType` is
     what decides, on **both** surfaces, which reducer bodies and which `MAP_ZIP_WITH` key pairs
     validate.
  -   `ARRAY_ZIP_WITH` binds the two lambda parameters to the two array element
     types (both nullable, since the shorter array is padded); return type is `ARRAY<body-type>`.
  -   `MAP_FILTER`, `MAP_TRANSFORM_KEYS`, `MAP_TRANSFORM_VALUES` bind parameter
     0 to the map key type and parameter 1 to the map value type. `MAP_FILTER` requires a boolean
     body and returns the input `MAP<K,V>`; `MAP_TRANSFORM_KEYS` allows any body and returns
     `MAP<body-type,V>`; `MAP_TRANSFORM_VALUES` allows any body and returns `MAP<K,body-type>`.
  -   `MAP_ZIP_WITH` generalizes both maps' key types to their common type `K`
     (the same `LogicalTypeMerging.findCommonType` rule the Table API uses, so both surfaces accept
     the same calls and derive the same type), casts each map operand to `MAP<K,Vn>` where needed —
     as `MAP_UNION` does — and binds the three lambda parameters to `K`, the first map's value type,
     and the second map's value type; return type is `MAP<K,body-type>`. The call is rejected only
     when the two key types have no common type at all.
-   `FunctionType ↔ RelDataType` bridging in `FlinkTypeFactory`, via the new `@Internal`
   `FunctionRelDataType` (and accepting Calcite's `FunctionSqlType` in the reverse direction).
-   `FlinkTypeFactory.leastRestrictive`: let an unresolved lambda parameter (plain `ANY`) defer to
   Calcite's default logic while **preserving** the guard that rejects mismatched generic RAW types
   (narrowed to `instanceof GenericRelDataType`).
-   The same deferral applies to the operators that *build* a type from their operands rather than
   going through Flink type inference: the `ARRAY[...]` and `MAP[...]` value constructors and
   `CAST`. A lambda body is type-derived before the enclosing higher-order call binds the parameter,
   so an operand that (transitively) depends on the parameter is still a plain `ANY`;
   `FlinkTypeFactory`'s collection-type factories and `SqlTypeUtil.convertTypeToSpec` reject that
   placeholder outright. `SqlArrayConstructor`/`SqlMapConstructor` therefore return `ANY` themselves
   in that pass and `SqlCastFunction.canCastFrom` accepts the placeholder; both recompute on the
   re-validation the operand checker triggers once the parameter is bound. The shared predicate
   (`SqlValidatorUtils.containsUnresolvedLambdaParameter`) descends into struct fields, array
   components and map key/value types, since a placeholder also leaks through a composite type
   (`ROW(x, 1)` derives as `ROW(ANY, INT)`), and — as everywhere else in this design — treats
   Flink's encapsulated RAW (`GenericRelDataType`, also `SqlTypeName.ANY`) as a resolved type.
-   **No planner-side code generation for the eight built-ins.** Each is an ordinary
   `BuiltInScalarFunction` in `flink-table-runtime`, named as the `runtimeClass` of its
   `BuiltInFunctionDefinitions` entry and reached through the same bridging path
   (`BridgingSqlFunction` → `BridgingFunctionGenUtil`) as every other built-in with a runtime class.
   The loops therefore live in the runtime classes, not in the planner: a per-element loop for the
   array functions, an accumulator fold for `ARRAY_REDUCE`, a walk to the longer array's length for
   `ARRAY_ZIP_WITH` (`NULL` past the shorter array's end), and per-entry loops for the map functions
   — `MAP_FILTER` copying retained entries, `MAP_TRANSFORM_KEYS`/`MAP_TRANSFORM_VALUES` rebuilding
   the map with the transformed keys/values (`MAP_TRANSFORM_KEYS` rejecting duplicate and `NULL`
   keys), and `MAP_ZIP_WITH` iterating the union of keys, looking each key up in both maps and
   passing `NULL` for an absent value. Each receives its lambda as the `FunctionDataN` object
   described in [*Public Interfaces → 5*](#5-lambda-arguments-for-user-defined-functions) and reads
   its array or map argument as `ArrayData` / `MapData`, so there is no data-conversion boundary at
   the lambda
   call. Every buffered body result — appended to the result array/map, or carried into the next
   iteration as `ARRAY_REDUCE`'s accumulator — is `deepCopy`-ed, which is what makes a mutable body
   result element-wise distinct rather than *N* aliases of the last one
   ([*Public Interfaces → 3*](#3-built-in-higher-order-functions)). This is the alternative an
   earlier draft **rejected** on per-element cost; the rejection is withdrawn because the cost it
   named was an artefact of the lambda ABI, which is now fixed
   ([*Rejected Alternatives*](#rejected-alternatives),
   [appendix A3](#a3-code-generation-mechanics)).
-   Consequently `ExprCodeGenerator` carries no lambda-binding state. A lambda operand is not a
   runtime value, so `visitCall` emits a placeholder for it and the operand is consumed through
   `CallContext#getLambdaArgument`; `visitLambda` and `visitLambdaRef` are reachable only when a
   lambda escapes a higher-order call, and both throw. The single place a lambda body becomes
   executable code — for a built-in and for a UDF alike — is
   `BridgingFunctionGenUtil.generateLambdaFunctionObject`.
-   `FlinkRexUtil`: render `RexLambda`/`RexLambdaRef` in plan strings.
-   Lambda-body closures over outer columns and enclosing lambda parameters (see
   [*Lambda captures*](#lambda-captures) below): `FlinkSqlLambdaScope`, a Flink-owned subclass of
   Calcite's `SqlLambdaScope`, delegates non-parameter identifiers to the parent scope instead of
   rejecting them, and the copied `SqlToRelConverter.convertLambda`/`lookupExp` resolve a reference
   to an enclosing lambda's parameter to that parameter's `RexLambdaRef` and a reference to a column
   as an ordinary input reference of the current row. Nested lambda parameters are assigned distinct
   (cumulative) `RexLambdaRef` indices during conversion, for the reason and with the renumbering
   described in [*Lambda captures*](#lambda-captures).
-   A lambda parameter also shadows a **table alias** of the same name, so that in
   `SELECT ARRAY_TRANSFORM(o.items, o -> o.amount) FROM orders AS o` the body's `o.amount` is the
   field `amount` of the row-typed parameter `o`. Conversion binds every identifier whose first
   component names a parameter to that parameter, and validation must agree with it:
   `FlinkSqlLambdaScope.fullyQualify` qualifies such a compound identifier against the lambda scope,
   checking each further component against the fields of the bound parameter type, and
   `FlinkSqlLambdaScope.resolve` hides an enclosing namespace of the parameter's name — without it
   the validator would derive the type of `o.amount` from the table `o`, and the validated type and
   the converted expression would disagree. Calcite's main branch qualifies compound identifiers the
   same way, but has no equivalent of the `resolve` override.
-   `convertLambda` converts the body in the **enclosing conversion context**, not merely over the
   enclosing inputs: the lambda blackboard inherits the enclosing root, its `AggConverter` when the
   query aggregates, and its registered sub-queries. This is what lets a body reference an aggregate
   or a sub-query of the enclosing query — the body then holds a plain reference into the
   `Aggregate` output, the shape capture lifting already handles. It is also required for
   correctness of an ordinary capture: in "agg mode" a group key must resolve against the
   `Aggregate` output, which agrees with the pre-aggregation row only when the `GROUP BY` order
   happens to match the input order. Relatedly, `FlinkSqlLambdaScope.isParameter` — whose only
   caller is Calcite's `AggChecker` — considers the parameters of enclosing lambdas as well, so that
   a nested body closing over one is not reported as an ungrouped column.

### Lambda captures

A lambda body may reference free variables of the surrounding scope: columns of the surrounding
query, e.g. `ARRAY_TRANSFORM(arr, x -> x + base)`; for nested higher-order calls, the parameters of
an *enclosing* lambda, e.g. `ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))`; and
values the enclosing query produces once per row rather than per element — an `OVER` window
(`RexOver`) or a sub-query (`RexSubQuery`). Calcite models a `RexLambda` as a *closed* expression
(only parameters and body, no place for captured values). The design therefore **closure-converts**
every free variable out of the lambda (`HigherOrderFunctionUtil.liftCaptures`), so the `RexLambda`
that reaches code generation, serialization, and user code stays closed:

- each distinct free variable in the body — an outer `RexInputRef` (column), a free `RexLambdaRef`
  (a parameter bound by an enclosing lambda), or a whole `RexOver`/`RexSubQuery` — becomes an
  additional, trailing lambda parameter, and
- the corresponding free variable is appended as a trailing operand of the enclosing call, where it
  is evaluated in the scope that binds it.

An `OVER` window and a sub-query are captured *whole*, without descending into them: their operands
belong to the enclosing row, and rewriting those into references to the lambda's parameters would be
meaningless. An aggregate over an outer column needs no rule of its own — the conversion has already
turned it into a reference into the `Aggregate` output, so the body sees an ordinary column capture.

So `ARRAY_TRANSFORM(arr, x -> x + base)` becomes `ARRAY_TRANSFORM(arr, (x, cap) -> x + cap, base)`,
and `ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x + a[1]))` becomes `ARRAY_TRANSFORM(a2d, a
-> ARRAY_TRANSFORM(a, (x, cap) -> x + cap[1], a))` — the inner call gains the trailing operand `a`,
evaluated in the outer lambda's body where `a` is bound. Captures more than one level out are
**threaded hop by hop**: each intervening lambda gains the capture as a parameter and forwards it
inward, so a lifted reference never skips a scope. The built-in code generation evaluates the
trailing operands at the call site (hence at the correct loop-nesting depth — once per row for a
column, once per enclosing element for an enclosing parameter) and binds the lifted parameters to
their values; user-defined higher-order functions consume the same lifted form via their function
object (see [*User-defined functions*](#user-defined-functions)).

Because a nested lambda's parameters would otherwise collide with an enclosing lambda's on Calcite's
*positional* `RexLambdaRef` index (index `0` for the first parameter of every lambda, so a free
reference to an enclosing parameter is indistinguishable in the `RexNode` layer from the inner
lambda's own first parameter), the front end assigns nested lambda parameters distinct indices while
converting, so that free references are identifiable; `liftCaptures` then lifts them and renumbers
each closed lambda's parameters. The *representation-level* clean fix — a scope-qualified
`RexLambdaRef` so nested parameters are inherently distinguishable in the IR, serde, and plan
digests — belongs upstream in Calcite; closure-converting every free variable out of the lambda
avoids needing it, keeping the `RexLambda` closed so no IR/serde change is required.

The renumbering is not to the ordinary 0-based positional form throughout. A lambda's own parameters
keep `0, 1, 2, …`, but the lifted capture parameters appended after them are numbered from a
reserved base (`HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE`, 1,000,000), so **a lambda
reference is not purely positional**. The index, not the name, is what marks a parameter as lifted,
and it has to be, because `RexLambdaRef#equals` compares only index and type — a parameter's name is
deliberately not part of its identity, since two lambdas differing only in parameter names denote
the same function. Calcite deduplicates such lambdas when it builds a `RexProgram`, so without the
reserved base a capturing lambda and a non-capturing one could be merged into a single node carrying
whichever call site's names were converted first. Numbering captures apart keeps the two
structurally distinct, and lets every downstream stage — the operand checker, the `CallContext`, the
code generator — recover which parameters are lifted from the lambda alone, with no side channel.
The base is chosen far above any reachable parameter count: a lambda is handed to `eval()` as one of
the five `FunctionDataN` interfaces, so a function can declare at most four user-visible
parameters.

Lifting is applied uniformly to both surfaces and to built-in and user-defined calls: the SQL path
runs it as a `RelHomogeneousShuttle` in `FlinkPlannerImpl.rel` (before decorrelation/trimming); the
Table API applies it during expression conversion. Both drive it bottom-up, so an inner call is
closed before its enclosing call is processed. Because captures become ordinary operands and the
lambda stays closed, compiled-plan (de)serialization needs no special handling.

**Capturing across a sub-query boundary (correlation).** A lambda evaluated inside a sub-query may
close over a column of the *enclosing* query — as in `SELECT (SELECT ARRAY_TRANSFORM(a, x -> x +
t.a)[1]) FROM t`, where the body references `t.a` from outside the scalar sub-query. Such a
reference is not an input reference at conversion time but a *correlated* field access (`$cor.a`),
so the capture-lifting above cannot hoist it then — the value only becomes an ordinary column of the
lambda's row once the correlation is removed. Decorrelation rewrites `$cor.a` into a plain input
reference of the decorrelated row, but in the pinned Calcite 1.41.0 decorrelation traverses
`RexNode`s with visitors and shuttles that do not descend into a `RexLambda` body (the same
limitation behind the determinism patch below), so without help the correlation is left untouched
inside the lambda and leaks into the plan. The design closes this in three steps — the two that patch
vendored Calcite classes are fenced with `FLINK MODIFICATION` markers and are deletable on the
Calcite 1.42.0 upgrade, which fixes the underlying `RexShuttle` behaviour upstream (see
[*Calcite fork*](#calcite-fork-what-is-upstream-what-is-residual)) — the third lives in Flink's own
`FlinkDecorrelateProgram`:

- `org.apache.calcite.sql2rel.RelDecorrelator` is patched to descend into and rewrite lambda bodies
  (its discovery visitor, `DecorrelateRexShuttle`, and `RemoveCorrelationRexShuttle` each gain a
  `visitLambda` that rebuilds the lambda from a rewritten body), and to preserve a higher-order
  call's result type when an operand changes — that type is invariant under decorrelation, whereas
  re-deriving it for a call whose lambda operand was rewritten yields `ANY` and later fails type
  conversion.
- The rewrite leaves the (now input-reference) capture un-lifted *inside* the still-closed lambda,
  so a post-decorrelation pass in `FlinkDecorrelateProgram` re-runs capture lifting bottom-up over
  the decorrelated plan (descending into lambda bodies), hoisting the reference back out as a
  trailing capture operand and restoring the closed-lambda invariant. The pass is a no-op for
  lambdas that are already closed.
- When a lambda captures two or more distinct outer columns, each enters as its own correlation
  variable; Calcite consolidates them in `SqlToRelConverter.getCorrelationUse` via
  `DeduplicateCorrelateVariables`, whose `RexShuttle` again does not enter lambdas on 1.41.0, so a
  secondary variable survives inside the body and the `Correlate` is never formed. A supplementary
  lambda-aware remap in `getCorrelationUse` finishes the de-duplication inside lambda bodies. As a
  safety net, `FlinkDecorrelateProgram`'s leftover-correlation check also descends into lambdas, so
  any correlation that still escapes surfaces as a clean planning error rather than a code-generation
  crash.

**Lambda-body determinism.** A call is only as deterministic as its lambda body: e.g.
`ARRAY_TRANSFORM(a, x -> RAND())` is non-deterministic. Calcite's `RexVisitorImpl.visitLambda`
returns without descending into the body, so the stock `RexUtil.isDeterministic` would wrongly treat
such a call as deterministic and expose it to unsafe common-subexpression elimination, predicate
pushdown, or result caching. Flink therefore carries a minimal patch to the vendored
`org.apache.calcite.rex.RexUtil` (marked `BEGIN/END FLINK MODIFICATION`) that visits the lambda body
when computing determinism, so a non-deterministic body correctly makes the enclosing higher-order
call non-deterministic. CALCITE-6242 fixes this upstream in 1.43.0 by making `RexVisitorImpl`
recurse when `deep`, so the patch can simply be deleted on that upgrade.

The patch alone is not enough, because two of Flink's own traversals compute determinism without
going through it, and both are lambda-blind for the same reason:

- `ShortcutUtils.isDeterministicThroughProgram` resolves `RexLocalRef`s against the enclosing
  `RexProgram`'s expression list itself instead of delegating to `RexUtil`, and only falls back to
  the patched entry point when there is no program. It is what `ExprCodeGenerator` consults before
  caching a local reference's generated value — the guard that keeps two occurrences of the same
  non-deterministic expression from collapsing into one evaluation. Since `RexProgramBuilder`
  collapses structurally identical expressions (lambda parameter *names* do not enter the
  comparison), without lambda descent `SELECT ARRAY_TRANSFORM(a, x -> nd(x)), ARRAY_TRANSFORM(a, y
  -> nd(y))` evaluates the body once and returns the same array for both columns, while the scalar
  equivalent `SELECT nd(1), nd(1)` correctly evaluates twice.
- `FlinkRexUtil.getNonDeterministicCallName` is the entry point of the non-deterministic-update
  (NDU) analysis in `StreamNonDeterministicUpdatePlanVisitor`. Without lambda descent a
  non-deterministic body in a projection or condition passes the streaming determinism requirement
  unreported.

Both descend into `RexLambda` bodies. They are Flink code, so unlike the `RexUtil` patch they stay
after the Calcite 1.43.0 upgrade.

A lifted `OVER` window needs one further step, shared by both surfaces: it may not remain in the same
projection as the call. `ProjectToWindowRule` slices a projection containing a window into a windowed
and a non-windowed part, and the slicing works on the flattened `RexProgram` of the projection, in
which a lambda is an expression of its own — so the lambda would be materialized as an output field
of the non-windowed part, where it is meaningless (a lambda is only an argument of the call that
applies it). A planner rule, `ExtractLambdaOverWindowRule`, therefore moves such windows into a
projection **below** the call and leaves an input reference behind, giving the plan a user would get
by writing the window in a sub-query. It is registered in the **batch and stream** default rewrite
rule sets, immediately before `ProjectToWindowRule`. Sub-queries need no equivalent: a `RexSubQuery`
operand is handled by the existing sub-query rewrite.

A lifted capture parameter is named with a reserved `cap$` prefix
(`HigherOrderFunctionUtil.CAPTURE_PARAM_PREFIX`), which lets user-defined-function code generation
recognize and strip the trailing capture operands that are not part of the function's declared
arguments (`BridgingSqlFunctionCallGen`). On the SQL path lambda parameter names are user-chosen, so
to keep this convention unspoofable the validator rejects lambda parameter names beginning with the
reserved prefix (`FlinkCalciteSqlValidator.validateLambda`, with the message added to Flink's
`ExtraCalciteResource`); the `$` character cannot appear in an unquoted
identifier, so only a deliberately back-quoted name such as `` `cap$x` `` is affected. Table API
parameter names are framework-generated and never collide.

A lambda's own parameter names must also be **unique**. Both surfaces bind parameters by name — the
validator's lambda scope keys them by name, and so does the Table API resolver — so a repeated name
would collapse into a single parameter, leave the first one unreferenceable, and let the declared
arity disagree with the derived `FUNCTION` type. `FlinkCalciteSqlValidator.validateLambda` (again
with the message in `ExtraCalciteResource`) and the `UnresolvedLambdaExpression` constructor reject
it, in both cases before the body is resolved. Shadowing an *enclosing* lambda's parameter stays
allowed; only a repetition within one parameter list is rejected.

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

The capture nesting depth is what places each evaluation at the right loop level: `base` is
loop-invariant for the whole row, while `f` is bound by the outer element loop, so the inner call
threads it in as a trailing operand evaluated once per outer element (see the rewrite above).

### Table API (`flink-table-api-java`, `flink-table-common`)
-   An `UnresolvedLambdaExpression` (parameter names + body) and a resolved `LambdaExpression` whose
   parameters are `LocalReferenceExpression`s; the resolved lambda carries a `FunctionType` output
   type. `LambdaExpression` is `@PublicEvolving` and exposes its parameters via `getParameters():
   List<LocalReferenceExpression>`; consequently `LocalReferenceExpression` is promoted from
   `@Internal` to `@PublicEvolving`, so no internal type leaks through the public expression API.
-   The eight higher-order functions are also registered as `BuiltInFunctionDefinition`s (with a
   lambda-aware input strategy and the result-type strategy above) so the resolver can type-check
   them. They also declare a **runtime class**, which is what makes them ordinary bridged built-ins:
   the SQL stack reaches them through `BridgingSqlFunction` and `TypeInferenceOperandChecker` rather
   than through a hand-written `FlinkSqlOperatorTable` operator with an operand type checker of its
   own, so both surfaces validate against the *same* strategies.
-   That shared strategy is what makes every argument rule hold on **both** paths, but it does not
   make the paths identical: SQL reaches the strategy through Calcite's validator and the Table API
   through expression resolution, and each derives the lambda's parameter types itself before the
   strategy sees them. A rule enforced on only one path would leave the other to fail at runtime and
   would make the same call produce a different result type depending on the surface it was written
   on, which is why the parity harness asserts both ([*Test Plan*](#test-plan)). Where a
   rule compares or merges two types, both paths must additionally use the *same* merging function:
   the two functions that relate two independently derived types — `MAP_ZIP_WITH` (the two key
   types) and `ARRAY_REDUCE` (accumulator vs. reducer body) — use
   `LogicalTypeMerging.findCommonType` on both surfaces (see
   [*SQL planner*](#sql-planner-flink-table-planner) above for the rule,
   [*Rejected Alternatives*](#rejected-alternatives) for why not Calcite's `leastRestrictive`).
   Concretely, `MAP_ZIP_WITH`'s input strategy uses that merged key type — nullable if either map's
   key type is — for both the lambda's key parameter and the result map, matching the key type the
   planner derives for SQL. What a user is *told* when a call is rejected needs no such rule: the
   expected-signature text is rendered from the input strategy's `getExpectedSignatures` on both
   surfaces, so there is one description of a function's signature and not two that could diverge.
-   Expression-resolver support to bind lambda parameter types from sibling arguments and resolve
   the body in a scope containing the lambda parameters, the parameters of any enclosing lambdas,
   **and** the surrounding query's input columns, so the body may close over enclosing lambda
   parameters and over columns (inner parameters shadow enclosing parameters, and parameters shadow
   columns, of the same name). Both kinds of capture are closure-converted out of the lambda (see
   [*Lambda captures*](#lambda-captures)). Lambda arguments are never wrapped in an implicit `CAST`.
   The body's scope also inherits the enclosing scope's registered over-windows, so a body may
   reference one (`$("base").sum().over( $("w"))`).
-   Aggregate extraction (`OperationExpressionsUtils`) descends into lambda bodies. The Table API
   does not resolve an aggregate in place: it hoists each one out of the unresolved projection into
   the enclosing aggregation and leaves a reference behind. A lambda body was previously invisible
   to that pass, because `UnresolvedLambdaExpression` hides its body from generic traversals — it
   must be resolved in the lambda's own scope. Descending into it is what lets `a.arrayTransform(x
   -> x.plus($("base").sum()))` mean the same as the SQL form: the aggregate becomes an ordinary
   column of the aggregation's output that the body captures. This is the Table API's own route to
   the behaviour the SQL path reaches through `convertLambda`; an aggregate over a lambda
   *parameter* is rejected here instead, since it has no such reading.
-   `ExpressionConverter`: `LambdaExpression → RexLambda` (parameters become `RexLambdaRef`s indexed
   in declaration order, offset by the enclosing lambdas' parameter count so nested parameters stay
   distinguishable; `liftCaptures` renumbers them to 0-based). The enclosing higher-order call is
   built with the result type taken from the resolved call, because a `RexLambda` only carries its
   body type — the `FUNCTION` type that the operator's return-type inference consumes exists only
   during SQL validation.
-   DSL methods (`arrayTransform`, `arrayFilter`, `arrayReduce`, `arrayZipWith`, `mapFilter`,
   `mapTransformKeys`, `mapTransformValues`, `mapZipWith`) that accept a host-language lambda.

### Python (`flink-python`)
- Python `Expression` methods mapping to the Java lambda expression via the existing gateway; type
  mapping and docs.

### User-defined functions
-   the `LambdaInputTypeStrategy` interface and `CallContext.getLambdaArgument(...)`
   returning a `LambdaInfo` (parameter fields, result type, and the body expression once the
   function is being specialized). A function derives each lambda parameter's type from the call
   itself in `getExpectedLambdaParameterTypes(...)` — typically the element type of an array
   argument, the key or value type of a map argument, or the type of another argument for
   accumulator-shaped parameters. Any other derivation is expressible too, since the function
   computes the types itself. Lambda parameter binding is unified across built-ins and UDFs via
   `LambdaInputTypeStrategy`, which both implement, so the expression resolver derives the parameter
   types the same way for both.
-   Function-kind scoping (see
   [*Public Interfaces → 7*](#7-function-kinds-and-process-table-functions)):
   implementing `LambdaInputTypeStrategy` *is* the declaration that a function takes a lambda
   argument — there is no separate marker method to answer the question, and every binding path
   decides it with the same `instanceof LambdaInputTypeStrategy` check. `SystemTypeInference.of` —
   which both the SQL bridging layer and the Table API's
   `ResolveCallByArgumentsRule` route every call through — uses that to reject a lambda argument
   declared on a function kind that cannot host one (everything except `SCALAR`, `TABLE` and
   `PROCESS_TABLE`); `BridgingSqlAggFunction`, which builds its own inference rather than going
   through `SystemTypeInference.of`, calls the same check. The same check admits a `FUNCTION`-typed
   static argument only for a process table function whose input type strategy implements
   `LambdaInputTypeStrategy`, since a static signature has no channel for deriving a lambda's
   parameter types from its siblings; `StaticArgument` itself rejects an optional or a nested
   `FUNCTION` where it is declared. For a PTF, `SystemTypeInference` wraps the declared strategy in a
   system input strategy that keeps implementing `LambdaInputTypeStrategy` and checks the derived
   arity against the declared `FUNCTION(n)`. Every message names the alternative, and together they
   keep a future aggregate lambda additive.
-   Process table functions (see
   [*Public Interfaces → 7*](#process-table-functions-ptfs)): `ProcessTableRunnerGenerator` builds
   the function object for a `FUNCTION`-typed operand in the generated `callEval()`, through the same
   `BridgingFunctionGenUtil` code path used for scalar and table functions. `HigherOrderFunctionUtil`
   rejects a capture in a PTF's lambda before lifting it, and `TypeInferenceOperandChecker` declares
   the SQL parameter type of a `FUNCTION`-typed static argument as `ANY`, which Calcite's operand
   matching for table function calls accepts.
-   The lambda reaches the UDF's `eval` method as a first-class function object: the
   `org.apache.flink.util.function.FunctionN` matching its arity (the conversion class of the
   `FUNCTION` type), with `Function`, `BiFunction` and `TriFunction` accepted as equivalents at
   arities one to three. The UDF simply calls `lambda.apply(...)` and owns its loop; it does not
   implement `SpecializedFunction`. Because a
   `RexLambda` only carries its body type at the `RexNode` level, the planner `CallContext`
   synthesizes the `FUNCTION` argument type for a lambda operand.
-   The framework generates the function object in `BridgingFunctionGenUtil`, and does so for a
   built-in higher-order function exactly as for a user-defined one — this is the *only* place a
   lambda body becomes executable code. It compiles the body into an `ExpressionEvaluator` (via the
   `RexLambda → RexNodeExpression` bridge that rewrites each `RexLambdaRef` to an input reference
   over the parameter fields), and the generated evaluator class implements `LambdaFunctionFactory`.
   Its `bindLambda` method binds an `Object[]` containing the captures and returns an anonymous
   functional-interface object whose `apply` casts the arguments to the parameters' declared classes
   and calls the evaluator's typed `eval` directly. There is no `MethodHandle`, no `Object[]`
   allocation or adaptation **per application**; the capture array is allocated once per function
   object, including as an empty array for a non-capturing lambda. The evaluator is opened once in
   the enclosing generated class (the operator, or — for a lambda nested in another lambda body —
   the enclosing expression evaluator, which supplies a `FunctionContext` without a runtime
   context) and acts as the factory for the per-evaluation objects. The `FUNCTION` operand is
   therefore included as a runtime `eval` operand (a value);
   unlike all other operands it is passed through without conversion, because it *is* the
   representation. (How a *nested* evaluator's generated class is kept distinct from its enclosing
   one is in [appendix A3.2](#a32-the-generated-function-object-for-a-lambda).)
-   How the lambda object contract
   ([*Public Interfaces → 5*](#5-lambda-arguments-for-user-defined-functions)) is realized. The
   object is an anonymous conversion-class instance **constructed at the call
   site**, so a fresh one exists per evaluation of the enclosing call, with the capture values
   evaluated just before it and bound to locals behind it — which is what makes the lifetime,
   identity, and capture-snapshot rules true by construction rather than by convention. The
   evaluator behind it is a **single instance per call site**, opened in the enclosing generated
   class's `open()` and held in a `transient` field; it carries the ordinary per-instance
   code-generation state (reused buffers, converters), which is the reason the object is
   single-threaded and must not be applied concurrently. `apply` rethrows an unchecked exception
   from the body unchanged, so a UDF lambda fails with exactly the error a built-in given the same
   body raises — they are the same generated code — and wraps a checked exception, which the
   functional interface cannot declare, in `FlinkRuntimeException` with the original exception as
   its cause. Symmetrically, the evaluator
   is registered as a reusable **close** statement of the enclosing generated class, and the
   generated evaluator class itself emits a `close()` carrying its reusable close code; a function
   called from a lambda body is therefore closed with the operator, exactly like one called outside
   a body. Nothing in the object is exposed to the UDF beyond `apply`, and none of it exists during
   the UDF's own `open`/`close`.
-   Captures: a UDF lambda may close over outer columns and over enclosing lambda parameters.
   Lambda-capture lifting (see [*Lambda captures*](#lambda-captures)) turns those into extra closed
   lambda parameters and trailing call operands; the framework strips the trailing capture operands
   from the call used for the UDF's own type inference (which only knows its declared arguments),
   evaluates them at the call site, and binds them behind the function object. The `FUNCTION` type
   that the planner `CallContext` synthesizes for a lambda operand likewise excludes the lifted
   capture parameters, so the arity the UDF sees — and hence which conversion class it gets — is
   the lambda's declared arity; the captures remain visible only in the
   `LambdaInfo` parameter fields used by code generation. Because an enclosing lambda parameter is
   not loop-invariant, each captured value is bound to a fresh local per iteration before the
   function object is (re)built. Any function with Flink type inference that appears in a lambda
   body and depends on an enclosing lambda parameter — a nested UDF call, or an ordinary built-in
   applied to the parameter such as `IFNULL(param, 0)` — has its operand and return type inference
   deferred while that parameter is still an unresolved (transient plain `ANY`) placeholder, and
   re-run once the enclosing operand checker binds it (mirroring the built-in array operand
   checkers). So a lambda parameter is usable by any function in the body, not only by expressions
   that pass it through unchanged. Because this guard runs for *every* type-inference-backed call,
   it inspects the operands as `SqlCallBinding#operands()` reports them — named arguments permuted
   and optional ones padded with `DEFAULT` — and derives each operand's type from the node rather
   than by position in the raw call.
-   For SQL calls (`SELECT my_udf(a, x -> ...)`), the bridging operand type checker binds the lambda
   parameter types from the sibling arguments and re-validates the lambda body (analogous to the
   built-in array operand checkers); the same binding is shared with the operand type inference so
   the lambda carries a concrete `FUNCTION` type before it is inspected. Calcite runs that inference
   on the call as written, whose `SqlCallBinding#operands()` are permuted into the declared order
   but whose `getOperandType(i)` is not; for a call with named arguments the inference therefore
   binds the permuted call — what the operand checker sees — so a strategy derives each lambda's
   parameter types from the sibling it addresses, whatever order the arguments are named in. Only a
   PTF's static signature admits a named lambda argument.
-   Binding failures are reported as validation errors on both surfaces. A lambda whose parameter
   count differs from the declared one, or a lambda passed at an argument position that does not
   declare a lambda, fails with a message naming the position (the Table API reports the equivalent
   errors from the expression resolver). Two properties of the SQL path are worth recording, because
   both are easy to regress: an invalid *argument count* is reported before any lambda binding is
   attempted, so a call with too few or too many operands still yields the usual signature mismatch
   rather than a lambda error; and a lambda-binding failure is reported on its own, without the
   argument types of the call, since an unbound lambda parameter stays a plain `ANY` and rendering
   the synthesized `FUNCTION` type of such an operand would itself fail.
-   Multiple lambda arguments: a UDF may declare more than one lambda argument. Each lambda operand
   is lifted independently by `HigherOrderFunctionUtil.liftCaptures`, and its captures are appended
   as trailing call operands in left-to-right operand order. Because each lambda's `LambdaInfo`
   parameter fields record its own lifted (`cap$`-named) parameters, code generation
   (`BridgingSqlFunctionCallGen` / `BridgingFunctionGenUtil`) partitions the trailing operands back
   to the owning lambda by consuming, per lambda, as many as it declares. The operand type checker
   already binds each lambda position independently, so no other machinery changes.

### Compiled plan (`flink-table-planner`)
-  `RexNodeJsonSerializer`/`Deserializer`: `RexLambda` and `RexLambdaRef`. A lambda is serialized as
  its parameter references (name, index, type) plus its body expression; the `FUNCTION` type itself
  is not serialized but recomputed via `RexBuilder.makeLambdaCall` on deserialization (see
  [*Design overview*](#design-overview)).
  -  **What a plan pins, and what it recomputes.** Nothing about a lambda's types is re-derived from
    type inference when a plan is read: each parameter's index, name and type, the body's result
    type, the enclosing higher-order call's result type and the node's output type are all written
    into the plan. The only recomputed part is the `RexLambda`'s own `FUNCTION` type, which carries
    nothing but the arity — and the arity is itself pinned by the serialized parameter list. A later
    change to how a type is *derived* can therefore change what new plans look like, which the plan
    and IT tests cover, but it cannot silently retype an old one.
  -  **A serialized lambda is always closed, and its numbering is a documented convention.** The
    positional ambiguity that `RexLambdaRef` has in the IR (see
    [*Residual after both upstream fixes*](#residual-after-both-upstream-fixes) → 4) exists only
    between conversion and capture lifting: `liftCaptures` closure-converts and renumbers every
    lambda, and `checkClosed` asserts the result, before anything is serialized. The plan format
    never carries a nested or scope-qualified reference, which is why no IR or serde change is
    needed for it. What it does carry is the numbering described in
    [*Lambda captures*](#lambda-captures): a lambda's own parameters at `0, 1, 2, …` and its lifted
    captures from `1000000` upward. Those large indices are visible in a compiled plan and are part
    of its contract — they are what makes a capturing lambda structurally distinct from a
    non-capturing one, not a defect.
  -  **Restoring re-validates that invariant.** A plan is an external input that another version may
    have written, so `RexNodeJsonDeserializer` runs `checkClosed` on every lambda it reads and
    additionally checks that each body reference agrees with the name and type the plan declares for
    that position, and that the capture parameters form a contiguous run from the reserved base. A
    future change to the numbering convention then fails the restore with a clear error instead of
    binding a reference to the wrong parameter.
-  `RestoreTestBase` coverage for `Calc` nodes containing the eight functions, a capturing built-in
  lambda, and a user-defined higher-order function with one and with two capturing lambdas. Because
  capture lifting rewrites both the lambda's parameter list and the call's operand list, the lifted
  `cap$` parameters and their trailing operands are part of what is serialized and restored. A
  `ProcessTableFunction` node is covered by a set-semantic PTF whose lambda argument folds each
  partition into keyed state, so the restored lambda is applied to state written before the
  savepoint.
  -  **What these resources do and do not demonstrate.** The committed resources (11 batch plans, 12
    stream plans, and 12 stream savepoints) are written *and* read by this version, so what they
    verify today is that a plan containing a `RexLambda` — with its lifted capture parameters and
    operands — survives serialization, deserialization and a savepoint restore unchanged. They are
    **not** evidence of restoring a plan or state produced by a *released* Flink version, which is
    impossible for a feature that does not exist in any release yet. Their compatibility value is
    prospective: they become the frozen baseline that every later version is tested against, which
    is exactly how every other `ExecNode` version-1 restore resource in this repository enters the
    tree.

## Known limitations

-  **A lambda is applied through a function object, not inlined into the caller's loop.** Every
  lambda — a built-in's and a user-defined function's alike — is a generated object whose `apply`
  calls the compiled body ([appendix A3.2](#a32-the-generated-function-object-for-a-lambda)), where
  an expression written outside a lambda is straight-line code. The dominant cost is not the call
  itself, which measures as nearly free, but the `Object`-typed ABI around it: an element is boxed
  on the way into `apply` and its result boxed on the way out, so a primitive element type pays
  32 bytes per element that straight-line code does not. The cost is bounded and measured on an
  isolated per-element harness: a built-in runs at 1.00×–1.31× of its inlined predecessor for
  `ARRAY<INT>` with the cheapest possible body, and at 0.96×–1.07× for `ARRAY<STRING>` or any more
  expensive body ([appendix A3.3](#a33-what-the-lambda-abi-costs)) — which is why no inlined variant
  survives in the design. The remaining headroom is larger than that gap and does not need inlining
  to reach ([appendix A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)).
  Two properties remain **unmeasured** and are stated as such: the size of the methods generated for
  a large lambda body, and the JIT behaviour of deeply nested lambdas (each nesting level adds an
  evaluator whose `apply` is a further call site). A third — that one runtime loop is shared by every
  call site of that built-in in the JVM, so its `apply` site sees one generated class per distinct
  lambda running anywhere in the TaskManager — *is* measured, and it is small and self-limiting:
  1/2/3/5 distinct lambda classes through `ArrayTransformFunction.eval` cost 6.84/7.36/7.82/7.53 ns
  per element, i.e. it plateaus at the megamorphic case rather than degrading further. What *is*
  pinned by a test is the correctness-critical part — that a body far beyond
  `table.generated-code.max-length` still splits and compiles (see
  [*Test Plan*](#test-plan)), so a large body fails to be fast rather than failing to run.
-  **`FUNCTION` is not a storable type** — not a column, persisted return type, or state type, and
  not nested inside one of those or inside a static-signature argument. Every one of those checks
  is recursive.
-  **A lambda object does not outlive the `eval` call it is passed to**, may only be applied from
  that call's thread, and is therefore unusable anywhere a UDF would want to defer or parallelize
  work — no retaining it in a field or in state, no executor, no asynchronous completion. Neither
  rule is checked at runtime, so violating one is undefined behaviour rather than an error. The full
  normative contract, why it is the shape it is, and why it is not enforced, are in *Public
  Interfaces → 5*.
-  **Only scalar, table, and process table functions may declare a lambda argument.** Aggregate,
  table-aggregate, and asynchronous functions may not; each is rejected with a message naming the
  kind. The reasoning, and why a later extension is additive rather than breaking, is in
  [*Public Interfaces → 7*](#7-function-kinds-and-process-table-functions). A higher-order *call* is
  unaffected: it is an ordinary scalar expression and may be used as an argument of, inside the
  input query of, or over the result of any of these functions.
-  **A process table function's lambda argument is self-contained.** Its body may reference only its
  own parameters, literals, and the parameters of lambdas nested in it; a captured sub-query is
  rejected. It must be declared through `getTypeInference()`, since extraction from `eval` yields
  the lambda's position and arity but no `LambdaInputTypeStrategy`, and it must not be optional. See
  [*Process table functions*](#process-table-functions-ptfs).
-  **Lambdas support at most four parameters.** The `FUNCTION` type's conversion class — what a UDF
  receives its lambda argument as — is the member of the `org.apache.flink.util.function.FunctionN`
  family matching its arity, from `Function0` to `Function4`. Higher arities have no conversion
  class.
  This bound and its lower counterpart are **different kinds of constraint**, and only the upper one
  is a limitation: the upper bound is a *representation* limit — how many functional interfaces
  Flink owns — while zero parameters is simply supported, because Calcite's grammar
  (`SimpleIdentifierOrListOrEmpty`) already accepts `()` and nothing downstream needed a lower
  bound.

  The family is a single naming scheme across the whole range, which is the reason it exists rather
  than reusing `Function`/`BiFunction`/`TriFunction` where they happen to fit. `java.util.function`
  has no zero- or four-argument type, so any scheme built on the JDK types alone would have to break
  uniformity at exactly the two arities where a user is least able to guess the name — and the
  resulting mixture (`Function0`, `Function`, `BiFunction`, `TriFunction`, `Function4`) is precisely
  the sort of thing a reader has to memorise rather than derive. With `FunctionN` the arity is
  readable off the name at every position. The internal `FunctionDataN` family follows the same
  scheme for the same reason, so the two families line up member for member.

  `Function1`, `Function2` and `Function3` **extend** `java.util.function.Function`,
  `java.util.function.BiFunction` and `org.apache.flink.util.function.TriFunction` respectively and
  add nothing, and those three types remain **accepted conversions** alongside the family member: a
  UDF may declare either spelling on its `eval` parameter, and either may be named in
  `bridgedTo(...)`, with the declared one being what the code generator implements. A Java developer
  who reaches for `BiFunction` is therefore not penalised for it. `Function4` is standalone, because
  no four-argument functional interface exists in `java.util.function` or
  `org.apache.flink.util.function`. `Function0` is the one asymmetry: the generated lambda object's
  method is always named `apply`, so `java.util.function.Supplier`, whose single abstract method is
  `get()`, cannot be implemented by the generated object and therefore cannot be an accepted
  conversion. `Function0` instead `extends Supplier`, declares `R apply()` as its own single
  abstract method and defaults `get()` to it — so a UDF may still *declare* a plain `Supplier`
  parameter and receive the generated `Function0` for it, but `Supplier` alone cannot be bridged
  to. That is the sole place in the range where the two notions come apart.

  A function that declares a lambda argument of an unsupported arity is rejected where it is
  declared — the derived parameter types are checked against it with "A lambda argument must have
  between 0 and 4 parameters, …" — rather than failing later with a conversion-class error. Both
  binding paths (`ResolveCallByArgumentsRule` for the Table API,
  `TypeInferenceOperandChecker` for SQL) validate the derived parameter types right after
  `getExpectedLambdaParameterTypes(...)` returned, through the same `@Internal`
  `LambdaTypeValidation` helper so the two cannot drift, and reject more than four derived
  types with "A lambda argument must have between 0 and 4 parameters, but *n* parameter types
  were derived. A lambda is passed to a user-defined function as an
  `org.apache.flink.util.function.FunctionN`, where N is its number of parameters and ranges from
  `Function0` to `Function4`, so no other
  number of parameters can be represented at runtime." The public factory `DataTypes.FUNCTION(n)`
  carries the same bound: it rejects any `n` outside zero to four with the same wording ("…but a
  `FUNCTION` type with *n* parameters was requested. …"), so a caller of the declared public API
  never sees a conversion-class error about a class it did not name. The public `FunctionType`
  constructor applies the same bound with the same message, so the type cannot be constructed
  through public API in a state whose conversion class does not exist. A lambda that carries lifted
  captures can exceed four parameters, and two internal paths genuinely construct such a type:
  `FlinkTypeFactory.toLogicalType`, converting a Calcite `FunctionSqlType` whose field count
  includes the lifted `cap$` parameters, and `LogicalTypeParser`, reading back the
  `asSerializableString()` form (`FUNCTION(5)`) that every logical type must round-trip through.
  Both go through the `@Internal` factory
  `FunctionType.ofUncheckedArity(int)`, which skips only the convertibility check and still rejects
  a negative count. A compiled plan is *not* one of those paths: it stores the `RexLambda`'s
  parameters and body and never the `FUNCTION` type, which is recomputed on deserialization.
  `OperatorBindingCallContext` also calls the internal factory, but filters the `cap$` parameters
  out first, so the count it passes is the user-visible arity that the public constructor would
  accept anyway. The unchecked arity is reachable from internal code by name, and not from the
  public API by accident. Four is one above every
  built-in delivered here (`MAP_ZIP_WITH` is the widest, at three), and one above where the two
  engines that face the same problem land, leaving room for a plausible next function — a
  four-array `ZIP_WITH`, or a windowed fold — without a further surface change. ksqlDB — the only
  other engine whose user-defined functions may
  take a lambda — arrives at its bound by the identical construction, differing only in which fixed
  set it draws on: where Flink declares the `FunctionN` family and so stops at `Function4`, ksqlDB's
  UDF loader recognises a lambda parameter as `Function`, `BiFunction`, or its own `TriFunction`
  (`UdfUtil`, `FunctionLoaderUtils`) — a borrowed set whose widest member is the three-parameter one,
  and its documentation states the consequence ("Currently, ksqlDB supports
  up to three arguments in a single lambda function"). Spark hits the same wall wherever a lambda
  crosses an API boundary rather than staying inside SQL: `functions.scala` offers exactly three
  private `createLambda` overloads (`Column => Column`, `(Column, Column) => Column`, `(Column,
  Column, Column) => Column`) and PySpark rejects anything else outright ("function arity is between
  1 and 3", error class `WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION`), with the lambda variables
  named `x`, `y`, `z`. The engines with no cap are the engines with no user-defined higher-order
  functions — Trino gives each built-in its own functional interface, kept in `trino-main` and out
  of the plugin SPI, so the question never reaches a user (see
  [*Naming in other engines*](#naming-in-other-engines)). Extending to a fifth arity is
  **source-compatible and additive**, and the family exists to make it so: the
  arity→conversion-class mapping is a lookup table indexed by arity, so a new arity is one new
  interface plus two table entries, existing UDFs are unaffected, and both the `FUNCTION` type and
  the lambda representation already carry an arbitrary parameter count. Nothing in the planner,
  the code generation, the compiled-plan format or the Python API is arity-bound — a prototype
  taken to arity 22 changed none of them (see
  [*Rejected Alternatives → Committing to a wide `FunctionN` family now*](#rejected-alternatives)).
-  **A lambda body is a scalar expression over the row it is evaluated in.** It is compiled into an
  expression that runs once per element, so it may reference its own parameters, the parameters of
  enclosing lambdas, and any value the enclosing query produces for the current row — a column, an
  aggregate, an `OVER` window, or a sub-query. What it may *not* do is evaluate one of those
  row-level constructs over a lambda **parameter**:

  -  **Not expressible: aggregating over a lambda parameter.** An aggregate folds a set of *rows*,
    while a lambda parameter is an array element inside the expression of a single row, so no
    relation exists for it to fold over; `ARRAY_TRANSFORM(a, x -> SUM(x))` has no reading. Folding
    an array's own elements is a different operation with its own operator, `ARRAY_REDUCE(a, 0,
    (acc, x) -> acc + x)`. The same holds for an `OVER` window partitioned or ordered by a
    parameter, and for a sub-query correlated on one (`x -> x IN (SELECT …)`). All are rejected
    during validation: "Aggregate functions over a lambda parameter are not supported in the body of
    a lambda expression. …". SQL names the offending parameter; the Table API does not, because a
    Table API lambda parameter carries a generated name that would mean nothing to the reader. A
    parameter of a lambda nested *inside* the construct is bound within it and does not trigger
    this, so `x -> x + SUM(ARRAY_REDUCE(a, 0, (acc, y) -> acc + y))` is accepted.
  -  **Not expressible: table functions.** These produce rows, not a value a per-element expression
    could consume, and are rejected unconditionally: "Table functions are not supported in the body
    of a lambda expression".
  -  **Not expressible: asynchronous scalar functions.** An `AsyncScalarFunction` completes a future
    after its `eval` returns, whereas a lambda body is a per-element expression that must yield its
    value in place; the planner rule that extracts asynchronous calls out of a projection is one of
    the `RexShuttle`s that do not descend into a lambda on the pinned 1.41.0 (see
    [*Lambda captures → Lambda-body determinism*](#lambda-captures)), so such a call would reach
    code generation and fail to compile. Descent alone would not make it work — an async call cannot
    be hoisted out of a per-element body into the enclosing projection in the first place — so the
    rejection is unconditional in every Calcite version, whether or not the call
    references the lambda parameter: "Asynchronous scalar functions are not supported in the body of
    a lambda expression". This is the mirror image of the rule that asynchronous kinds may not
    *declare* a lambda argument, and leaves the supported composition untouched: a higher-order call
    may still be an argument of an asynchronous function or wrap its result.

  Everything else is supported, on both the SQL and the Table API surface (a sub-query has no Table
  API spelling). `ARRAY_TRANSFORM(a, x -> x + SUM(base)) … GROUP BY g` evaluates `SUM(base)` once
  per group and the body sees the result; the same holds for `SUM(base) OVER ()` and for a scalar
  sub-query, plain or correlated, and for all of these inside a nested lambda or inside a
  user-defined higher-order function's lambda. Nothing in the `FUNCTION` type, the runtime
  representation, or the plan is specific to this: the construct is resolved to a reference into the
  enclosing row — into the `Aggregate` output for an aggregate, into a projection below the call for
  an `OVER` window, which must be evaluated where the enclosing query can see it — and capture
  lifting then hoists that reference out of the lambda like any other capture. The two surfaces
  arrive there by different routes: SQL through the conversion of the lambda body in the enclosing
  conversion context, the Table API by hoisting the aggregate into the enclosing aggregation before
  the body is resolved. The resulting plan is the one the sub-query work-around produces by hand:

  ```sql
  SELECT ARRAY_TRANSFORM(a, x -> x + s) FROM (SELECT a, SUM(base) AS s FROM t GROUP BY a)
  ```

  Hoisting the aggregate out of the body does not exempt the rest of the expression from the
  ordinary grouping rules: `SELECT ARRAY_TRANSFORM(a, x -> x + SUM(base)) FROM t` without a
  `GROUP BY` is still rejected, because the aggregate is then global and `a` is not grouped — the
  same rejection as `SELECT a, SUM(base) FROM t`, and unrelated to the lambda.

  Engines differ here, and this places Flink at the permissive end. Trino excludes the whole class
  ("Subqueries are not supported: `x -> 2 + (SELECT 3)`. Aggregations are not supported: `x ->
  max(y)`"). Spark — verified on 3.5.3 — evaluates outer-column aggregates, `OVER` windows and
  (correlated) scalar sub-queries in a lambda body, as Flink now does; but for the parameter case it
  has no defined behaviour, reporting `transform(a, x -> SUM(x))` as a missing `GROUP BY` and, once
  one is added, failing at runtime with a `NullPointerException`. Flink rejects it cleanly instead.
  That Spark behaviour is a genuine bug rather than a missing feature — its analyzer accepts an
  aggregate over a lambda parameter and the executor then crashes on it — and is noted for reporting
  to the Spark project; the repro, the analyzer plan and the analysis are in
  [appendix A4](#a4-spark-bug-found-in-the-cross-engine-comparison). Nothing in this FLIP depends on
  it, and the comparison above holds either way.
-  **Function references are not lambdas.** A lambda argument must be written as a lambda:
  `ARRAY_TRANSFORM(a, x -> my_udf(x))`, not `ARRAY_TRANSFORM(a, my_udf)`. SQL has no function-valued
  expressions, so a bare function name at a lambda position is an ordinary identifier and is
  reported as an unresolved column. No engine with lambdas does otherwise: verified on Spark 3.5.3,
  a bare function name at a lambda position — user-defined or built-in, in `transform`, `filter` and
  `aggregate` alike — is reported as an unresolved column; Trino's grammar makes a lambda a distinct
  production requiring `->` (`identifier '->' expression`); and ksqlDB's goes further, making
  `lambdaFunction` (`identifier '=>' expression`) a separate production from `functionArgument`, so
  a lambda is not an expression there at all. Eta-expansion of a named function into a lambda would
  be a separate, additive feature: it needs no change to the `FUNCTION` type or to the runtime
  representation committed here, only a resolution step that rewrites the reference into a lambda.
-  **Field access on a deferred expression inside a lambda body.** `SqlDotOperator` requires a
  struct operand, and an expression whose type inference is deferred while a lambda parameter is
  unbound reports a plain `ANY` in that pass, so `ARRAY_TRANSFORM(a, x -> ARRAY[ROW(x,
  1)][1].EXPR$0)` fails with "Incompatible types". This is a property of deferral in general, not of
  the value constructors: `ARRAY_TRANSFORM(a, x -> IFNULL(ROW(x, 1), ROW(0, 0)).EXPR$0)` fails
  identically. Every non-`DOT` form works (`ARRAY[ROW(x, 1)]`, `MAP['k', ROW(x, 1)]`, `ARRAY[ROW(x,
  1)][1]`), as does field access on a `CAST`, whose return type is concrete (`CAST(ROW(x, 1) AS
  ROW<c1 INT, c2 INT>).c1`).
-  **`MULTISET[...]` is not usable in a lambda body** — but only because Flink does not support the
  multiset value constructor at all; `SELECT MULTISET[1, 2, 3]` fails to plan outside a lambda too.
-  **`ARRAY_REDUCE` accumulators are restricted to types the reducer body is assignable to**, i.e.
  the common type of the initial-value type and the body type must be the initial-value type itself.
  Only *top-level* nullability is ignored when comparing the two, so a constructed accumulator whose
  nested nullability differs from the body's is rejected: `ARRAY_REDUCE(a, ARRAY[0], (acc, x) ->
  ARRAY[x])` over an array with nullable elements fails, because the body is `ARRAY<INT>` while the
  accumulator is `ARRAY<INT NOT NULL>`; casting the initial value (`CAST(ARRAY[0] AS ARRAY<INT>)`)
  aligns them. Constructed accumulators as such do work (`ARRAY`, `MAP` and `ROW` accumulators are
  all supported). This is the operand checker's assignability rule, unrelated to lambdas as such.

## Compatibility, Deprecation, and Migration Plan

1. This change is additive for queries that do not use the eight new built-in names. It introduces
  no changes for existing queries that avoid those names, and nothing is deprecated. However,
  existing unqualified calls to a temporary catalog or catalog function whose name collides with one
  of the eight new built-ins may resolve differently after the upgrade: Flink resolves an
  unqualified function reference in the order *temporary system → built-in system → temporary
  catalog → catalog* (`FunctionCatalog#resolveAmbiguousFunctionReference`), so a new built-in system
  function takes precedence over a same-named catalog UDF and the call can bind to the built-in (or
  fail with a new signature-validation error if the arguments do not match).
  - 
    `ARRAY_TRANSFORM`/`ARRAY_FILTER`/`ARRAY_REDUCE`/`ARRAY_ZIP_WITH`/`MAP_FILTER`/`MAP_TRANSFORM_KEYS`/
    `MAP_TRANSFORM_VALUES`/`MAP_ZIP_WITH` become reserved built-in system function names.
  -  Migration options for a colliding user-defined function, each consistent with the resolution
    rules above (and pinned by resolver regression tests in `FunctionCatalogTest`):
    -  Rename the user-defined function to a non-colliding name.
    -  Call it through a fully qualified catalog identifier (`catalog.database.name`); a precise
      reference is resolved by `FunctionCatalog#resolvePreciseFunctionReference` (*temporary →
      catalog*) and never consults built-in system functions, so qualification bypasses the
      collision.
    -  Register a temporary system function with that name; temporary system functions have the
      highest resolution priority and therefore override the built-in for the session.
2. **The `leastRestrictive` guard narrowing is an every-query behaviour change, confined to one
  case.** It ships as the **opening commit of layer 1's pull request**, under layer 1's subtask
  rather than a JIRA of its own (see [*Delivery plan*](#delivery-plan)); it is called out here
  because it is the one layer-1 change that is *not* conditioned on a lambda being present, and a
  voter should see the full analysis in one place.
  `FlinkTypeFactory.leastRestrictive` decides whether Flink imposes its own RAW-type
  reasoning instead of deferring to Calcite. The guard changed from
  `t.getSqlTypeName() == SqlTypeName.ANY` to `t instanceof GenericRelDataType`. **The trigger is the
  unresolved lambda *parameter*, not the `FUNCTION` type.** A lambda parameter has no type during
  the first validation pass of the body, so Calcite gives it the plain `SqlTypeName.ANY` placeholder
  it uses for every not-yet-known type; the old guard saw that `ANY` and rejected the query with a
  RAW-type message before the parameter could be bound in the second pass. The `FUNCTION` type
  itself was never affected — `FunctionRelDataType` is `SqlTypeName.OTHER` and never matched the old
  guard at all.

  **The narrowing also aligns the guard with its own message.** The condition tested
  `SqlTypeName.ANY` while the message, and the intent, are about Flink's encapsulated RAW type
  (`GenericRelDataType`). Calcite's own contract is the opposite of the old guard:
  `SqlTypeFactoryImpl.leastRestrictiveSqlType` states *"if any of the inputs are ANY, the output is
  ANY"*, i.e. `ANY` is the top type and a least-restrictive computation over it succeeds. Flink
  overrode that for every `ANY`. **No existing query reaches the divergence, however.** Flink does
  create plain `ANY`s on master — `SqlFirstLastValueAggFunction` (`FIRST_VALUE`/`LAST_VALUE` operand
  inference), `SqlWindowTableFunction`, `SqlMLEvaluateTableFunction`,
  `SqlVectorSearchTableFunction`, `TypeInferenceOperandChecker#toParamType` for untyped table
  arguments, and `SqlItemOperator` for an `ANY`/`DYNAMIC_STAR` operand — but every one of them
  produces that `ANY` as *operand metadata* (`paramTypes`, the deprecated
  `getParameterTypes`/`getReturnType`, signature and named-argument handling), or derives it from an
  operand that is already `ANY`. None of them puts a plain `ANY` into a result type that reaches
  `leastRestrictive`, so the old guard's over-reach was latent rather than user-visible, and the
  measured blast radius below ([*Delivery plan → Blast radius*](#delivery-plan)) confirms it: no
  pre-existing test in `flink-table-planner` is sensitive to the guard. The narrowing is therefore
  presented as what it is — a correctness fix to a guard that never matched its own message, needed
  because lambdas are the first feature that can reach it — and **not** as a bug fix that stands on
  its own user-visible merits.

  `GenericRelDataType` is the only Flink-native producer of `SqlTypeName.ANY` — `RawRelDataType`,
  `FunctionRelDataType` and `BitmapRelDataType` are `SqlTypeName.OTHER`, `StructuredRelDataType` is
  `STRUCTURED` — so the change is **behaviour-preserving for every RAW type**, which a regression
  test pins. That premise is no longer asserted only in prose: `FlinkTypeFactoryTest`
  `#testEveryLogicalTypeRootIsAudited` and `#testOnlyGenericRawTypesMapToSqlTypeNameAny` convert a
  representative instance of **every** `LogicalTypeRoot` through
  `FlinkTypeFactory.createFieldTypeFromLogicalType` and fail the build if any of them — or any
  newly added root — maps to `SqlTypeName.ANY`. Today the set is empty; the modern `RawType` maps to
  `RawRelDataType` (`OTHER`), and the sole `ANY` producer is the legacy `TypeInformationRawType`.

  The change is not a no-op for a **Calcite-native basic `ANY`**. Before: the call raised *"Generic
  RAW types must have a common type information."*. After: it defers to Calcite's default
  `leastRestrictive`, which normally succeeds. A query that previously failed with that message may
  now plan and produce an `ANY`-typed column. No query that previously planned changes its result,
  and a native `ANY` meeting a RAW type still fails with the same message. Both directions are
  pinned by `FlinkTypeFactoryTest#testLeastRestrictiveDefersForCalciteNativeAny` and
  `#testLeastRestrictiveStillFailsWhenNativeAnyMeetsRawType`.

  **An unresolved dynamic parameter `?` is *not* affected**, contrary to what an earlier revision of
  this document stated. `SqlValidatorImpl` types a not-yet-inferred node with
  `typeFactory.createUnknownType()`, and Flink already overrides that to
  `UnknownSqlType extends BasicSqlType(SqlTypeName.NULL)` (an existing `FLINK MODIFICATION` in the
  vendored `SqlTypeFactoryImpl`). Dynamic parameters therefore never reached this guard before the
  FLIP and are unchanged by it; `FlinkTypeFactoryTest#testUnknownTypeIsNotSqlTypeNameAny` pins this.

  This change cannot be conditioned on a lambda being present, because at the point the guard runs
  there may be no lambda in scope; see [*Delivery plan → Blast radius*](#delivery-plan).
3. Adding `LogicalTypeVisitor.visit(FunctionType)` is a source-compatible default method. Adding the
  new `LogicalTypeRoot` was accompanied by the exhaustive-switch audit above
  ([*Proposed Changes → Type system*](#type-system-flink-table-common)): the type is handled on the
  lambda-planning path, rejected with a clear error at the five rejection choke points, and
  unreachable elsewhere behind throwing defaults. The new enum constant also appears additively as
  an allowed value in the SQL Gateway REST OpenAPI schema (the `LogicalTypeRoot` enumeration in
  `rest_v{1..4}_sql_gateway.yml` is regenerated) — a purely additive, wire-compatible change to the
  public REST spec.
  -  **The new enum constant is binary-additive but not source-compatible in one narrow case.**
    Already-compiled third-party code keeps working: a new constant does not change the signature or
    ordinal of any existing one, and code that reaches an unknown constant takes its `default`
    branch. On *recompilation*, however, a third-party `switch` **expression** over
    `LogicalTypeRoot` that enumerates every constant without a `default` becomes a compile error
    ("the switch expression does not cover all possible input values"), since exhaustiveness is
    checked at compile time. A `switch` *statement* is unaffected, as is any switch with a
    `default`. This is inherent to adding a constant to a `@PublicEvolving` enum — `LogicalTypeRoot`
    has grown this way before (`DESCRIPTOR`, `VARIANT`, `BITMAP`) — and the remedy is the one-line
    addition of a `default` branch, which such code needs in any case to stay forward-compatible. It
    is called out here so the cost is stated rather than implied.
4. Adding `CallContext#getLambdaArgument(pos)` is likewise a source-compatible default method: an
  existing implementation of the `@PublicEvolving` `CallContext` keeps compiling and answers "no
  lambda", which is correct for a call that has none. Every framework context that wraps another one
  overrides it to delegate instead (see
  [*Proposed Changes → Type system*](#type-system-flink-table-common)), so the default is only ever
  reached where there genuinely is no lambda.
5. `LocalReferenceExpression` is promoted from `@Internal` to `@PublicEvolving`, because the new
  `@PublicEvolving` `LambdaExpression` exposes it through `getParameters():
  List<LocalReferenceExpression>` (no `@PublicEvolving` API may leak an `@Internal` type). This is a
  source-compatible widening of the public API surface — no existing member is removed or changed —
  and is verified by the production architecture tests
  (`PUBLIC_EVOLVING_API_METHODS_USE_ONLY_PUBLIC_EVOLVING_API_TYPES`). What is committed is kept as
  small as the leak requires: the class remains framework-constructed (package-private constructor,
  and its only factory is in the `@Internal` `ApiExpressionUtils`), so the guarantee covers reading a
  parameter's `getName()` and its inherited `ResolvedExpression` members, not constructing one. For
  the same reason `LambdaExpression`'s own constructor is annotated `@Internal` — a user cannot
  supply its parameter list — following the precedent of the `@PublicEvolving` `CallExpression`,
  whose constructor and `permanent(...)` factory are `@Internal` for the same reason.
6. The `FUNCTION` type's nullability is not meaningful (a lambda is never a runtime value) and is
  canonicalized (to nullable) at the `FunctionType` boundary: `copy(boolean)` ignores the requested
  nullability, and `FunctionRelDataType` is always nullable and treats `createWithNullability` as a
  no-op. Every construction path (`DataTypes.FUNCTION(n)`, `.notNull()`, `copy(false)`, a parsed
  `FUNCTION(n) NOT NULL`) therefore yields the same logical type, the same serializable string, and
  the same `FunctionRelDataType` digest, so a nullable/non-null bit can never split two otherwise
  identical lambda arities during coercion or inference. The nullability of the lambda's parameter
  and result types remains meaningful; it lives on the `LambdaInfo` obtained from
  `CallContext#getLambdaArgument(pos)`, not on the `FUNCTION` type.
7. **Release notes.** Release-notes entries for the new SQL functions and Table API / PyFlink
  methods **will be added** on the corresponding JIRA tickets (the release manager consolidates them
  at release time). Because the work lands as four functional layers
  ([*Delivery plan*](#delivery-plan)), each entry will be filed on the subtask that makes its
  surface reachable — the subtasks themselves will be created once this FLIP is accepted — and the
  name reservation described above takes effect per function family, in the release that contains
  that layer: the four `ARRAY_*` names with layer 1, the four `MAP_*` names with layer 2.

## Implementation

This is one design, and it is completely implemented and tested on the
[prototype branch](https://github.com/swisscom-bigdata/flink/tree/flink-31207-higher-order-functions-and-lambdas).
The branch is **evidence, and it is also already in delivery shape**: it is cut into **four commits,
one per functional layer** ([*Delivery plan*](#delivery-plan) below), sitting on top of **one
small preparatory commit** — five commits in all. That commit is the narrowing of the generic RAW
guard in `FlinkTypeFactory.leastRestrictive` from `SqlTypeName.ANY` to `GenericRelDataType`: a
correctness fix to a guard whose condition never matched its message, which lambdas are the first
feature able to reach (see
[*Compatibility*](#compatibility-deprecation-and-migration-plan)). It is kept as its own commit
because it is the only every-query change in the feature that is not conditioned on a lambda, but it
carries no JIRA of its own — it opens layer 1's pull request under layer 1's subtask.
Together the commits show
that the design closes, that nothing in it is
hand-waved, and that the semantics specified above are the ones that actually come out. What is
proposed for **merge** is that same code, one pull request per layer — layer 1 as five stacked
sub-unit pull requests into a layer-1 branch ([*Delivery plan*](#delivery-plan)) — each of
which compiles, verifies and ships on its own. The unit of delivery is the layer, not the branch;
the cut changes nothing about the design, it fixes only the order in which the design lands and what
a user can do at each intermediate release.

The implementation comprises three functional areas, each building on the previous one — which is
also how the branch is best read, in three passes, and along which the delivery plan is organized.
The public `FUNCTION` type belongs to the first area (built-in functions), where it is first needed;
its full justification (Table API resolution and UDF type inference) is realized by the other two.
Because the eight built-ins are ordinary `BuiltInScalarFunction`s reached through the bridging layer,
the `LambdaInputTypeStrategy` extension point, the `RexLambda → Expression` bridge and the
function-object generation all belong to the *first* area too: they are what makes a built-in work,
and the third area adds only what is specific to a *user-defined* host.

-  **Lambdas, the `FUNCTION` type, and built-in array and map functions (SQL + compiled plan).** The
  logical type with its materialization guards, its `FunctionDataN` family
  conversion classes and the `FlinkTypeFactory`/`FunctionRelDataType` bridge; the eight functions as
  runtime classes in `flink-table-runtime`, each declared by a `BuiltInFunctionDefinitions` entry
  with a declarative input and output type strategy and validated by the bridging layer's
  `TypeInferenceOperandChecker`; the `LambdaInputTypeStrategy` contract, `CallContext.getLambdaArgument`,
  the `RexLambda → Expression` bridge and the generated function object those runtime classes are fed
  through; lambda-capture closure conversion, together with the SQL
  front-end pieces that make nested captures work — `FlinkSqlLambdaScope`, cumulative `RexLambdaRef`
  indices in the vendored `SqlValidatorImpl`/`SqlToRelConverter`, conversion of a lambda body in the
  enclosing conversion context, the lambda-body validation rules in `FlinkCalciteSqlValidator`, and
  the `FlinkPlannerImpl` driver shuttle that rebuilds a lambda with its lifted body;
  `ExtractLambdaOverWindowRule` with its batch and stream rule-set registration; and
  `RexLambda`/`RexLambdaRef` compiled-plan serde with `RestoreTestBase` coverage.
-  **Table API + Python.** `UnresolvedLambdaExpression` / `LambdaExpression`, expression-resolver
  support (including carrying enclosing lambda parameters and the enclosing over-windows into a
  nested lambda's scope), lambda-aware aggregate extraction and the Table API's lambda-body
  validation, the `ExpressionConverter` (`LambdaExpression → RexLambda`, with cumulative parameter
  indices so nested captures stay distinguishable for the shared `liftCaptures`), the eight DSL
  methods, and the corresponding PyFlink `array_*` / `map_*` bindings.
-  **User-defined higher-order functions.** Opening the mechanism the built-ins already use to
  user code: a lambda argument declared by a user-supplied `LambdaInputTypeStrategy` and received
  externally (as one of the `Function0`…`Function4` classes) rather than internally, over both the
  Table API and the SQL call path `SELECT my_udf(a, x -> ...)`, plus the `eval`-signature check that
  rejects a declaration and an `eval` method that disagree — the external form and that check are
  the same ones the built-ins already use, so they ship with layer 1 and this area only opens them
  to user code. `RestoreTestBase` coverage for a user-defined higher-order function
  with one and with two capturing lambdas. Also the function-kind scoping of
  [*Public Interfaces → 7*](#7-function-kinds-and-process-table-functions): the
  `SystemTypeInference` / `BridgingSqlAggFunction` check and the `StaticArgument` guard, and lambda
  arguments of process table functions — the static `FUNCTION(n)` declaration, the generated
  function object in the PTF runner, and the rejection of captures.

### Delivery plan

The prototype is roughly 20,600 added lines across 172 files, which is not a reviewable pull
request. It is
delivered as **four functional layers**, one JIRA subtask and one merge unit each under
[FLINK-31207](https://issues.apache.org/jira/browse/FLINK-31207) — one pull request per layer,
except that layer 1 is proposed as five stacked sub-unit pull requests into a layer-1 branch (see
*Why layer 1 cannot be smaller* below). They land behind one small preparatory commit, listed
below as step 0 because layer 1 depends on it; it belongs to layer 1's subtask and pull request and
carries no JIRA of its own (see *Step 0 in full*). The branch is cut into exactly that sequence:
**five commits**, one preparatory and four layers. A layer is not a slice of
plumbing: each one **compiles, is verified and is releasable on its own**, and each one ends with
strictly more that a user can do than the layer before it. The first three functional areas above
map onto layers 1+2, 3 and 4; the first area is split because array and map higher-order functions
are independently useful and independently testable, which halves the largest layer without
splitting anything that has to land together.

| # | Layer | What a user can do that they could not before | Design sections | main / test / docs |
|---|---|---|---|---|
| **0** | **Preparatory commit — part of layer 1, no JIRA of its own.** The narrowing of the generic RAW guard in `FlinkTypeFactory.leastRestrictive` to `GenericRelDataType`, with the audit that keeps it the sole Flink producer of `ANY`. Kept as a separate commit because it is layer 1's only every-query change that is not conditioned on a lambda; layer 1 needs it, since an unbound lambda parameter is a plain `ANY` | Nothing on its own — no existing query can reach the guard's over-reach | [Compatibility](#compatibility-deprecation-and-migration-plan), *Step 0 in full* below | 4 / 257 / 0 |
| **1** | **Lambdas and array higher-order functions in SQL.** The `FUNCTION` type and its materialization guards; the five-member `FunctionDataN` family; the `FlinkTypeFactory` / `FunctionRelDataType` bridge; the SQL front end (`FlinkSqlLambdaScope`, cumulative `RexLambdaRef` indices, lambda-body validation, decorrelation, `ExtractLambdaOverWindowRule`); capture lifting; the `LambdaInputTypeStrategy` contract, the `RexLambda → Expression` bridge and the generated function object (including its external `Function0`…`Function4` form and the arity validation that goes with it); `RexLambda` compiled-plan serde; and `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE`, `ARRAY_ZIP_WITH` as runtime classes | Write a lambda in SQL and apply the four array functions to it — with captures, nesting, `OVER`/sub-query bodies, compiled plans and savepoint restore | [Type system](#type-system-flink-table-common), [SQL planner](#sql-planner-flink-table-planner), [Lambda captures](#lambda-captures), [Compiled plan](#compiled-plan-flink-table-planner) | 5,294 / 4,676 / 288 |
| **2** | **Map higher-order functions in SQL.** `MAP_FILTER`, `MAP_TRANSFORM_KEYS`, `MAP_TRANSFORM_VALUES`, `MAP_ZIP_WITH`: four more runtime classes with their per-entry loops, the key wrapper, duplicate/`NULL` key rejection, and `MAP_ZIP_WITH`'s key generalization and union-of-keys iteration. No new mechanism — the same lambdas, lifting, bridging and serde as layer 1 | The four map functions in SQL; the SQL surface of this FLIP is then complete | [SQL planner](#sql-planner-flink-table-planner) | 791 / 1,897 / 56 |
| **3** | **Table API and Python.** `UnresolvedLambdaExpression` / `LambdaExpression`, expression-resolver support, lambda-aware aggregate extraction, `ExpressionConverter`, the eight DSL methods and the PyFlink `array_*` / `map_*` bindings. Also the second surface of what layers 1–2 asserted in SQL only: the Table API assertions of the two built-in IT cases, the SQL/Table API validation parity test, and the `table:` column of the function tables | Write the same eight calls in the Java Table API and in PyFlink | [Table API](#table-api-flink-table-api-java-flink-table-common), [Python](#python-flink-python) | 1,357 / 1,112 / 95 |
| **4** | **Lambda arguments for user-defined functions.** Opening layer 1's mechanism to user code: a user-supplied `LambdaInputTypeStrategy` and the function-kind guards (`StaticArgument`, `SystemTypeInference` / `BridgingSqlAggFunction`), plus lambda arguments of process table functions — their static declaration and the function object in the generated PTF runner. The external `Function0`…`Function4` representation and its `eval`-signature check ship with layer 1, which is why this layer is 381 production lines — most of them for process table functions — against 6,260 test lines: what it adds is mostly the removal of a restriction, and the evidence that user-supplied strategies really do reach every path the built-ins reach | Declare a lambda argument on their own scalar, table or process table function and call it from SQL and the Table API | [Public Interfaces → 5](#5-lambda-arguments-for-user-defined-functions), [Public Interfaces → 7](#7-function-kinds-and-process-table-functions), [User-defined functions](#user-defined-functions) | 381 / 6,260 / 591 |

The per-layer figures are **measured on the branch** as it is cut (each commit against its parent):
production lines are Java, Scala and Python under `src/main` and `pyflink`, test lines everything
under `src/test` and `pyflink/**/tests`, documentation everything under `docs/` and
`flink-python/docs`. For the feature as
a whole that is **7,819 production lines**, **14,078 test lines** and 1,017 lines of documentation.

The layer figures sum to 7,827 production, 14,202 test and 1,030 documentation lines — 8, 124 and
13 above the totals. The difference is not extra work, it is the same lines written twice: layers
1–2 carry **SQL-only** variants of `HigherOrderFunctionsITCase` and
`HigherOrderFunctionsBatchITCase` and SQL-only function-table entries, which layer 3 rewrites into
the dual-surface form once the Table API exists, and layer 4 rewrites a handful of layer 1's
production lines — the function-kind sentence of the lambda contract's Javadoc, the visibility of
two code-generation helpers, and the evaluator factory of the process table function runner — when
it admits process table functions, and the lambda-binding call of operand type inference, which it
moves onto the permuted call so that lambdas passed by name bind to the right parameter. That is the cost of rules 2 and 3: a layer
cannot assert a surface that it does not yet ship, and it must not ship one it does not assert.

**Step 0 in full.** `FlinkTypeFactory.leastRestrictive` keys its generic RAW guard off
`GenericRelDataType` instead of `SqlTypeName.ANY`. The old condition never matched its own message
— Flink's encapsulated RAW type is `GenericRelDataType`, and while every `GenericRelDataType` is
`SqlTypeName.ANY`, not every `ANY` is a `GenericRelDataType` — so the guard caught strictly more
than it meant to. The fix also ships the audit that keeps `GenericRelDataType` the sole Flink
producer of `ANY`, which is the premise the whole lambda front end rests on
(*The "conditioned on a lambda placeholder" claim* below).

**It is deliberately *not* filed as a standalone bug.** An earlier revision of this document
presented it as a bug fix that stands on its own and is backportable. That claim does not survive
scrutiny: as [*Compatibility* entry 2](#compatibility-deprecation-and-migration-plan) now records,
every plain `ANY` Flink produces on master is operand metadata or derived from an already-`ANY`
operand, and none of them reaches `leastRestrictive` in a result-type position. The over-reach is
latent, no query can trigger it, and the blast-radius measurement below shows no pre-existing test
in the module is sensitive to it. A separate JIRA would therefore buy nothing a separate JIRA is
for — there is no user-visible bug for a release note to describe, and a backport would carry a
behaviour change to a release branch with no benefit — while costing a second review cycle that
layer 1 would block on, spent arguing about a reproducer that does not exist.

It is still kept as **its own commit**, the first in layer 1's pull request and under layer 1's
subtask. That preserves what the split was actually worth — a one-line diff readable in isolation,
a clean `git bisect` point, and the ability to state that *every remaining* layer-1 change on an
every-query path is conditioned on a lambda placeholder or a `RexLambda`. Its compatibility note
belongs to FLINK-31207's release notes, which is where it is reachable from: the divergence only
becomes observable once lambdas exist. Its 261 added lines are included in the figures above.

**What every layer satisfies.**

1.   **It compiles and verifies on its own.** No layer needs a later one to be correct, and none of
    them is a partial refactor that a follow-up completes. On the branch as it stands, each layer
    commit has been verified by building main *and* test sources of the touched modules with
    Spotless and Checkstyle clean. Before a layer merges, its pull request additionally has to be
    green under `mvn clean verify` on the touched modules and on `flink-table` as a whole, with
    Spotless/Checkstyle/ArchUnit clean and the layer rebased on current `master` — a gate to pass,
    not a claim already discharged for the intermediate commits.
2.   **It is functionally complete.** A layer never leaves a half-wired surface: everything it makes
    reachable is validated, code-generated, plan-serializable, covered by batch *and* streaming IT
    cases asserting computed results, registered in the function catalog, documented in English and
    Chinese, and carries its release-note entry. There is no state in which a function parses but
    rejects a captured column, or works in SQL but is missing from `SHOW FUNCTIONS`, or lacks its
    restore test. Completeness is per *surface*: layers 1–2 assert and document the SQL surface,
    which is the only one they ship, and layer 3 extends the same IT cases and function tables to
    the Table API when it adds that surface.
3.   **It is releasable.** Every layer boundary is a valid release cut. Flink can ship after layer 1
    and users get array higher-order functions in SQL; layers 2, 3 and 4 are additive to it and to
    each other, and never revisit what an earlier layer made public.
4.   **It is a slice of the reviewed branch, not a rewrite.** The four layers sum to exactly the
    prototype — the tree at the last layer is byte-identical to the squashed feature commit — so
    each can be diffed against it and the design that was reviewed is the design that lands.
5.   **It is internally ordered for review.** The merge unit is the layer, and the branch carries
    one commit per layer in the order above, so a reviewer works through the change in four chunks
    rather than through one diff. Every commit is a buildable, self-consistent tree — each layer
    commit compiles main *and* test sources of the touched modules with Spotless and Checkstyle
    clean — so `git bisect` and per-commit review both work; they are not squashed on merge. A layer
    may be further broken into a series of self-contained commits or stacked pull requests when it
    is opened (see the layer-1 sub-units below); that breakdown belongs to its subtask, not to this
    FLIP.

**Why layer 1 cannot be smaller and still be a *releasable* layer, and how it is still cut for
review.** A lambda is unusable until the type, the front end, capture lifting, code generation and
plan serde are all present — each of those is inert in isolation, and a pull request that ships only
some of them adds public API surface (`DataTypes.FUNCTION`) that nothing can produce. Rule 2 is what
forbids *releasing* such a fragment, so layer 1 as a release unit carries the whole mechanism plus
the smallest function family that exercises it.

That is an argument about releases, not about review, and "reviewers can follow the commits" is not
an adequate answer at this size. The branch keeps layer 1 as a single commit, but for merge it is
proposed as **four named sub-units** preceded by the step-0 commit, each its
own pull request, merged in order into the layer-1 feature branch and released together — they
share layer 1's subtask and its release:

| Sub-unit | Contents | Reviewable on its own because |
|---|---|---|
| **1a — Guard narrowing** (step 0) | `FlinkTypeFactory.leastRestrictive` keyed off `GenericRelDataType`, with the `LogicalTypeRoot` audit that keeps it the sole producer of `SqlTypeName.ANY` | A four-line change in one method, pinned by unit tests that call `leastRestrictive` directly; depends on nothing and is the only piece not conditioned on a lambda |
| **1b — Type** | `LogicalTypeRoot.FUNCTION`, `FunctionType`, `DataTypes.FUNCTION`, the `FunctionDataN` family, `LogicalTypeParser`/`asSerializableString` round-trip, the materialization guards and the exhaustive-`switch` audit | Self-contained in `flink-table-common`; its whole contract is round-tripping and rejection, both unit-testable with no planner involved |
| **1c — SQL front end** | `FlinkSqlLambdaScope`, cumulative `RexLambdaRef` indices, lambda-body validation, the `FlinkTypeFactory`/`FunctionRelDataType` bridge, decorrelation, `ExtractLambdaOverWindowRule` | Depends on 1a–1b; reviewable as "does a lambda validate and convert to a correct `RexNode`", with plan tests and no runtime |
| **1d — Capture lifting and code generation** | Capture lifting, the `LambdaInputTypeStrategy` contract, the `RexLambda → Expression` bridge, the generated function object | Depends on 1c; reviewable as "does a validated `RexLambda` become a correct executable object" |
| **1e — Array functions** | `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE`, `ARRAY_ZIP_WITH` and `RexLambda` compiled-plan serde | Depends on 1d; four runtime classes plus serde, reviewable per function against the semantics table |

The boundaries are the ones the design already has, so the split costs nothing beyond five pull
requests instead of one. What it does **not** do is make each sub-unit independently *releasable* —
1a–1d are inert without 1e, which is why they share one layer and one release. If a maintainer
prefers a single layer-1 pull request, the fallback is the coarser two-way cut (inert mechanism,
then functions) at the same boundaries, with step 0 still its own commit; that is a
review-convenience decision, not a design one.

**Blast radius, and why no feature flag.** Layer 1 touches three paths that every query goes
through: the SQL front end and decorrelation, the `ARRAY[...]` / `MAP[...]` / `CAST` type
derivation, and determinism analysis. `RexUtil.isDeterministic` and its two Flink-side companions
are the third — they were missing from an earlier revision of this list, not subsumed by the other
two. `SqlCastFunction`'s one-line change is part of the second path (`CAST` type derivation), and
`ExtractLambdaOverWindowRule` is not an every-query path at all: it is a new rule whose matching
predicate requires a `RexLambda`, so it never fires on a lambda-free plan.

Every change on those paths is conditioned on the presence of an unresolved lambda placeholder or of
a `RexLambda`, so a query without lambdas takes the pre-existing behaviour by construction and the
new machinery is unreachable.

A fourth every-query path, `leastRestrictive`, is **the one exception**, and it is the reason step 0
is kept as its own commit rather than folded into 1b–1e:

-  **`leastRestrictive` guard narrowing** (`FlinkTypeFactory`). The guard that decides whether Flink
   imposes its own RAW-type reasoning was narrowed from `SqlTypeName.ANY` to `GenericRelDataType`.
   It is behaviour-preserving for every RAW type, but *not* for a Calcite-native basic `ANY`, which
   previously threw and now defers to Calcite. That is an every-query behaviour change; it is
   specified in full, with its test pins, in
   [*Compatibility* entry 2](#compatibility-deprecation-and-migration-plan) and is not restated
   here. It is
   intrinsic to the lambda work — an unresolved lambda *parameter* is Calcite's plain `ANY`
   placeholder during the first validation pass of the body, and the old guard rejected it as a RAW
   type before it could be bound — and it cannot be conditioned on a lambda being present, because
   at the point the guard runs there may be no lambda in scope yet.

   Two qualifications narrow the exception almost to nothing. First, a dynamic parameter `?` is
   typed `SqlTypeName.NULL`, not `ANY` (see *Compatibility* entry 2), so the commonest suspected
   trigger is not one. Second, and decisively: although Flink does create plain `ANY`s on master
   (`FIRST_VALUE`/`LAST_VALUE` operand inference, window TVFs, ML TVFs, untyped table arguments, the
   item operator), each of those is operand metadata or is derived from an operand that is already
   `ANY` — none of them puts a plain `ANY` into a result type that reaches `leastRestrictive`. The
   divergence is therefore unreachable for any query that exists today, which is why the narrowing
   is *not* claimed as a standalone bug fix and does not carry its own JIRA
   (see *Step 0 in full* above).

   **Measured blast radius.** The claim that the narrowing is invisible to existing queries was
   checked empirically rather than argued: the pre-FLIP guard was restored on the prototype branch
   and the `flink-table-planner` test corpus was re-run.

   | Suite | Tests run | Failures | Where |
   | --- | ---: | ---: | --- |
   | Unit tests (`default-test`) | 11,428 | **1** | `FlinkTypeFactoryTest#testLeastRestrictiveDefersForCalciteNativeAny` — the test that pins this very change |
   | `*ITCase` (208 of 226 classes reported; run aborted by an unrelated fork OOM) | 10,584 | **30** | all four lambda ITCases only: `HigherOrderFunctionsITCase`, `HigherOrderFunctionsBatchITCase`, `UserDefinedHigherOrderFunctionITCase`, `UserImplementedLambdaInputTypeStrategyITCase` |

   No pre-existing, non-lambda test in the module is sensitive to the guard. The lambda suites fail
   without it — with an internal *"Unexpected error in type inference logic of function
   'ARRAY_TRANSFORM'. This is a bug."*, not a clean validation error — which also establishes that
   the narrowing is **necessary**, not merely convenient. That is the containment evidence a
   reviewer should ask for, and it is reproducible by reverting one line.

**The "conditioned on a lambda placeholder" claim rests on one predicate, used in five places.**
`SqlValidatorUtils.isUnresolvedLambdaParameter` is exactly
`type.getSqlTypeName() == SqlTypeName.ANY && !(type instanceof GenericRelDataType)`. It is not
called directly from outside its own class: every gate goes through the recursive wrapper
`SqlValidatorUtils.containsUnresolvedLambdaParameter`, which also matches a placeholder nested in a
collection or row type, and there are exactly five such call sites — `SqlCastFunction`,
`TypeInferenceOperandChecker` (two sites), `SqlMapConstructor` and `SqlArrayConstructor`. So "plain
`ANY` means unbound lambda parameter" is a branch-wide convention, not a single guard, and the audit
that keeps it sound is the same one:
`GenericRelDataType` must remain Flink's only producer of `SqlTypeName.ANY`. That premise is pinned
by `FlinkTypeFactoryTest#testEveryLogicalTypeRootIsAudited` and
`#testOnlyGenericRawTypesMapToSqlTypeNameAny`, which fail the build if a new logical type ever maps
to `ANY`. A reviewer should treat that audit, not the individual call sites, as the thing to check.

`leastRestrictive` is **not** one of the five, and it is worth being precise about why the same
convention nonetheless covers it. Its guard is the logical complement:
`types.stream().anyMatch(t -> t instanceof GenericRelDataType)`, which fires *on* `GenericRelDataType`
and throws, whereas the lambda predicate fires on everything `ANY` *except* `GenericRelDataType`.
The narrowed `leastRestrictive` therefore never names a lambda placeholder; it simply stops throwing
for the plain `ANY` that is one, and defers to Calcite's default. The two guards partition
`SqlTypeName.ANY` between them, so they rest on the same premise and are invalidated by the same
audit — but the count of sites that consult the predicate is five, not six.

That the guard is isolated in its own commit is also why a configuration kill switch is not proposed
for layer 1: apart from step 0, every layer-1 change on an every-query path is
conditioned on a lambda placeholder or a `RexLambda`, so not using the functions *is* the off
switch. Nor is a switch proposed for the guard itself: it is a one-line change whose
divergence is confined to a case that previously threw and that no existing query can reach, gating
it would mean keeping the old
exception reachable, and the measurement above shows the pre-existing corpus cannot tell the
difference. A flag would add a second, less-tested path through the same validator. The existing
planner and IT suites are the regression evidence for that claim, and every layer's pull request
has to be green on them.

**How to review.** The design is this document, and it is complete; the prototype branch answers
*does the design close*; the layers answer *is each step mergeable and shippable*. The
*Design sections* column maps between them, so a reviewer of layer *n* can read the normative text
for exactly that layer.

## Test Plan

Everything below is executed and asserted on computed results rather than on plans, and — except
where a construct has no Table API spelling — over both surfaces. The built-in *execution*
semantics run in batch *and* in streaming (`HigherOrderFunctionsBatchITCase` and
`HigherOrderFunctionsITCase` cover the same eight functions in the two modes: `NULL` and empty
collections, type changes, non-capturing and capturing nesting, shadowing, and map-key equality).
The mode-independent areas — aggregates, `OVER` windows, sub-queries and correlation in a body,
views and materialized tables, `FUNCTION`-type rejection, non-determinism and body code-splitting —
are asserted once, in batch, and have no streaming counterpart by design.
The user-defined-function cases run in batch, which is
the mode both share, except where the construct itself is streaming-only — an asynchronous function
or a process table function — or where a case exists specifically to pin that the streaming
translation agrees with the batch one; those are written in streaming and marked as such.
**This section is the test plan, and it is complete: every commitment is stated here.**
[Appendix A1](#a1-test-plan-full) is a non-normative expansion of exactly these commitments — it
enumerates the individual edge cases behind each group below, which is the evidence of design
diligence, but it introduces no commitment that is not already made here. Every layer of the
[*Delivery plan*](#delivery-plan) carries the part of this plan that covers the surface it makes
reachable, so no surface is ever merged ahead of its tests.

-  **Built-in semantics.** SQL IT cases for all eight functions over columns and literals: `NULL`
  and empty arrays/maps, `NULL` elements and values, element/accumulator/key/value type changes, and
  non-capturing nesting. Map-specific: a duplicate or `NULL` key produced by `MAP_TRANSFORM_KEYS` is
  rejected, and `MAP_ZIP_WITH` is covered over all three whole-map key-set shapes (disjoint,
  partially overlapping, identical), with a `NULL` input map, and over key types that are merely
  compatible (accepted, merged under the common key type) versus without a common type (rejected) —
  on the Table API surface too, since it validates through the input type strategy rather than the
  operand type checker. `ARRAY_REDUCE` correspondingly covers a reducer body assignable to but not
  identical with the accumulator, asserting the body is converted and the result keeps the
  accumulator type.
-  **Captures and nesting.** Bodies closing over one or more outer columns for each built-in and for
  a user-defined higher-order function; inner lambdas closing over an *enclosing* lambda's
  parameter, including a two-level capture threaded through an intermediate lambda, a nested capture
  combined with an outer-column capture, and name shadowing; alternating user-defined and built-in
  hosts four levels deep, where the innermost body captures every enclosing parameter and an outer
  column.
-  **Field access on a row-typed parameter.** `r.n` and `r.nested.v` on a `ROW` element, also from
  a nested body and under `GROUP BY`; a parameter that shadows a table alias of the same name
  (`ARRAY_TRANSFORM(r.rs, r -> r.amount) FROM t AS r` reads the element's field, not the table's
  column of a different type); and the rejections of an unknown field and of an aggregate over a
  parameter field.
-  **Correlation across a sub-query boundary.** A higher-order call inside a scalar sub-query whose
  lambda closes over a column of the *enclosing* query, executed end-to-end — covering a nested
  lambda, a body mixing a local and a correlated capture, and two distinct correlated columns (which
  exercises the lambda-aware correlation de-duplication), with controls that must be unaffected.
-  **Validation and error messages.** A lambda outside a higher-order function; `FUNCTION` rejected
  as a column type (directly, nested, metadata, computed, from a catalog, through CTAS), as a state
  type and as a return type; every cast to or from `FUNCTION`; duplicate lambda parameter names; a
  parameter name colliding with the reserved `cap$` prefix. The user-visible signature errors are
  pinned for all eight built-ins and, for user-defined functions, for a wrong lambda arity, a lambda
  at a position that declares none, a non-lambda where one is declared, an underivable parameter
  type, a wrong argument count (whose signature error must take precedence), and an unsupported
  lambda arity.
-  **Row-level constructs in a body.** Aggregates over outer columns under `GROUP BY` (both key
  orders), `COUNT(*)`, a user-defined aggregate, `OVER` windows with and without a partition, and
  scalar sub-queries plain and correlated — each also inside a nested lambda, and each repeated on
  the Table API. Rejected: an aggregate, `OVER` window or sub-query over a lambda **parameter**,
  with an aggregate over a *nested* lambda's parameters as the counter-case that must be accepted;
  plus a guard that hoisting the aggregate does not exempt the query from the ordinary grouping
  rules.
-  **Lambda object contract**
  ([*Public Interfaces → 5*](#5-lambda-arguments-for-user-defined-functions)) — a test for each of
  these normative rules observable from a UDF: lazy body evaluation, a fresh object per evaluation
  of the
  enclosing call, repeated and reordered application over one capture snapshot, unchecked body
  failures reaching `apply` unchanged, and a function called *from* a body being opened and closed
  exactly once. The checked-exception wrapping into `FlinkRuntimeException` and the unchanged
  propagation of an `Error` are specified in §5 but are not separately asserted; the failure tests
  use an unchecked `RuntimeException`. How each is asserted is in
  [appendix A1](#a1-test-plan-full).
-  **Multiple lambdas per call.** Two lambda arguments applied independently, each capturing a
  distinct outer column, and the same call nested inside a built-in lambda with one lambda capturing
  the enclosing parameter and the other an outer column. The test function combines its two lambda
  results **non-commutatively**, so misattributing a lifted capture operand changes the asserted
  result.
-  **Further user-defined shapes**: a `NULL` array argument and a `NULL` element; a lambda argument
  *followed* by a regular one; sibling calls reusing a parameter name; a UDF call directly inside
  another's body; a UDF lambda inside a built-in *map* body capturing both key and value; a
  UDF-supplied parameter-type derivation; a capturing lambda in streaming; and a higher-order
  call in a filter condition and as a grouping key.
-  **The public `LambdaInputTypeStrategy` extension point.** A hand-written strategy (no
  `@Internal` API involved) that widens the lambda parameter to a type no structural derivation
  would produce, exercised through **both** SQL and the Table API on the same function: same result type,
  same result value, and the same rejection when the strategy derives no type. A hand-written
  strategy that derives **zero** and one that derives **four** parameter types are rejected on both
  surfaces with the same derived-arity validation error — a syntactically valid one-parameter
  lambda is used in both cases, so only the strategy's return value is at fault, and the failure is
  a `ValidationException` rather than a conversion, indexing, or code-generation failure. The
  internal refinement hook is covered separately by a strategy that refines to the wrong arity,
  which must produce the invalid-strategy validation error rather than an indexing failure.
-  **Function kinds** ([*Public Interfaces → 7*](#7-function-kinds-and-process-table-functions)): a
  user-defined higher-order **table** function executed through `LATERAL TABLE(...)`; a
  user-defined higher-order **process table** function executed end-to-end (see below); the
  declaration rejections for aggregate and both asynchronous kinds, plus the misdeclared
  `FUNCTION`-typed `StaticArgument`s; and the converse rejection of an asynchronous or table function
  call *inside* a body — each with the composition that must still work as the counter-case. PTF
  composition is covered in all three supported positions.
-  **Views and materialized tables** — the two objects that persist the *expanded* query and
  re-parse it on every read, so the expansion must leave a lambda parameter unqualified while
  qualifying captured columns, tables and catalog function names. Each case asserts the persisted
  query *and* the
  rows returned, including parameter/column shadowing, nested shadowing, calls outside the
  projection, stacked views, a user-defined host, and a `CONTINUOUS` materialized table refreshed
  through the SQL gateway.
-  **Value constructors and mutable results.** A column per shape that the deferral of `ARRAY[...]`,
  `MAP[...]` and `CAST` must cover, plus the guarantee of
  [*Public Interfaces → 3*](#3-built-in-higher-order-functions): `ARRAY[...]` and `ROW(...)` bodies
  yield element-wise distinct values, not *N* references to the last one.
-  **Type system and type inference.** `FunctionType` round-trips through `LogicalTypeParser`;
  conversion class per arity and none outside it; the RAW common-type regression guard; nullability
  canonicalization across every construction path; the parameter type strategies and their failure
  modes; and a wrapping `CallContext` reporting the lambda of the argument it was asked about.
-  **Function resolution** (`FunctionCatalogTest`) pins the compatibility behaviour of the eight
  reserved names: built-in precedence, bypass by full qualification, override by a temporary system
  function.
-  **Compiled plan.** Restore tests for the eight functions, a capturing lambda, a user-defined
  host, and a two-lambda call whose per-lambda capture partitioning must survive the round-trip;
  `RexNode` serde with and without a lifted capture; and the same programs registered for
  `BatchExecCalc` to establish batch/stream baselines.
-  **Closed-lambda invariant.** `HigherOrderFunctionUtil.checkClosed` at the single capture-lifting
  choke point, with unit tests for the positive case and each rejection, and re-checked on read-back
  from a compiled plan (out-of-range reference, non-0-based list, name and type mismatch).
-  **SQL↔Table-API parity harness** — a single table-driven test in which every built-in
  argument-rejection case carries an equivalent SQL string and Table API expression and both
  surfaces must reject it at validation. This is the enforced invariant behind
  [*Table API*](#table-api-flink-table-api-java-flink-table-common)'s "stated on both paths": a new
  function or argument rule adds a matching negative case here. It deliberately includes the type
  pairs on which Calcite's `leastRestrictive` and Flink's `findCommonType` disagree, since those are
  the ones a surface can only get wrong silently. Both surfaces now reach the *same*
  `InputTypeStrategy`, but along different routes — SQL through Calcite's validator, the Table API
  through expression resolution — and each derives the lambda's parameter types itself, so a rule
  can still hold on one and not the other. The harness no longer needs to pin expected-signature
  text separately: a built-in's signature is rendered from its declared type inference on both
  surfaces, so there is one string and not two to keep in agreement.
-  **Completeness, derived from the definitions rather than from a list.** Four checks make an
  omission in a *new* higher-order built-in fail a build rather than ship. Each enumerates the
  higher-order built-ins by asking every `BuiltInFunctionDefinition` whether its input type strategy
  declares a lambda argument, so none of them can go stale. `BuiltInFunctionDefinitionsTest`
  requires a runtime class; `HigherOrderFunctionCoverageTest` requires **both** an execution case in
  the semantics IT case *and* an argument-rejection case in the parity harness, asserted
  independently so that satisfying one cannot mask the other; `BaseExpressionsLambdaCompletenessTest`
  requires the typed Table API method; and `SqlFunctionsDocsCompletenessITCase` requires the
  documentation entries.
-  **Determinism.** A non-deterministic body makes the enclosing call non-deterministic and a
  deterministic one does not — covering all three entry points (the vendored `RexUtil` patch,
  `ShortcutUtils.isDeterministicThroughProgram` including through a `RexLocalRef`, and
  `FlinkRexUtil.getNonDeterministicCallName`) and each where it is consumed.
-  **PyFlink end-to-end**: unary/binary/ternary adapters, an outer-column capture, a nested lambda
  capturing the enclosing parameter, `MAP_ZIP_WITH` over disjoint keys, and a validation failure —
  executed and asserted on collected results, not only on the rendered expression.
-  **Large lambda bodies stay splittable**: a body far beyond `table.generated-code.max-length` is
  executed for all eight functions, with the same expression outside a lambda as the control. A
  lambda body is compiled into a generated class of its own, which is subject to the JVM's 64 KB
  per-method limit like any other, so a body that `flink-table-code-splitter` cannot reach fails
  with `Code grows beyond 64 KB` and the misleading generic "this is a bug" diagnostic — where the
  same expression written outside a lambda compiles fine.

## Documentation

User-facing documentation ships with the implementation, in English and Chinese — and, per rule 2 of
the [*Delivery plan*](#delivery-plan), in the same layer as the surface it documents rather than in
a trailing documentation commit:

-  A **Higher-Order Functions** section in the built-in function reference
  (`docs/content{,.zh}/docs/sql/functions/built-in-functions.md` plus the eight entries in
  `docs/data/sql_functions{,_zh}.yml`): lambda syntax, captures and nesting, what a body may be
  evaluated over (an aggregate, `OVER` window or sub-query of the enclosing query — but not over a
  lambda parameter), the restriction that a lambda is only valid as an argument of a higher-order
  function, per-function signatures, `NULL` behavior, the map key constraints, and Table API /
  PyFlink examples.
-  A **Lambda Arguments** section in the user-defined function documentation
  (`docs/content{,.zh}/docs/dev/table/functions/udfs.md`): declaring a lambda argument with
  a `LambdaInputTypeStrategy`, deriving the result type via `CallContext#getLambdaArgument`,
  the `Function0`…`Function4` runtime objects, multiple lambda arguments, and the
  normative **lambda object contract** of
  [*Public Interfaces → 5*](#5-lambda-arguments-for-user-defined-functions) in full. The same
  contract is stated on the API itself, in the JavaDoc of `LambdaInputTypeStrategy`,
  `CallContext#getLambdaArgument` and `LambdaInfo`, so it is reachable from where a function author
  declares the argument.
-  A **`FUNCTION`** entry in the data type reference
  (`docs/content{,.zh}/docs/sql/reference/data-types.md`), documenting it as a helper type that
  cannot be a column, state, or return type.
-  The new PyFlink expression methods in `flink-python/docs/reference/pyflink.table/expressions.rst`
  (their Sphinx docstrings ship with the methods).

## Rejected Alternatives

-  **Deferring layer 4 — UDF lambda arguments — to a follow-up FLIP.** Layers 1–3 justify the public
  `FUNCTION` type on their own (`ResolvedExpression#getOutputDataType` must be non-null, and
  `CallContext#getArgumentDataTypes()` must be able to describe a lambda operand), so a smaller FLIP
  covering only the built-ins and the two API surfaces would be coherent. It is rejected because
  layer 4 is not additional *machinery*, it is the removal of an artificial restriction on machinery
  layers 1–3 already contain. A user-defined higher-order function reuses the same lambda type, the
  same cumulative `RexLambdaRef` indexing, the same capture lifting, the same closed-lambda
  invariant, the same decorrelation handling and the same compiled-plan serde as a built-in; what it
  adds is permission: the guards that let a *user-supplied* `LambdaInputTypeStrategy` declare a
  lambda argument, on the function kinds that can take one, plus the call from the process table
  function runner into the same function-object generator. The type-inference vocabulary and the
  object handed to `eval` are already there. Measured on the branch, layer 4 is 381 production
  lines — most of them for process table functions — against 6,260 test lines
  ([*Delivery plan*](#delivery-plan)). Splitting it out would mean designing the built-in path first
  and then discovering, in a
  second FLIP, whether the type inference and capture machinery generalize — and if they did not,
  the first FLIP's public `FUNCTION` type would already be frozen around the wrong shape. Bundling
  is what lets this FLIP claim that the type system commitment is the right one, because it is
  demonstrated against both a closed set of built-ins and an open set of user functions. The cost of
  bundling is a larger vote, not a larger merge: layer 4 is a separate subtask and a separate pull
  request, is last in the order, and is the only layer whose removal would leave everything before
  it intact — so a reviewer who disagrees can still take layers 1–3 and re-open layer 4 on its own,
  without the earlier layers having committed to anything layer 4 would need to change.

-  **Keeping the `FUNCTION` type planner-internal (no public type).** Viable only for a SQL-only
  build; a prototype confirms it (≈6 files, no public type). Two things this is explicitly *not*
  rejected for, since both would be false: the planner's own `RelDataType` for a lambda operand
  (`FunctionRelDataType`) **is** `@Internal` in this design already, and compiled plans do **not**
  serialize a lambda type at all — they serialize the `RexLambda` and recompute its type on
  deserialization (see [*Design overview*](#design-overview)). So `RexLambda` plus recomputed types
  does work with a purely internal type, for as long as the lambda never leaves the planner. That is
  exactly the SQL-only case. It is rejected because two boundaries a lambda must cross lie outside
  the planner, in modules that do not (and must not) depend on `flink-table-planner`: Table API
  resolution and user-defined function type inference, both spelled out in
  [*Design overview*](#design-overview). An internal planner type is unreachable from either — a
  resolved `LambdaExpression` cannot name a planner class, and since `LambdaExpression` is itself
  `@PublicEvolving`, whatever type it returns is public API regardless. Designing the type once,
  publicly, avoids shipping an internal type and later reworking it into a public one. The
  counter-argument — that adding a `LogicalTypeRoot` is nearly irreversible and touches every
  exhaustive `switch` — is real and is the reason for the exhaustive-switch audit above
  ([*Proposed Changes → Type system*](#type-system-flink-table-common)): the new root is either
  handled, rejected with a clear message at one of the five rejection choke points, or unreachable
  behind a throwing default at every site, so it cannot silently produce a wrong result.

  **What the public type costs, stated plainly.** A rejection is only convincing if it names the
  price of the option that was chosen, so: the planner-internal alternative would have committed
  Flink to nothing outside `flink-table-planner`, while the public type commits it to (a) a new
  `LogicalTypeRoot` constant, which is exhaustively switched over across the whole table stack and
  is effectively irreversible once released; (b) a serializable string form `FUNCTION(n)` in
  `LogicalTypeParser` and `LogicalType#asSerializableString`, i.e. a parser and serializer surface
  that must keep round-tripping for every future release; (c) a `LogicalTypeVisitor#visit` default
  method that every existing visitor implementation inherits; and (d) the ordinary
  `@PublicEvolving` obligations on `FunctionType`, `DataTypes.FUNCTION(int)` and the
  `FunctionDataN` family — obligations that are not free, and that are precisely why the constructor
  validates its arity and why the arity-to-conversion-class mapping is kept out of the type's
  public surface rather than exposed on it. The judgement is that these are worth paying *once*,
  now, rather than shipping an internal type and paying a superset of them later during a public
  rework — but they are the cost, and they are permanent in a way the internal type would not have
  been.
-  **An explicit `isCapture` marker on `RexLambdaRef` instead of a sentinel index base.** Lifted
  captures are numbered from `HigherOrderFunctionUtil.CAPTURE_PARAM_INDEX_BASE = 1_000_000` and
  named `cap$N`; both are **plan-visible** — they appear verbatim in the committed restore plans
  (`calc-array-transform-capture.json`, `calc-udf-two-lambdas-capture.json`) — and are therefore a
  permanent part of the compiled-plan contract, not an implementation detail. The obvious
  alternative is a `boolean isCapture` field on the reference, which reads better and needs no magic
  number. It is rejected because `RexLambdaRef#equals` compares only *index and type*: a parameter's
  name is deliberately not part of its identity, since two lambdas differing only in parameter names
  denote the same function, and Calcite relies on that to deduplicate lambdas when building a
  `RexProgram`. A flag outside the identity would let a capturing lambda be merged with a
  structurally identical non-capturing one; putting the flag *into* the identity would mean changing
  `RexLambdaRef`'s equality contract in a vendored Calcite class, which is exactly the kind of
  divergence [*A6*](#a6-vendoring-cost) exists to minimise. Encoding the distinction in the index
  keeps it inside the existing identity with no Calcite change at all, and the base is unreachable
  from below because a lambda can declare at most four parameters
  ([*2. The `FUNCTION` type*](#2-the-function-type)). The cost is that `1_000_000` and `cap$` are
  now frozen in the plan format: they can be read by old and new versions alike, but they cannot be
  renamed without a plan migration.

-  **Encoding lambdas as strings** — passing the body as a string literal parsed by the function,
  e.g. `ARRAY_TRANSFORM(a, 'x -> x + 1')`, the shape raised by Timo Walther in the 2018 discussion
  of this feature as a way to avoid touching the parser. Calcite 1.34+ parses `x -> expr` natively,
  so the original motivation — parser divergence — has largely evaporated, but the alternative is
  worth closing on the record because its costs are independent of the parser: a string body is
  opaque to validation (no type checking, no column resolution, no determinism analysis), opaque to
  the optimizer (no expression reduction, no capture lifting, no sub-query decorrelation), invisible
  in `EXPLAIN`, and impossible to write in the Table API or PyFlink without duplicating the SQL
  parser in those surfaces. It would also make the body's dialect a permanent compatibility
  surface. Every guarantee this FLIP makes about lambda bodies — that they are ordinary scalar
  expressions, validated and optimized exactly like any other — depends on the body being a
  `RexNode`, not a string.

-  **Upgrading Calcite, or upstreaming the fixes first, instead of carrying the modifications.**
  Seventeen of the changes in this proposal live in vendored Calcite classes
  ([*A6*](#a6-vendoring-cost)), and the principled response to that is to fix Calcite rather than
  Flink's copy of it. It is rejected as a *precondition* — not as a direction. Nine of the 17
  blocks are already scheduled for deletion on upgrades in flight (3 at 1.42.0, 6 at 1.43.0), which
  is the upstream path working as intended; the remaining eight are either Flink-specific (deferred
  operand binding, Flink's own `findCommonType`) or sit in private methods and private static
  nested classes with no upstream-visible extension point, so an upstream patch would have to
  *introduce* those extension points first. Making this FLIP wait on a Calcite release cycle, a
  Flink-wide Calcite upgrade, and an upstream API design would couple a Flink feature to a third
  party's roadmap for an unbounded period, while the marginal cost being avoided is 17 blocks in
  five files that Flink already vendors and already rebases. The commitment made instead is
  narrower and checkable: each block records the Calcite issue that removes it, and
  [*A5*](#a5-why-the-calcite-changes-cannot-be-applied-through-hooks-or-subclasses) records, change
  by change, why no hook or subclass would do. Whether to accept this debt is ultimately a
  maintainer decision; this entry states the case rather than settling it.

-  **Modelling a lambda as an argument *kind* rather than an argument *type*** — i.e. a
  `StaticArgumentTrait.LAMBDA` plus a semantics accessor, with no `LogicalTypeRoot` at all, the way
  `TABLE` and `MODEL` arguments are modelled (`StaticArgument.model(...)` carries no `DataType`).
  This would remove the new root entirely, and was rejected because a lambda is not that kind of
  argument. The line Flink draws is whether the argument reduces to a *value in a scalar operand
  slot* that is handed to the function: `TABLE` becomes a relational input and `MODEL` a catalog
  reference — neither is ever an operand value, hence no type — whereas `SYMBOL` and `DESCRIPTOR`
  are operand values passed to `eval()` and therefore do have type roots. A lambda is squarely in
  the second group: a `RexLambda` *is* a `RexNode` operand, and a UDF receives a real
  `Function0`…`Function4` object in `eval()`, exactly as a PTF receives a `ColumnList`
  for a `DESCRIPTOR` argument. Modelling it as a kind would also mean the framework could not derive
  the `eval()` parameter class from the argument's conversion class the way it does for every other
  argument.
-  **A fully parameterized `FUNCTION` type** carrying the lambda's parameter types and result type
  (`FunctionType(argumentTypes, resultType)`, serialized as `FUNCTION<(t0, ...) -> tr>`). This was
  the original design and is rejected in favour of the parameter-count-only type. Nothing consumed
  the parameter types at all, and the result type was consumed only by the built-in functions' input
  and result type strategies, which can read it from `CallContext#getLambdaArgument(pos)` — the same
  channel a PTF already uses for a `DESCRIPTOR`'s column list. Carrying it in the type instead cost:
  a `FunctionRelDataType` that has to canonicalize nullability and recompute digests so that two
  identical signatures do not split during coercion (`SYMBOL` and `DESCRIPTOR` need no custom
  `RelDataType` at all — they map to a plain `SqlTypeName`); a new `->` token and a nested-type
  grammar production in `LogicalTypeParser`, on top of the keyword and required-precision production
  the count-only form needs anyway; and interaction with `leastRestrictive` over the inner
  types. Keeping the payload out of the type follows the precedent of both existing helper roots and
  keeps the audited surface at `DESCRIPTOR`'s level.
-  **Overloading a single `TRANSFORM` for arrays and maps.** No reference engine does this; arrays
  and maps get separate `ARRAY_*` and `MAP_*` functions.
-  **A combined map transform taking two lambdas** (`TRANSFORM(map, (k,v) -> newKey, (k,v) ->
  newValue)`, as ksqlDB does) instead of separate `MAP_TRANSFORM_KEYS` / `MAP_TRANSFORM_VALUES`.
  Rejected: separate single-purpose functions match Spark, Trino, and Databricks (ksqlDB is the only
  engine with the combined form), compose cleanly, and read better when only one side is
  transformed. The combined form's one genuine advantage — both lambdas observe the *original* `(k,
  v)` atomically — matters only when a value transform depends on the original key *while* keys are
  simultaneously rewritten, which is rare and still expressible by nesting. (The combined form is
  also a *multi-lambda-per-call* shape — a separate future capability, orthogonal to maps, that none
  of the functions here require.)
-  **Bare `TRANSFORM`/`FILTER`/`REDUCE` names.** `TRANSFORM` collides with Hive's script-transform
  syntax and `FILTER` with the SQL aggregate `FILTER (WHERE …)` clause. The `ARRAY_*` convention is
  consistent with Flink's existing array functions and avoids both collisions.
-  **`OperandTypes.ARRAY_FUNCTION` for `ARRAY_TRANSFORM`.** Despite the name it is the `ARRAY[…]`
  constructor's same-type checker; using it makes Calcite compute `leastRestrictive(ARRAY,
  FUNCTION)` and assert.
-  **Deleting the RAW common-type guard** to make lambda-body validation pass. This silently
  regresses genuine RAW mismatches into an untyped `ANY` column; the guard is narrowed instead.
-  **Committing to a wide `FunctionN` family now** — jOOQ and jOOλ ship `Function1`…`Function22` and
  `Function23` respectively, and Calcite's own grammar places no bound on lambda arity, so the
  obvious question is why stop at four. This was prototyped end to end at **arity 22** to answer it
  with a measurement rather than an argument. The result: the planner, the code generation
  (`BridgingFunctionGenUtil` builds the function object's `apply` from a parameter count and
  implements whatever interface the conversion class names, as a raw anonymous class),
  `LambdaFunctionFactory`, `UnresolvedLambdaExpression`, capture lifting, the Table API, the Python
  API and the compiled-plan format are **already arity-generic** — the prototype changed none of
  them, and the full test suite passed. The entire cost of a wider bound is therefore the
  `@PublicEvolving` types themselves: 41 permanent interfaces (22 external, 19 internal) for arity
  22, against 7 for arity 4. Rejected on that basis: every one of those types is a permanent
  surface commitment with architecture-test and documentation obligations, carried to serve
  functions that do not exist, and the measurement shows that nothing is bought by paying for them
  early — because the machinery is generic, the cost of adding an arity later is identical to the
  cost of adding it now. Four is chosen instead as one above the widest built-in delivered here,
  and the family's shape is what keeps the door open (see
  [*Known limitations*](#known-limitations)).
-  **Keeping `Function`, `BiFunction` and `TriFunction` as the conversion classes at arities one to
  three**, introducing `FunctionN` only at zero and four where nothing exists. This looks like the
  smaller change and was the first shape of the prototype, but it produces a conversion-class list
  a reader has to memorise — `Function0`, `Function`, `BiFunction`, `TriFunction`, `Function4` —
  in which the arity is recoverable from the name at only two of five positions, and it forces the
  same mixture into the documentation, the error messages, the bridging table and
  `getConversionClass()`. There is no compatibility argument for it: the `FUNCTION` type, lambda
  arguments and the `FunctionN` family all ship together here, so no existing UDF declares a lambda
  parameter of any spelling and nothing is being migrated. Making `FunctionN` the conversion class
  throughout costs nothing in expressiveness once `Function`/`BiFunction`/`TriFunction` are kept as
  accepted conversions — which they are — so the JDK types remain declarable and bridgeable, and
  the only thing that changes is which name the framework uses when it has to choose one itself.
-  **Inverting the family at arity three** (`TriFunction extends Function3` rather than the reverse),
  which is possible where `Function` and `BiFunction` are JDK-owned and therefore fixed. Rejected:
  it would make every existing `TriFunction` in Flink — including the `...WithException` family and
  user code with no connection to SQL — a `Function3`, coupling a general-purpose `flink-core-api`
  utility to the table module's lambda family for no gain. Accepting `TriFunction` as an equivalent
  conversion achieves the same result for lambda arguments without touching its supertypes.
-  **Handing the function the compiled `ExpressionEvaluator` directly**, rather than a function
  object. This is simpler for non-capturing lambdas, but it cannot support closures over outer
  columns: the captured values vary per row, while the receiving function's `eval` signature is
  author-fixed and has no channel for them. The function object solves this — it has stable arity
  and binds the per-row captures behind it — while still being *generated by* the evaluator's
  `LambdaFunctionFactory` implementation (the returned object's `apply` calls the evaluator's typed
  `eval` directly, so no reflective indirection or new body-codegen machinery is introduced) and
  leaving the function in control of its own loop via `apply(...)`.
-  **Generating the eight built-ins' loops inline in the planner**, in a
  `codegen/calls/HigherOrderFunctionCallGen.scala` beside the existing `SearchOperatorGen` and
  `Json*CallGen` generators, rather than implementing them as `BuiltInScalarFunction`s in
  `flink-table-runtime`. **An earlier draft of this FLIP proposed exactly this, and rejected the
  runtime-class form; that rejection is withdrawn and the positions are reversed.** The argument for
  inlining was per-element cost: a lambda body is a `RexNode`, so a runtime loop can only reach it
  through a compiled `ExpressionEvaluator`, and *per element* the query would pay an
  internal→external conversion of each argument, a `MethodHandle.invoke` through an `Object`-typed
  `apply`, and an external→internal conversion of the result — where the inlined form is
  straight-line code over the array's internal accessors (`ARRAY_TRANSFORM(a, x -> x + 1)` becoming
  `arr.getInt(i) + 1`). Both halves of that cost turned out to be removable, and neither was
  intrinsic to the runtime form. The conversions are gone because a lambda argument has an internal
  representation like every other argument (the `FunctionDataN` family,
  [*Public Interfaces → 5*](#5-lambda-arguments-for-user-defined-functions)), so a built-in reading
  `ArrayData` hands its lambda internal data and gets internal data back. The invocation cost is
  gone because the generated functional-interface object's `apply` calls the evaluator's typed
  `eval` directly instead of reaching it through a `MethodHandle` — a 1.5×–4.7× improvement to the
  lambda ABI, which the UDF path was paying too. What remains is not the virtual call — that
  measures as nearly free — but the boxing the `Object`-typed ABI forces around it, and on an
  isolated per-element harness it puts a built-in at 1.00×–1.31× of the inlined loop for
  `ARRAY<INT>` with a trivial body and at 0.96×–1.07× for `ARRAY<STRING>` or an expensive body
  ([appendix A3.3](#a33-what-the-lambda-abi-costs)). Two corrections to the argument as it was
  originally made belong beside that number. The inlined form was never the straight-line code the
  argument credited it with: `HigherOrderFunctionCallGen.generateArrayTransform` boxed every element
  into a `java.util.ArrayList` and handed `toArray()` to a `GenericArrayData`, so both candidates
  materialized the same boxed result, and the choice was always between two loops within 1.8 ns per
  element of each other — the shipped one being the cheaper of the two on the output side, since it
  presizes an `Object[]` from `array.size()` instead of growing a list. And the genuine
  straight-line form, which writes into a `BinaryArrayWriter` and boxes nothing, is 4×–5× faster
  than either and is reachable from the runtime class without a planner loop at all
  ([appendix A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)), so inlining is not the
  lever that unlocks it. Against that, the inlined form costs a Calcite
  operator, an operand type checker, an `ExpressionConverter` mapping and a code-generation method
  per function, plus lambda-binding state (`RexLambdaRef` scoping, capture evaluation) inside
  `ExprCodeGenerator` — machinery that exists nowhere else in the function stack and that every new
  higher-order function has to extend. The runtime form makes a new higher-order function a
  definition entry, a type strategy and a runtime class, which is what adding *any* scalar function
  costs, and it collapses the two lambda-execution stacks into one: a built-in and a UDF given the
  same body now compile to the same code, so nesting, captures, `deepCopy` of buffered results, null
  semantics and error messages cannot drift apart between them. Retaining inlining as a
  benchmark-gated fast path *beside* the runtime classes was also considered and is not needed: the
  worst shape costs 1.8 ns per element more than the loop it would replace, and no shape is slower
  for a reason that inlining is required to fix, so there is no dispatch, no flag and no second
  implementation to keep in sync. It remains the correct answer if some future shape cannot be
  brought back inside the runtime class, and it is testable in a way per-function bespoke codegen is
  not — the runtime class is the executable reference semantics, so equivalence is the existing
  suite run with inlining on and off.
-  **A `MAP_REDUCE` fold.** Deliberately omitted. Unlike the map functions shipped here (each
  present in Spark, Trino, and Databricks), a map fold has no such cross-engine precedent — only
  ksqlDB offers one, as an overloaded `REDUCE(map, init, (acc, k, v) -> …)` — and it is already
  expressible with existing built-ins as `ARRAY_REDUCE(MAP_ENTRIES(m), init, (acc, e) -> … e.f0 …
  e.f1 …)`. Revisit if demand appears.
-  **Requiring identical `MAP_ZIP_WITH` key types.** An earlier revision required the two maps' key
  types to be identical up to nullability and rejected merely compatible ones (`INT` and `BIGINT`),
  because code generation materializes the keys of both maps under one key wrapper and looks the
  union keys up in it — with two different key representations the merged map would either fail at
  runtime or count `1` and `1L` as two distinct keys. Users would have had to align the keys with an
  explicit `CAST`. This is rejected: it diverges from Spark's `map_zip_with` for no user-visible
  benefit, and, more importantly, from Flink's own `MAP_UNION`, which already generalizes two map
  types to their common type. The constraint is a code-generation detail, and the established way to
  satisfy it is the one `MAP_UNION` uses — generalize during validation and let the framework insert
  the implicit `CAST` — not to push it onto the user.
-  **Coercing `MAP_ZIP_WITH` keys inside code generation** instead of casting the operands during
  validation. Rejected: it would duplicate the existing `MapToMapAndMultisetToMultisetCastRule`
  inside the higher-order function generator, hide the widening from the plan, and leave the two
  surfaces free to derive different result types. Casting at the plan level keeps code generation
  unchanged — it still sees two maps with an identical key type — and makes the widening visible
  where every other implicit cast in Flink is.
-  **Deriving `ARRAY_REDUCE`'s assignability rule from Calcite's `leastRestrictive`** on the SQL
  side (the Table API strategy has no access to it and must use `LogicalTypeMerging.findCommonType`
  anyway). Rejected because the two disagree for a number of type pairs, and only the latter follows
  Flink's own type system. Calcite treats `FLOAT` as the widest approximate type — correct for the
  SQL standard, where `FLOAT` may be double precision, but not for Flink's 4-byte `FLOAT` — so it
  would accept a `DOUBLE` reducer body for a `FLOAT` accumulator and silently narrow the result; it
  also merges pairs Flink cannot cast between at all (e.g. `INT` into `TIME`), which then fail
  during code generation rather than validation. Using one merging function on both surfaces is what
  makes the parity harness meaningful.

## Future Work

-  **A standing JMH benchmark for higher-order functions.** The residual figure is no longer the
  debt it was: it has been re-measured on an isolated per-element harness with allocation alongside,
  and the result — with what it corrects in the original pipeline-level A/B — is recorded in
  [appendix A3.3](#a33-what-the-lambda-abi-costs). What is still missing is the *standing* form: a
  `flink-benchmarks` harness on the same footing as the scheduler and state backend benchmarks, so
  that these numbers are guarded against regression rather than established once. Its protocol is in
  [appendix A3.4](#a34-the-benchmark-protocol), and the property most worth pinning is that a
  built-in and a user-defined higher-order function given the same body stay identical, since they
  are the same generated code. A JIRA **will be filed** under FLINK-31207 once this FLIP is
  accepted. Nothing in the design depends on the outcome.
-  **Primitive specialization of the higher-order runtime classes.** The measurement in
  [appendix A3.3](#a33-what-the-lambda-abi-costs) puts a 4×–5× improvement, and every byte of the
  per-element garbage, in one place: the boxing that the `Object`-typed lambda ABI forces at the
  evaluator's boundary, together with the `Object[]`/`GenericArrayData` assembly of the result.
  Neither is intrinsic — the compiled body already computes on primitives, and both hooks needed to
  exploit that (a built-in's per-call-site specialization, and the `FUNCTION` type's conversion class
  selecting the lambda interface) already exist.
  [Appendix A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining) records the mechanism,
  the nullability question that gates it, and how the payoff splits. It is deliberately **not**
  proposed here: on a sixteen-element array the whole saving is around 90 ns per record against a
  record's microsecond-scale journey through an operator chain; the fast path applies only to
  `NOT NULL` primitive element types unless the interface gains a null channel; and `ARRAY_REDUCE`'s
  two independently-typed parameters multiply the interface matrix out. The trigger for doing it is
  evidence, not intent — a real job showing higher-order element throughput or boxing-attributed
  allocation as a hot spot.
-  More array and map HOFs (`ARRAY_EXISTS`, `MAP_KEYS_FILTER`, …). A new one costs what any scalar
  function costs: a `BuiltInFunctionDefinitions` entry naming a runtime class, a declarative input
  and output type strategy, the runtime class itself, tests, the Table API and PyFlink methods, and
  two documentation entries — with no new Calcite operator, operand checker, `ExpressionConverter`
  mapping, code generator or restore artifact. Completeness tests derive the set of higher-order
  built-ins from the definitions rather than from a list, so an omission in any of those fails a
  build rather than shipping (see [*Test Plan*](#test-plan)). As the family grows, consider
  extracting a shared element-loop skeleton for the array iterators that differ only in the
  per-element action and result assembly.
-  **Lambda arguments for aggregate functions**, whose lambda operand would need code generation on
  the aggregate path; the `FUNCTION` type, the `FunctionN` conversion classes and the
  closed-`RexLambda` representation would be reused unchanged. It is rejected with a clear message
  today, so it cannot be depended on before it is designed.
-  **Captures in a process table function's lambda**
  ([*Public Interfaces → 7*](#process-table-functions-ptfs)). They would need a channel for the
  captured values other than trailing operands, which a PTF's static signature has no room for. A
  capture is rejected with a clear message today, so admitting one later is additive.
-  **A process table function's lambda in `onTimer`.** The lambda object is built for one `eval`
  invocation, and `onTimer` receives no scalar arguments. Handing it to `onTimer` needs new API on
  `OnTimerContext`, and a defined answer for a timer that fires after a restore before any `eval`
  has run — which rules out remembering the object of the last `eval`, and the object cannot be put
  into state. Since `onTimer` never sees the lambda today, adding it later is additive.
-  **Optional lambda arguments of a process table function.** An omitted optional argument reaches
  `eval` as `NULL`, and `FUNCTION` has no `NULL` value. Supporting it needs either a nullable
  `FUNCTION` contract or a declared default lambda. An optional lambda is rejected at declaration
  today, so either is additive.

## Calcite fork: what is upstream, what is residual

Calcite's original lambda support (CALCITE-3679) targets *self-contained* lambdas — the body may
reference only the lambda's own parameters. The closures and nesting this FLIP requires need changes
inside Calcite's validator, converter and decorrelator.

Two upstream issues bear on this, and they land in **different releases**, so the set of changes
Flink has to carry shrinks in two steps rather than one:

- [CALCITE-7497](https://issues.apache.org/jira/browse/CALCITE-7497) ("Support constant folding in
  Lambda expressions", **Closed/Fixed, fixVersion 1.42.0 — released**, commit
  [`fa951db4b900`](https://github.com/apache/calcite/commit/fa951db4b900f16a74d087688096e98fb7dadf1e)).
  Its `RexShuttle.java` hunk (+6 / −2) replaces `visitLambda`'s discard-the-result body with a
  rebuild-if-changed one:

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

  That is unconditional — it is not gated on a conformance flag or a rule — and it is the whole of
  what the rewriting shuttles need, since each of them expresses its rewrite by overriding a leaf
  `visitXxx` and relies on the base class to reassemble the tree around it. It therefore covers
  `RelDecorrelator.DecorrelateRexShuttle`, `RelDecorrelator.RemoveCorrelationRexShuttle` and
  `DeduplicateCorrelateVariables` as used by `SqlToRelConverter.getCorrelationUse`.
- [CALCITE-6242](https://issues.apache.org/jira/browse/CALCITE-6242) ("Enhance lambda closure",
  Resolved/Fixed, fixVersion **1.43.0 — unreleased**, commit `81d93213e6d2`) implements a large part
  of the *validation and conversion* side upstream, from the same starting point
  (`Param 'T2.V' not found in lambda expression`).

**Delivery baseline.** Flink pins Calcite **1.41.0**, and this FLIP is delivered against that pin —
a Calcite upgrade is its own FLIP-sized change and is not bundled here. So the set of vendored
patches that actually ship is the **1.41.0 column** of the table below; the 1.42.0 and 1.43.0 columns
are what a maintainer needs in order to know what the upgrades delete. Every block that an upgrade
subsumes says so in place, naming the JIRA key and the version; every block that survives 1.43.0 says
why.

### Classification against the three upstream states

"carried" = Flink must vendor the change; "upstream" = the released/planned Calcite does it and the
Flink block becomes a deletion.

| Flink patch | 1.41.0 (pinned, shipped) | 1.42.0 (released) | 1.43.0 (planned) | Upstream equivalent |
| --- | --- | --- | --- | --- |
| `RelDecorrelator.DecorrelateRexShuttle.visitLambda` rebuilds the rewritten body | carried | **upstream** | upstream | CALCITE-7497 `RexShuttle.visitLambda` |
| `RelDecorrelator.RemoveCorrelationRexShuttle.visitLambda` rebuilds the rewritten body | carried | **upstream** | upstream | CALCITE-7497, same base-class fix |
| `SqlToRelConverter.getCorrelationUse` lambda-aware correlation de-duplication | carried | **upstream** | upstream | CALCITE-7497, via `DeduplicateCorrelateVariables`'s shuttle |
| `FlinkSqlLambdaScope.fullyQualify` delegates to the parent scope | carried | carried | **upstream** | CALCITE-6242, gated on the new `SqlConformance.allowLambdaClosure()` |
| `FlinkSqlLambdaScope.resolveColumn` delegates to the parent scope | carried | carried | **upstream** | CALCITE-6242 |
| `SqlValidatorImpl.registerFrom` registers lambda operands under the lambda scope | carried | carried | **upstream** | CALCITE-6242 (upstream registers only operand 1, the body) |
| `SqlToRelConverter.convertLambda` inherits the enclosing `nameToNodeMap` | carried | carried | **upstream** | CALCITE-6242 |
| `SqlToRelConverter.Blackboard.lookupExp` falls through instead of asserting | carried | carried | **upstream** | CALCITE-6242 |
| `RexUtil.isDeterministic` descends into `RexLambda` bodies | carried | carried | **upstream** | CALCITE-6242 makes `RexVisitorImpl.visitLambda` recurse when `deep` |
| `RelDecorrelator.CorelMapBuilder`'s visitor descends into lambda bodies | carried | carried | **upstream** | CALCITE-6242, same mechanism (`RexVisitorImpl(true)`) |
| `FlinkSqlLambdaScope.isParameter` walks enclosing lambda scopes | carried | carried | carried | none — residual 1 |
| `SqlToRelConverter` cross-query-boundary capture (`isSameQueryLambdaCapture`) | carried | carried | carried | none — residual 2 |
| `SqlToRelConverter.convertLambda` roots the body at the post-aggregation row | carried | carried | carried | none — residual 3 |
| `SqlToRelConverter` cumulative nested-parameter indices + renumbering | carried | carried | carried | none — residual 4 |
| `RelDecorrelator` preserves a higher-order call's return type | carried | carried | carried | none — residual 5 |
| `SqlCastFunction` accepts a placeholder while a lambda parameter is unbound | carried | carried | carried | none — Flink-specific deferred operand binding |

Two upstream decisions in CALCITE-6242 need attention at the 1.43.0 upgrade; both were re-verified
against Calcite `main` for this revision and are still true of the code as committed. It gates
closure on a new conformance flag, `SqlConformance.allowLambdaClosure()`
(`SqlConformance:652`, `SqlConformanceEnum:445`), which Flink's conformance will have to enable. And
it **rejects** a lambda parameter that shadows an enclosing one
(`RESOURCE.duplicateLambdaParameter`, `SqlValidatorImpl:7082,7092`), which this FLIP deliberately
allows and tests; that is a behavioural difference to raise upstream rather than adopt.

### Residual after both upstream fixes

These survive 1.43.0. The list is ranked by value to a future upgrade.

1.   **`AggChecker` reports an enclosing lambda parameter as ungrouped.**
    `SqlLambdaScope.isParameter` matches only the scope's own parameters, upstream included, so
    `ARRAY_TRANSFORM(a, e -> ARRAY_TRANSFORM(a, x -> x + e)) ... GROUP BY a` reports that `e` is not
    being grouped. `FlinkSqlLambdaScope.isParameter` walks the enclosing lambda scopes. This is the
    highest-value residual: it is a one-method, self-contained fix in a public non-final class, it is
    a plain bug rather than a design choice, and it is the natural follow-up to CALCITE-6242 — nested
    lambdas are exactly what that issue set out to enable.
2.   **Capturing across a query boundary loses its correlation.** CALCITE-6242's `lookupExp` sets
    `isParent = false` for *any* resolution made from a lambda scope that has inputs. That is right
    for a body capturing a column of its own query block, but wrong for a lambda inside a scalar
    sub-query capturing a column of the outer query: the reference must keep Calcite's correlated
    `DeferredLookup`/`RexCorrelVariable` path. Flink's `isSameQueryLambdaCapture` strips only
    consecutive lambda scopes and requires the resolved ancestor to be the enclosing query block.
    This is a bug in CALCITE-6242 as committed.
3.   **A lambda body is converted against the pre-aggregation row.** `convertLambda` roots the
    lambda `Blackboard` at `bb.inputs` but leaves its root, its `AggConverter` and its registered
    sub-queries unset, upstream included. In an aggregating query a captured group key then resolves
    against the pre-aggregation row — which agrees with the `Aggregate` output only when the `GROUP
    BY` order happens to match the input order, so the bug is invisible in the common case — and an
    aggregate in the body is never replaced by the reference the `AggConverter` registered for it.
4.   **Nested `RexLambdaRef`s are indistinguishable in the IR.** `RexLambdaRef` is *positional*, so
    a free reference to an enclosing lambda's parameter cannot be told apart from the inner lambda's
    own first parameter; CALCITE-6242 inherits the enclosing `nameToNodeMap` but does not renumber.
    This FLIP works around it by assigning nested parameters distinct indices during conversion and
    then closure-converting and renumbering (see [*Lambda captures*](#lambda-captures)). The
    representation-level fix — a scope-qualified `RexLambdaRef`, e.g. a `(depth, index)` pair —
    belongs upstream but touches the IR, serialization and plan digests, so it is the least
    self-contained of these.
5.   **A higher-order call's result type is invariant under decorrelation.** `RelDecorrelator`
    re-derives a call's return type whenever an operand changed, which for a rewritten lambda
    operand yields `ANY` (see
    [*Lambda captures → Capturing across a sub-query boundary*](#lambda-captures)). The type cannot
    change, because a correlated reference is replaced by an equivalently-typed input reference.

Flink-side companions live in `org.apache.flink.*` and are not upstream candidates. The
post-decorrelation re-lift and the leftover-correlation check in `FlinkDecorrelateProgram` both
descend into lambdas themselves; they exist because `RexShuttle` in the pinned 1.41.0 discards a
rewritten lambda body, so — unlike the vendored blocks above — they can be reconsidered on the
1.42.0 upgrade, once CALCITE-7497's rebuild is in place.

Independently of CALCITE-6242, every `RexVisitorImpl`/`RexShuttle` in Flink's own code is
lambda-blind by default and must opt in where a lambda body is material to what it computes. Three
do: the leftover-correlation check above, `ShortcutUtils.isDeterministicThroughProgram` and
`FlinkRexUtil.getNonDeterministicCallName` (see [*Lambda-body determinism*](#lambda-captures)). The
rest were audited and need no change, because a `RexLambda` reaching them is either closed (capture
lifting hoists every outer reference into an operand of the enclosing call, so the input-reference,
correlation, nested-field and time-indicator finders see everything they need in the operands) or
cannot contain what they look for (validation rejects aggregate, table and asynchronous functions in
a lambda body). The one traversal that a lambda body would otherwise reach with an unsupported call
— the async-call extraction behind `AsyncCalcSplitRule` — is why an `AsyncScalarFunction` is
rejected there in the first place.

Also not proposed upstream: the closed-`RexLambda` model (no slot for captured values) is a
reasonable design that closure conversion accommodates without an IR change; and the missing public
operand checker for "array + lambda over its element" is covered by a Flink-side checker (Calcite's
`OperandTypes.EXISTS` hard-codes `EXISTS` in its signature message and cannot defer while the array
type is unresolved), which is adequate for the functions shipped here.

### Why the changes cannot be applied through hooks or subclasses, and what they cost

Everything that *can* be done from outside a vendored copy, is: Calcite's `SqlLambdaScope` is public
and non-final with public resolution methods, so the closure behaviour lives in a Flink-owned
subclass, `FlinkSqlLambdaScope`; and `SqlValidatorImpl.validateLambda`, `.getLambdaScope` and
`.newValidationError` are public, so the reserved-`cap$`-prefix check and the lambda-body validation
rules are an override in `FlinkCalciteSqlValidator` rather than an edit to the vendored validator.
**No new Calcite class is copied by this FLIP.** The remainder has no reachable extension point —
private methods, package-private members and private static nested classes, with `RexShuttle`'s
lambda behaviour fixed in the base class; the change-by-change blocker table, verified against
`calcite-core-1.41.0`, is in
[appendix A5](#a5-why-the-calcite-changes-cannot-be-applied-through-hooks-or-subclasses).

Five vendored Calcite classes are patched, all five **already vendored** in Flink today. This work
adds **17** `FLINK MODIFICATION` blocks on top of **36** pre-existing blocks in the same five files.
A **sixth** file under `org/apache/calcite/**` is touched — `ExtraCalciteResource`, which gains four
validator messages — but it is Flink-owned rather than a vendored Calcite copy, so it carries no
rebase cost and is not counted here.
So this work adds **no new copied class** and enlarges a cost Flink already pays; the cost argument
uses the 17, not the 53. Of the 17, 9 are scheduled deletions on the Calcite upgrades already in
flight — 3 at **1.42.0** and a further 6 at **1.43.0** — rather than permanent carry. The remaining
Flink-specific lambda logic lives in Flink-owned files (`FlinkSqlLambdaScope`,
`FlinkCalciteSqlValidator.validateLambda`) and carries no rebase cost at all; the per-file
breakdown, and how the blocks are counted, is in [appendix A6](#a6-vendoring-cost).

## Appendix: supporting evidence (non-normative)

This appendix holds the supporting evidence for this FLIP: material a reviewer of the implementation
wants and a reader evaluating the proposal does not need. **Nothing here is normative.** Every
guarantee, every rejected-alternative rationale, every test-plan commitment and the cause and
consequence of every known limitation live in the body above, which is self-contained on its own.
The appendix only ever *elaborates* what the body already commits to.

Section references in *italics* below ([*Known limitations*](#known-limitations),
[*Test Plan*](#test-plan), [*User-defined functions*](#user-defined-functions), …) link to sections
of the body above.

Contents:

-  [A1. Test Plan (full)](#a1-test-plan-full)
- 
  [A2. `LogicalTypeRoot.FUNCTION` — exhaustive-switch audit (site by site)](#a2-logicaltyperootfunction--exhaustive-switch-audit-site-by-site)
-  [A3. Code-generation mechanics](#a3-code-generation-mechanics)
- 
  [A4. Spark bug found in the cross-engine comparison](#a4-spark-bug-found-in-the-cross-engine-comparison)
- 
  [A5. Why the Calcite changes cannot be applied through hooks or subclasses](#a5-why-the-calcite-changes-cannot-be-applied-through-hooks-or-subclasses)
-  [A6. Vendoring cost](#a6-vendoring-cost)
-  [A7. Further PTF composition examples](#a7-further-ptf-composition-examples)

### A1. Test Plan (full)

The FLIP body carries the test plan; every commitment is stated there. The following is the
unabridged enumeration behind it: the individual edge cases are what evidence the design diligence
behind each group, and a summary cannot carry them. Nothing here is a commitment the body does not
already make.

-  SQL IT cases (batch + streaming) for all eight functions: over columns and literals, `NULL`
  array/map, `NULL` elements/values, empty array/map, element/accumulator/key/value type changes,
  non-capturing nesting; and, in batch, the view/SQL-expansion round-trip (detailed in its own
  bullet below).
  Map-specific: `MAP_TRANSFORM_KEYS` producing a duplicate or `NULL` key is rejected; `MAP_ZIP_WITH`
  over each of the three whole-map key-set shapes — fully disjoint (every entry sees a `NULL` on
  exactly one side), partially overlapping, and identical (no `NULL` reaches the lambda at all) —
  and with a `NULL` input map; `MAP_ZIP_WITH` over two maps whose key types differ but are
  compatible (e.g. `INT` and `BIGINT`, or `DECIMAL(4,2)` and `DECIMAL(6,3)`) is accepted and merged
  under the common key type, while key types without a common type (e.g. `INT` and `STRING`) are
  rejected. Both are covered on the Table API surface as well, since it reaches the shared input
  type strategy along its own route: the same acceptance and rejection, plus an
  assertion that zipping a `NOT NULL`-keyed map with a nullable-keyed one yields the same result
  type as the equivalent SQL query. `ARRAY_REDUCE` correspondingly covers a reducer body that is
  assignable to, but not identical with, the accumulator (an `INT` body for a `DECIMAL(12,2)` and
  for a `BIGINT` accumulator), asserting that the body is converted and that the result keeps the
  accumulator type.
-  Closure tests over both surfaces: a lambda body that closes over one or more outer columns for
  each built-in function (including parameter shadowing), and a user-defined higher-order function
  whose lambda closes over an outer column. The map functions are additionally covered with a
  captured outer column over both a populated and a `NULL` input map (the `NULL` map short-circuits
  to `NULL` and the captured expression does not affect the result).
-  Correlation-across-a-sub-query-boundary tests: a higher-order call inside a scalar sub-query
  whose lambda closes over a column of the *enclosing* query (`SELECT (SELECT
  ARRAY_TRANSFORM(ARRAY[1], x -> x + t.a)[1]) FROM (VALUES (2)) AS t(a)`), executed end-to-end —
  covering a nested lambda, a body mixing a local and a correlated capture, and two distinct
  correlated columns (exercising the lambda-aware correlation de-duplication), with a same-query
  capture and a lambda-free correlated sub-query as controls that must be unaffected.
-  Nested-capture tests for the built-in functions over both surfaces: an inner lambda that closes
  over an enclosing lambda's parameter (`ARRAY_TRANSFORM(a2d, a -> ARRAY_TRANSFORM(a, x -> x +
  a[1]))`), including a two-level capture threaded through an intermediate lambda, a nested capture
  combined with an outer-column capture, and name shadowing between enclosing and inner parameters.
-  Nested-capture tests for a user-defined higher-order function over both surfaces: an inner UDF
  lambda that closes over an enclosing built-in lambda's parameter (`ARRAY_TRANSFORM(a2d, a ->
  my_array_transform(a, x -> x + a[1]))`), in addition to a UDF lambda closing over an outer column.
-  A deep-nesting test over both surfaces: four levels alternating user-defined and built-in
  higher-order calls (`my_nested_array_transform(a3d, a -> ARRAY_TRANSFORM(a, b ->
  my_array_transform(b, x -> ARRAY_REDUCE(b, x, (acc, y) -> acc + y + base + a[1][1]))))`), where
  the innermost lambda captures every enclosing lambda parameter as well as an outer column, and a
  lambda parameter is itself an array (the UDF receives a `Function<Integer[][], Integer[][]>`).
-  A built-in function with its own type inference (e.g. `IFNULL`) applied to a lambda parameter
  (`ARRAY_TRANSFORM(a, x -> IFNULL(x, 0))`), over the SQL, Table API, and API-rendered-as-SQL
  surfaces — exercising the deferral of a nested inference-based call whose operand is an enclosing
  lambda parameter that is still an unresolved plain `ANY` during the enclosing call's type
  inference.
-  Validation tests: a lambda outside a higher-order function rejected on both surfaces (SQL rejects
  it at the parser; the Table API resolver reports that a lambda is only supported as an argument of
  a function that declares one at that position, and a lambda passed to a function without a lambda
  argument is rejected by name); `FUNCTION` rejected as a column type by schema resolution
  (directly, nested in a constructed type, for a metadata and a computed column, when a catalog
  already holds one, and through CTAS), as a state type, and as a function return type; every cast
  to or from a `FUNCTION` type rejected, including between different arities; duplicate lambda
  parameter names rejected on both surfaces; a lambda parameter name that collides with the reserved
  capture prefix (a back-quoted `` `cap$x` ``) rejected. For the built-in functions, the
  user-visible signature errors are pinned for all eight: a first argument of the wrong type, a
  lambda with the wrong parameter count, a non-lambda at the lambda position, and a non-`BOOLEAN`
  `ARRAY_FILTER` predicate — each reported by Calcite with the call's argument types and the
  function's supported form. These pin the *SQL* rendering; the Table API's own expected-signature
  text is held to the same wording by the parity harness below. For user-defined functions, over the
  SQL and Table API surfaces: a lambda whose parameter count differs from the declared one, a lambda
  passed at an argument position that does not declare one, and a regular expression passed where a
  lambda argument is declared; over SQL additionally, a lambda whose parameter types cannot be
  derived because the sibling argument they depend on has the wrong type, a call with a wrong
  argument count (whose signature error must take precedence over the lambda binding error), and a
  function declaring a lambda argument with an unsupported number of parameters (four).
-  Lambda-body tests for the row-level constructs, over the built-in and the user-defined host: an
  aggregate over an outer column under `GROUP BY` on the array, on another column, and on a group
  key the body also captures — with the group keys given in both orders, since a body that resolved
  against the pre-aggregation row would still agree with the `Aggregate` output in one of them;
  `COUNT(*)`; an aggregate combined with an ordinary capture in one body; a user-defined aggregate;
  an `OVER` window with and without a partition; a scalar sub-query, plain and correlated; and each
  of these inside a nested lambda. Rejected, with the message naming the offending parameter: an
  aggregate, `OVER` window or sub-query over a lambda parameter — its own or an enclosing one — and,
  as a counter-case that must be *accepted*, an aggregate whose argument only references the
  parameters of a lambda nested inside it. Repeated over the Table API surface — both `GROUP BY`
  orders, an aggregate mixed with a column capture, the same aggregate used inside and outside the
  lambda (extracted once), an `OVER` window in a body, a nested lambda, and the two rejections
  (whose message does not name the generated parameter). Finally, a guard that hoisting the
  aggregate does not exempt the array column from the grouping rules: without a `GROUP BY` the query
  is rejected, and on the Table API exactly as the same query written without a lambda is.
-  Multiple-lambda tests for a user-defined function declaring two lambda arguments: each lambda
  applied independently, each closing over a distinct outer column (verifying the trailing capture
  operands are partitioned back to the owning lambda), and the same call nested inside a built-in
  lambda with one of its lambdas capturing the enclosing lambda parameter and the other an outer
  column (combining per-lambda partitioning with a nested capture). The test function combines its
  two lambda results **non-commutatively**, so that swapping the two lambdas — or attributing a
  lifted capture operand to the wrong one — changes the asserted result instead of cancelling out.
-  Further user-defined higher-order tests covering the argument shapes and call sites that the
  built-in functions do not exercise: a `NULL` array argument (the function returns without invoking
  the lambda) and a `NULL` element passed into the lambda body; a lambda argument *followed* by a
  regular argument (lifted captures are appended behind all declared arguments, so a trailing
  regular argument must keep its position); two sibling calls in one projection whose lambdas
  declare the same parameter name; a user-defined higher-order call directly inside another one's
  lambda body (as opposed to alternating with a built-in); a user-defined lambda inside a built-in
  *map* function's lambda, capturing both its key and value parameters; a user-defined function over
  a `MAP` argument with a `(key, value)` lambda whose parameter types come from a UDF-supplied
  derivation; a capturing lambda in streaming mode; and a higher-order call
  outside a projection, in a filter condition and as a grouping key.
-  Function-kind tests (see
  [*Public Interfaces → 7*](#7-function-kinds-and-process-table-functions)): a user-defined
  higher-order **table** function (`(ARRAY<E>, E -> R)` emitting one row per transformed element)
  executed end-to-end through `LATERAL TABLE(...)`, since a table function is the second supported
  host; and the rejections for the kinds that cannot host one — an `AggregateFunction` declaring a
  lambda argument, reported with the offending function kind, plus a `FUNCTION`-typed
  `StaticArgument` that is optional, declared by a scalar function, or nested in a constructed type,
  each with its own message — rejected where it is declared. The asynchronous
  kinds are covered in the same shape: an `AsyncScalarFunction` and an `AsyncTableFunction`
  declaring a lambda argument are rejected naming `ASYNC_SCALAR` / `ASYNC_TABLE`, with a
  higher-order *call* as an argument of an ordinary asynchronous function as the counter-case that
  must still work. The converse direction — a call of an `AsyncScalarFunction` *inside* a lambda
  body — is rejected on both surfaces, with and without a reference to the lambda parameter,
  alongside the equivalent table-function case.
-  **Lambda object contract tests**
  ([*Public Interfaces → 5*](#5-lambda-arguments-for-user-defined-functions)), one for each of the
  following normative
  rules that are observable from a UDF: the body is evaluated only when the function applies the
  lambda (a function that ignores its lambda argument completes even though the body would fail); a
  fresh object reaches every evaluation of the enclosing call, asserted by identity over the objects
  a function collects — one per row for a top-level call, one per element of the enclosing array
  when the call sits in a lambda body; the same object may be applied repeatedly and in an order the
  function chooses (each element
  applied twice, the array visited back to front, every application seeing the same capture
  snapshot); a failure in the body leaves `apply` unchanged — asserted by a function that catches
  it, by one that lets it propagate and fails the job, and against a built-in given the
  same body failing identically (an unchecked `RuntimeException`; the checked-exception and `Error`
  forms of the rule are not separately asserted); and a function called *from* a lambda body is
  opened and closed
  exactly once over the job's lifetime, pinning that the compiled body's lifecycle is the enclosing
  operator's.
-  **Process table function lambda tests**
  ([*Public Interfaces → 7*](#process-table-functions-ptfs)): a row-semantic PTF applying a
  one-parameter lambda to a column of every row, called from SQL with named and positional
  arguments, with a constant body, with a nested higher-order call that captures the lambda's
  parameter, and from the Table API; a PTF with two lambdas of different arities, called with its
  named arguments in a different order than declared; a set-semantic PTF folding a partition with a
  two-parameter lambda whose running result is kept in state, also as a restore test with a
  committed plan and savepoint; the same PTF lambda through a compiled plan that is serialized to
  JSON, loaded and executed; and the rejections — a captured sub-query, a lambda of the wrong arity,
  a PTF whose lambda is extracted from its `eval` method and therefore has no
  `LambdaInputTypeStrategy`, and a strategy that derives a different arity than the static
  declaration. Every PTF writes its strategy as a lambda over the interface's default methods,
  pinning that the defaults accept the system arguments the framework appends. Extraction tests pin
  that `Function` and `FunctionN` parameters of a PTF's `eval` are extracted as `FUNCTION(n)`.
-  PTF composition tests: a higher-order call as a PTF scalar argument, inside the query behind a
  PTF table argument, and projected over the relation a PTF produces — each executed and asserted
  on the collected
  rows, pinning that a higher-order call remains an ordinary scalar expression around a PTF. The
  third case applies the call to a constant array rather than to a column the PTF emits, so it pins
  the projection position, not consumption of a PTF result column.
-  **Views and materialized tables** — the two objects that do not persist their query as written
  but the *expanded* one (the validated statement unparsed back to SQL, with tables and catalog
  functions rewritten to their fully qualified identifiers and every other identifier merely
  quoted) and re-parse that string on every read. A lambda parameter is bound by the enclosing
  higher-order call rather than by the query's row type, so the expansion must leave it unqualified
  while a captured column is qualified like any other. Each case asserts the persisted query *and*
  the rows a `SELECT` from the object returns: a view over all eight built-in functions, whose
  names are rendered quoted but unqualified (`` `ARRAY_TRANSFORM`(…) ``) while the source table
  becomes `` `default_catalog`.`default_database`.`t` ``; a lambda
  parameter that shadows a column of the source table (qualifying it would silently read the column
  instead); nested lambdas whose parameters shadow each other, mixed with the capture of an
  enclosing parameter and of an outer column; a higher-order call in a `WHERE` clause, as a grouping
  key, and next to a `SELECT *`; a view stacked on a view whose column is itself a lambda result; a
  view over a user-defined higher-order function, whose *name* the expansion also rewrites when it
  is a catalog function (expanded to its fully qualified identifier, whereas a temporary *system*
  function is quoted only, like a built-in), including a two-lambda call with
  per-lambda captures; and a `CONTINUOUS` materialized table whose definition query filters,
  transforms and reduces an array and transforms a map, run on a mini cluster through the SQL
  gateway so that the refresh job re-plans the persisted query.
-  **Value constructors and mutable results in a lambda body** — a query column per shape that the
  deferral of `ARRAY[...]`, `MAP[...]` and `CAST` has to cover: an array/map built from the
  parameter and indexed, `CARDINALITY` over one, nesting (`ARRAY[ARRAY[x]]`), a constructed array
  consumed by a further higher-order call, a constructed array as an `ARRAY_REDUCE` body and as a
  `MAP_TRANSFORM_VALUES` body, and a `ROW(...)` cast to a concrete row type and projected. A
  separate test pins that mutable body results are not aliased: `ARRAY_TRANSFORM`, `ARRAY_ZIP_WITH`,
  `MAP_TRANSFORM_KEYS`, `MAP_TRANSFORM_VALUES` and `MAP_ZIP_WITH` with `ARRAY[...]` and `ROW(...)`
  bodies must yield element-wise distinct values, not N references to the last one.
  `ArrayReduceAccumulatorOwnershipITCase` is a buffering regression test for `ARRAY_REDUCE`
  accumulators: it asserts that the returned accumulator value survives being buffered by a
  downstream join, under batch object reuse and under streaming.
-  Type-system tests: `FunctionType` serialization, which round-trips through `LogicalTypeParser`
  (`FUNCTION(n)`, with malformed forms rejected), and its conversion class per arity (`Function0`
  through `Function4`, and no supported conversion above that range), with `Function`, `BiFunction`
  and `TriFunction` asserted to be bridgeable at their arities and `Supplier` asserted not to be;
  mismatched generic
  RAW types still fail common-type resolution (regression guard). Nullability-canonicalization tests
  assert that every construction path (`DataTypes.FUNCTION(n)`, `.notNull()`, `copy(false)`) yields
  an equal logical type, the same serializable string, and the same `FunctionRelDataType` digest.
-  Function-resolution tests (`FunctionCatalogTest`) pin the compatibility behavior for the eight
  reserved built-in names: an unqualified call collides with the built-in (which takes precedence
  over a same-named catalog or temporary-catalog function), a fully qualified identifier bypasses
  built-in resolution and finds the catalog function, and a temporary system function overrides the
  built-in.
-  Type-inference unit tests for the lambda declaration surface: a lambda argument is accepted only
  for a `FUNCTION`-typed argument and is never cast; the built-in strategies' parameter derivation
  (element type of an array argument, accumulator argument, map key/value type) reads the sibling
  argument and yields no type when it is absent or of the wrong kind; an underivable parameter makes
  the whole derivation fail; unsupported parameter counts are rejected. `SystemTypeInferenceTest`
  drives `checkLambdaArgs` / `of` over every `FunctionKind` with an `@EnumSource`, so the allow-list
  and the "a kind added later is rejected by default" property are pinned mechanically rather than
  by enumeration. A `CallContext` that wraps another one reports the lambda of the argument it was
  asked about: pinned for the split context of `SubsequenceInputTypeStrategy`, whose position shift
  would otherwise be silently missing from `getLambdaArgument` alone.
-  Compiled-plan restore tests for the eight functions, including a *capturing* lambda whose capture
  is lifted into a trailing lambda parameter and call operand, a user-defined higher-order function
  whose lambda argument round-trips through the compiled plan, and a user-defined function with two
  capturing lambdas (the per-lambda partitioning of the trailing capture operands must survive the
  round-trip). The `RexNode` serde tests cover a `RexLambda` and `RexLambdaRef` both with and
  without a lifted capture parameter.
-  Table API + Python expression tests, asserting the fully rendered call including the lambda body
  (the generated parameter names are normalized first); UDF lambda-argument tests, covering a
  one-parameter (`ARRAY_TRANSFORM`-shaped, received as a `Function1`), a two-parameter accumulator
  (`ARRAY_REDUCE`-shaped, received as a `Function2`), and a three-parameter (`MAP_ZIP_WITH`-shaped,
  received as a `Function3`) lambda over the Table API and SQL in batch, plus a capturing UDF
  lambda. The two arities with no built-in are covered by dedicated cases, since nothing else
  exercises them: a zero-parameter lambda declared as `Function0`, as a plain `Supplier` (which
  works because `Function0` extends it), and with a capture — the last asserting that the capture is
  bound once per function object while the body is applied once per element — and a four-parameter
  lambda plainly and with a capture, with a five-parameter declaration asserted to be rejected at
  the bound. Streaming is pinned by parity cases rather than by repeating the matrix: the unary and
  binary shapes on the Table API and a capturing unary shape in SQL are re-executed in streaming;
  the ternary shape is asserted in batch only.
-  **SQL↔Table-API parity harness**: a single table-driven test in which every built-in
  argument-rejection case (non-`BOOLEAN` `ARRAY_FILTER`/`MAP_FILTER` predicate, non-array first
  argument, `ARRAY_REDUCE` reducer result not assignable to the accumulator, `MAP_ZIP_WITH` key
  types without a common type) carries an equivalent SQL string and Table API expression and asserts
  that **both** surfaces reject it at validation. This is the enforced invariant behind the note in
  the *Table API / input type strategies* section that every code-generation-protecting argument
  rule is stated on both surfaces: adding a new higher-order function (or argument rule) adds a
  matching negative case here, forcing both surfaces to enforce it. It deliberately includes the
  type pairs for which Calcite's `leastRestrictive` and Flink's `findCommonType` disagree, since
  those are the ones a surface can only get wrong silently: a `DOUBLE` reducer body for a `FLOAT`
  accumulator (Calcite would accept it and narrow the result), a `TIMESTAMP_LTZ` body for a
  `TIMESTAMP` accumulator, and an `INT` body for a `TIME` accumulator (a pair Flink cannot cast
  between at all, so it would otherwise only fail during code generation). The harness no longer
  pins expected-signature text separately: both surfaces render a function's signature from its
  `InputTypeStrategy#getExpectedSignatures`, so there is a single description that cannot disagree
  with itself. What each case does assert is that the *rejection* happens at validation on both
  surfaces, and every higher-order built-in must appear here —
  `HigherOrderFunctionCoverageTest` fails the build for one that does not.
-  **Completeness of a new built-in.** Four checks, each deriving the set of higher-order built-ins
  by asking every `BuiltInFunctionDefinition` whether its input type strategy declares a lambda
  argument, so none can go stale as the family grows: `BuiltInFunctionDefinitionsTest` (a runtime
  class is declared), `HigherOrderFunctionCoverageTest` (an execution case in the semantics IT case
  **and** an argument-rejection case in the parity harness, asserted independently),
  `BaseExpressionsLambdaCompletenessTest` (the typed Table API method) and
  `SqlFunctionsDocsCompletenessITCase` (the English and Chinese documentation entries). No restore
  fixture is needed for a new function: the committed plans exercise the lambda machinery, which is
  shared, not the individual function.
-  **Determinism**: a lambda body that is non-deterministic (`ARRAY_TRANSFORM(a, x -> RAND())`)
  makes the enclosing call non-deterministic, while a deterministic body (`x -> x + 1`) does not — a
  regression guard covering all three entry points: the vendored `RexUtil.isDeterministic` patch,
  `ShortcutUtils.isDeterministicThroughProgram` (including through a `RexLocalRef`) and
  `FlinkRexUtil.getNonDeterministicCallName`. Each is also covered where it is consumed: two
  textually identical higher-order calls over a counting non-deterministic function in one
  projection are evaluated independently (and once more across a filter and a projection),
  end-to-end in batch; and a non-deterministic lambda body under a determinism requirement is
  reported by the NDU analysis rather than silently accepted.
-  **Closed-lambda invariant**: a fail-fast post-condition (`HigherOrderFunctionUtil.checkClosed`)
  run at the single capture-lifting choke point asserts every reachable `RexLambda` is closed
  (0-based positional parameters; no free outer column, captured `OVER` window or sub-query; every
  `RexLambdaRef` in range), with unit tests for the positive case and each rejection. The same
  invariant is re-checked when a `RexLambda` is *read back* from a compiled plan, which is an
  external input a different version may have written: `RexNodeJsonDeserializer` runs `checkClosed`
  and additionally verifies that every body reference agrees with the name and type the plan
  declares for that position, so a positional reference that no longer matches fails the restore
  instead of binding to the wrong parameter. Covered by `RexNodeJsonSerdeTest` with an out-of-range
  reference, a non-0-based parameter list, and a name and a type mismatch.
-  **PyFlink end-to-end execution**: unary/binary/ternary lambda adapters, an outer-column capture,
  a nested lambda whose inner body captures the enclosing lambda's parameter, `MAP_ZIP_WITH` over
  disjoint key sets, and a validation failure, each executed in batch and streaming and asserted on
  the collected result rather than only the rendered expression.
-  **Batch compiled-plan restore**: the eight built-in functions' streaming restore programs are
  additionally registered for `BatchExecCalc`, establishing batch/stream compiled-plan compatibility
  baselines.
-  **Large lambda bodies stay splittable**: a body far beyond `table.generated-code.max-length` —
  500 non-foldable `CAST` terms, ~64 KB of generated Java on its own — is executed for all eight
  functions, with the same expression outside a lambda as the control. The test fails with
  `InternalCompilerException: Code grows beyond 64 KB` if the element loop is emitted as a `for`
  rather than a `while` loop, i.e. as soon as the loop body stops being reachable by
  `flink-table-code-splitter`. The terms are summed as a balanced tree rather than the left-deep
  chain `a + b + c + ...` parses into: `ExprCodeGenerator` recurses once per nesting level of the
  expression, so a 500-deep chain overflows a 1 MB thread stack — the JVM default on Linux x86-64,
  hence on CI — before any code is generated, with or without a lambda. What the test is about is
  the volume of generated code, and the balanced shape keeps that volume while leaving the code
  generator's recursion limit, a pre-existing property unrelated to lambdas, unpinned.

### A2. `LogicalTypeRoot.FUNCTION` — exhaustive-switch audit (site by site)

The body states the design principle — `FUNCTION` is a planning-only helper type, intercepted before
any internal data path — and names the five rejection choke points (see
[*Type system*](#type-system-flink-table-common)). The full audit follows: every exhaustive `switch`
over `LogicalTypeRoot`, in four categories, with the reason each guard sits where it does and the
test that pins it.

1. **Explicitly handled — lambda planning path.**
   - `FlinkTypeFactory`: the `FunctionType ↔ RelDataType` bridge, both directions. A Flink
     `FunctionType` converts to `FunctionRelDataType`, a new `@Internal` `AbstractSqlType`
     (`SqlTypeName.OTHER`) that carries the parameter count and renders it in plan digests; the
     reverse direction accepts both that type and Calcite's own `FunctionSqlType`, which the
     validator produces for a lambda written in SQL. (Calcite's type stays parameterized; the SQL
     side reads the parameter and result types from it directly, so nothing is lost by Flink's type
     not repeating them.)
   - `FunctionType`: external conversion class `org.apache.flink.util.function.Function0` through
     `Function4`, with `java.util.function.Function`, `java.util.function.BiFunction` and
     `org.apache.flink.util.function.TriFunction` accepted as equivalents at arities one to three,
     and internal conversion class `FunctionData0`…`FunctionData4`, all selected from arity-indexed
     tables by the parameter count in the type — the sole reason the count is part of the type.
     `LogicalTypeUtils.toInternalConversionClass` delegates to `FunctionType` rather than repeating
     the internal table.
   - `SqlTypeFactoryImpl` (already vendored in Flink): its `FunctionSqlType` handling arrived with
     Calcite's lambda support and needs no change here — listed so the audit is complete.
   - `LogicalTypeVisitor.visit(FunctionType)`: a **source-compatible default method**, so existing
     visitors continue to compile; the lambda-aware ones override it.
2. **Explicitly rejected with a clear message — the five rejection choke points** (four
   materialization guards plus the cast guard).
   - `InternalSerializers` throws `ValidationException` ("The FUNCTION data type is a helper type for
     lambda arguments …"). Every runtime materialization of a value — a table column, a state entry,
     an intermediate result — obtains its serializer here, so this one guard is the last line of
     defence for *all* of "not a column / not a state type / not a persisted value"
     (`InternalSerializersTest#testFunctionTypeCannotBeMaterialized`).
   - A serializer is only built when a table is actually read or written, however, while a *schema*
     is persisted much earlier. `DefaultSchemaResolver` therefore rejects a `FUNCTION` type in the
     declared type of any column — physical, metadata or computed, and also nested in a constructed
     type — so the type can neither enter a catalog through `CREATE TABLE`, a `TableDescriptor` or a
     CTAS statement, nor be handed out again by a catalog that already holds one
     (`SchemaResolutionTest#testFunctionTypeRejectedAsColumnType`,
     `HigherOrderFunctionsBatchITCase#testFunctionTypeRejectedAsSchemaColumn` and
     `#testFunctionTypeRejectedOnCatalogRoundTrip`). SQL DDL has no syntax for the type in the first
     place.
   - `TypeInferenceUtil#inferOutputType` rejects a `FUNCTION` result, so a UDF that declares a
     `FUNCTION` **return** type fails during validation with the same message rather than later,
     when its result serializer is built
     (`UserDefinedHigherOrderFunctionITCase#testFunctionTypeIsRejectedAsReturnType`).
   - `TypeInferenceUtil#inferStateInfo` rejects it symmetrically for a declared **state** type — an
     aggregate accumulator or a named state entry of a process table function — so a state type that
     a function declares programmatically carries the same explanation, raised where the type is
     inferred rather than in the state code generation that follows; reflective extraction never
     reaches the guard, because it already rejects a `FUNCTION` state as a non-composite type
     (`UserDefinedHigherOrderFunctionITCase#testFunctionTypeIsRejectedAsStateType` and
     `#testFunctionTypeIsRejectedAsNamedStateEntry`).
   - `LogicalTypeCasts` rejects every cast to or from a `FUNCTION` type — implicit and explicit — a
     lambda is never a runtime value, so there is nothing to convert. Only the identity (a function
     type of the same arity) is compatible, and lambda arguments are never implicitly cast anyway
     (`LogicalTypeCastsTest`, `LogicalTypeCastAvoidanceTest`).
3. **Unreachable data / serde paths, guarded by throwing (or benign) defaults.** Every other exhaustive
   `switch(LogicalTypeRoot)` — `LogicalTypeMerging`, `LogicalTypeChecks`, `RowData`/`ArrayData` field
   getters and setters, `ToVariantConverter` (the writer behind `CAST` to `VARIANT`), and
   both the compiled-plan and SQL-gateway REST `LogicalTypeJsonSerializer`s — does **not** enumerate
   `FUNCTION`. `ToVariantConverter` is reached only for a cast that `LogicalTypeCasts` admitted, and
   the cast guard in category 2 runs before the rule that lets a constructed type cast to `VARIANT`
   leaf by leaf, so a `FUNCTION` — top-level or nested — never gets there. Most terminate in a throwing `default` (`UnsupportedOperationException` /
   `IllegalArgumentException` / `TableException`); a few same-root predicates
   (`TypeCheckUtils.isReference`, `LogicalTypeChecks.areTypesOfSameRootComparable`) instead fall
   through to a benign `default` (`return true`). Because category 2 makes `FUNCTION` unreachable on
   these paths, neither kind ever fires in practice; if a future change ever leaked a `FUNCTION` onto a
   data path the throwing sites would fail loudly rather than silently produce a wrong result.
   `leastRestrictive` is the one deliberate exception, described in
   [*Type system*](#type-system-flink-table-common).
4. **Type is never serialized in compiled plans** — neither the public `FunctionType` nor the
   internal `FunctionRelDataType`. Compiled plans serialize the `RexLambda`
   (parameter references + body), not the `FUNCTION` type, which is recomputed on deserialization via
   `RexBuilder.makeLambdaCall`. No `FUNCTION` case is therefore needed — or added — in
   `LogicalTypeJsonSerializer` / `RelDataTypeJsonSerializer`. Likewise Python never surfaces
   `FUNCTION` as a `DataType` (a lambda is a host-language callable mapped to the Java DSL through the
   gateway), so no Python type-mapping entry exists that could silently mishandle it. Even so,
   `FunctionType.asSerializableString()` honors the general `LogicalType` contract and round-trips
   through `LogicalTypeParser` like every other logical type: the parser gains a `FUNCTION` keyword
   and a production reading `FUNCTION(n)` with a required precision (`parseRequiredPrecision`, beside
   the existing `parseOptionalPrecision`), since a bare `FUNCTION` names no particular type. No new
   token character, nested-type production or `->` syntax is introduced. The wrapper
   nullability is canonicalized on the way back (see
   [*Compatibility*](#compatibility-deprecation-and-migration-plan)).

### A3. Code-generation mechanics

The FLIP states the guarantees these mechanics produce: buffered body results are element-wise
distinct, a large lambda body compiles exactly as the same expression outside a lambda, and a lambda
application costs an interface call plus the boxing the `Object`-typed ABI puts around it. This
section records *why* the generated code has the shape it does — a fact about the design that the
code itself does not state anywhere.

#### A3.1 Where a built-in higher-order function's loop lives

- In the runtime class, not in the planner. Each of the eight is a `BuiltInScalarFunction` in
  `flink-table-runtime`, named as the `runtimeClass` of its `BuiltInFunctionDefinitions` entry, and
  reached through the ordinary bridging path. It reads its collection argument as `ArrayData` /
  `MapData` and receives its lambda as the matching `FunctionDataN` interface, so
  the loop runs entirely on internal data and the lambda application crosses no conversion boundary.
- The loop shapes differ genuinely, which is why there are eight classes and not one:
  map-and-collect (`ARRAY_TRANSFORM`), conditional-collect (`ARRAY_FILTER`, `MAP_FILTER`),
  accumulator fold (`ARRAY_REDUCE`), dual-input walk to the longer length with `NULL` padding
  (`ARRAY_ZIP_WITH`), per-entry rebuild (`MAP_TRANSFORM_KEYS`, which additionally rejects duplicate
  and `NULL` keys, and `MAP_TRANSFORM_VALUES`), and union-of-keys iteration with a lookup in both
  maps (`MAP_ZIP_WITH`).
- Every body result that is buffered — appended to the result array/map, or carried into the next
  iteration as `ARRAY_REDUCE`'s accumulator — is `deepCopy`-ed, because a body producing a mutable
  value (`ARRAY[...]`, `ROW(...)`) writes into a *reusable* `BinaryArrayData`/`BinaryRowData` member
  that the next iteration overwrites; without the copy every element of the result aliases the last
  one. `ARRAY_FILTER`/`MAP_FILTER` need no copy: they buffer source elements read through the source
  `ArrayData`'s element getter, which yields a fresh view per call.
- The 64 KB per-method concern that shaped the earlier inlined design is now structural rather than
  stylistic. A lambda body is arbitrary user code that can exceed the JVM's limit on its own, and
  `flink-table-code-splitter` — which splits oversized generated methods — only descends into
  `if`/`else`/`while` blocks (`BlockStatementGrouper`). Under the inlined design the body sat inside
  a generated loop and had to be emitted as a `while` for the splitter to reach it. It now sits at
  the top of a generated evaluator class of its own, so the splitter treats it exactly as it treats
  the same expression written outside a lambda, and the runtime loop is ordinary hand-written Java
  that the splitter never sees. The test that pins this is unchanged
  ([*Test Plan*](#test-plan)).
- `ExprCodeGenerator` holds no lambda state. `visitCall` emits a placeholder for a `FUNCTION`
  operand, since a lambda is consumed through `CallContext#getLambdaArgument` and never evaluated as
  a value; `visitLambda` and `visitLambdaRef` are reachable only if a lambda escapes a higher-order
  call, and both throw.

#### A3.2 The generated function object for a lambda

- The framework generates it in `BridgingFunctionGenUtil.generateLambdaFunctionObject` — the single
  place a lambda body becomes executable code, for a built-in and a user-defined function alike. It
  compiles the body into an `ExpressionEvaluator` (via the `RexLambda → RexNodeExpression` bridge
  that rewrites each `RexLambdaRef` to an input reference over the parameter fields), and the
  generated evaluator class implements `LambdaFunctionFactory`. Its `bindLambda` method unpacks the
  capture array once and returns an anonymous functional-interface object whose `apply` casts the
  arguments to the parameters' declared classes and calls the evaluator's typed `eval`. There is no
  `MethodHandle`, hence no `Object[]` allocation or `asType` adaptation per application; one capture
  array is allocated per function object.
- The evaluator is a single instance per call site, opened in the enclosing generated class's
  `open()` and held in a `transient` field, and its `LambdaFunctionFactory` implementation creates
  the per-evaluation function objects. The enclosing class is the operator, or — for a lambda nested
  in another lambda body — the enclosing expression evaluator, which supplies a `FunctionContext`
  without a runtime context.
- The body is compiled in the representation the *receiving function* declared for that argument:
  internal for a built-in and for a UDF that opts in, external otherwise. The lambda itself is
  representation-neutral; `LambdaInfo` describes it as written.
- Where a function requires the body to produce a type of its own choosing rather than the type the
  body happens to have — `ARRAY_REDUCE` coercing its reducer result to the accumulator type — the
  coercion is expressed as a `CAST` wrapped around the body before compilation, so it folds into the
  compiled body instead of costing a conversion per application. Nullability is deliberately not
  part of the comparison: a nullable body is only ever handed to a nullable parameter, and casting
  would only restate the body's own type.
- A nested evaluator is compiled with the enclosing evaluator's (Janino) class loader as its parent,
  so the two generated classes must not share a name; the evaluator's `CodeGeneratorContext`
  therefore takes the enclosing context as its parent context to share the name counter (it shares
  nothing else — an evaluator is a class of its own).

#### A3.3 What the lambda ABI costs

The design above replaced an earlier one in which the eight built-ins' loops were generated inline
in the planner and only a UDF's lambda went through a function object. Two measurements settled the
replacement, and a third — made afterwards, in isolation — corrects the second and is what the
design's performance claims now rest on. All three are recorded here, because the earlier design's
rejection of runtime classes rested entirely on the cost they quantify.

- **The invocation ABI.** Janino does not implement the signature polymorphism of
  `MethodHandle#invoke`, so a generated `apply` that held a handle had to call `invokeWithArguments`,
  boxing its arguments into a fresh `Object[]` and performing an `asType` adaptation on **every**
  application — once per array element or map entry visited, with captures appended to the same
  array, so a capturing body boxed more. A test-only prototype in which the generated evaluator
  implemented the functional interface directly, measured against the handle as control in a
  single-parallelism batch pipeline with five alternating repetitions per mode, was **1.5×–4.7×**
  faster: 3.63× / 4.70× / 3.27× for 16-, 256- and 1,024-element arrays with a trivial body, 4.18×
  for a more expensive body, 2.17× for a `STRING` result, 1.54× for a 64-entry map. A JFR recording
  of the mixed run attributed 15.2% of CPU samples and 9.5% of sampled allocation weight to
  `invokeWithArguments` (`BoundMethodHandle$Species_L` and `Class[]` adapter churn) — diluted, since
  only one of the three modes in that run used the handle at all. This comparison is causal: both
  sides ran the same loop over the same representation, with only the invocation changed. It is the
  measurement behind the surviving design, and it applied to the *already shipped* user-defined
  path independently of any built-in.
- **What is left after the ABI fix.** With the handle gone, a built-in's lambda application is an
  interface call and the boxing the `Object`-typed ABI forces around it. The figure first recorded
  here — 0.86×–1.04× against the inlined predecessor, with 1.63× on the map shape — came from the
  same pipeline-level A/B, and it was wrong in both its value and its attribution. It has since been
  re-measured directly, and the corrected numbers are below. The map figure was never attributable
  to the ABI at all: that prototype used the UDF's external `Map`/`HashMap` path, where the shipped
  runtime class operates on `MapData`.

**The isolated re-measurement.** A JMH harness (JMH 1.37, JDK 17, Apple M1 Pro, two forks, five
one-second iterations after three of warmup, `-prof gc`) runs the shipped
`ArrayTransformFunction.eval` loop against the deleted prototype's loop transcribed verbatim from
`HigherOrderFunctionCallGen.generateArrayTransform`, over the same `BinaryArrayData` and with the
generated function object's `apply` reproduced as the ABI it actually is — an interface call, a cast
per user parameter, and a typed `eval`. Both produce the identical `GenericArrayData`, so everything
downstream of the call is common and excluded. Per element, for `ARRAY_TRANSFORM(a, x -> …)` over
`ARRAY<INT>`, as time and as allocation:

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

`ARRAY<STRING>` shows no difference at all — 0.96×–1.07× across the same lengths and both bodies, at
68–82 ns and 520–590 bytes per element — because the body and the copy of its result dominate
everything the ABI does. The third and fourth columns are not implementations that ever existed;
they are controls, and their purpose is
[A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining).

- **What the comparison actually says.** The band against the design that was rejected is
  1.00×–1.31×, not 0.86×–1.04×: wider than reported, and worth ≤1.8 ns per element on the cheapest
  shape a higher-order function has. It closes to nothing as soon as the body costs anything or the
  element type is not a primitive. Two things about it are worth stating plainly, because the
  original argument turned on them. First, the inlined prototype was not straight-line code over
  internal accessors: it boxed each element into a `java.util.ArrayList` and passed `toArray()` to a
  `GenericArrayData`, which is why the shipped form — presizing an `Object[]` from `array.size()` —
  is the cheaper of the two on the output side and loses only on the input side. Second, the
  difference between them is not dispatch. It is boxing: 36 bytes per element against the presized
  control's 20 is exactly one extra `Integer` for the argument, the one that
  `elementGetter.getElementOrNull` must create before `FunctionData#apply` can accept it.
- **One runtime loop is shared by every call site.** `ArrayTransformFunction.eval` is a single
  JIT-compiled method, so its `lambda.apply` site sees one generated class per distinct lambda
  running anywhere in the TaskManager, where an inlined loop would have a private site per query.
  Measured on the same harness at 256 elements, driving 1/2/3/5 distinct lambda classes through the
  shared loop costs 6.84/7.36/7.82/7.53 ns per element while a private loop stays flat at 5.7 ns.
  The effect is real, bounded by the megamorphic plateau, and worth about 1 ns per element — and
  specialization removes it as a side effect, by splitting the shared site per element type.
- **One historical row is explicitly not a baseline.** The `STRING` row of the *pipeline* A/B
  (2.17× ABI, 0.57× against the built-in) measured a copy, not an invocation: the inlined built-in
  `deepCopy`-ed each body result into a `BinaryStringData` while that prototype's external path
  stored a Java `String` reference and copied nothing. With both sides operating on `ArrayData` and
  copying alike, the isolated harness shows the ratio gone, as expected.

#### A3.4 The benchmark protocol

For the standing benchmark filed as follow-up work ([*Future Work*](#future-work)). The decisions
above are already made and the residual figures have been taken once
([A3.3](#a33-what-the-lambda-abi-costs)); this is the shape the *standing* form should have, so that
they are guarded against regression rather than established once. Each point below notes what the
one-off re-measurement did and did not cover.

- Compare four variants over the same collection: **(A)** the built-in `ARRAY_TRANSFORM(a, x -> x +
  1)`; **(B)** a user-defined higher-order function with the same signature applying its lambda per
  element; **(C)** the same arithmetic without a lambda, as the control; and **(D)** an assembly
  that boxes nothing and writes the result through a `BinaryArrayWriter`, as the headroom bound
  ([A3.5](#a35-the-headroom-and-how-it-comes-out-without-inlining)). A and B should coincide — they
  are the same generated code — which is itself the property most worth guarding, and is the one
  thing the re-measurement did not check: it covered A, C and D only.
- Report **per element**, not per record, since the overhead is paid once per element, with
  allocation rate alongside. Allocation is the more stable signal of the two: the per-element byte
  counts in [A3.3](#a33-what-the-lambda-abi-costs) separate the variants exactly, where the times
  overlap at short lengths.
- Sweep four dimensions, because a single one-thousand-element `INT` case cannot separate a fixed
  per-application overhead from a body-proportional one: collection length (a few elements, where
  per-call overhead dominates, through tens of thousands, where allocation and cache behaviour
  dominate); body cost (trivial arithmetic against an expensive body); result shape (a scalar
  against a constructed `ARRAY`/`ROW`, which exercises conversion and copying); and element type (a
  boxed primitive against `STRING` or a nested type). Cover at least one array and one map function,
  since the map functions also carry key handling. The re-measurement covered length, body cost and
  element type, and left result shape and the map functions untouched.
- Measure on an isolated evaluator harness rather than end-to-end, and report variance across
  repetitions. A pipeline-level point estimate cannot separate 0.95× from 1.04×, which is precisely
  the band the original figures claimed to sit in — and, as it turned out, did not.
- Drive the shared runtime loop with more than one distinct lambda class in at least one
  configuration. A single-lambda JVM is the unrepresentative case: the built-in's loop is one
  JIT-compiled method for the whole TaskManager, and its type profile is shared across every call
  site in it. The re-measurement covered this separately, at one length only
  ([A3.3](#a33-what-the-lambda-abi-costs)).

#### A3.5 The headroom, and how it comes out without inlining

Not a proposal — the reasoning behind the follow-up item in [*Future Work*](#future-work), and the
explanation of the last two columns of the table in [A3.3](#a33-what-the-lambda-abi-costs). Between
the shipped loop at 7.21 ns and 36 bytes per element and the same transformation writing straight
into binary at 1.44 ns and nothing, there is a 4×–5× gap that has nothing to do with where the loop
is generated. It is in three places, none of them intrinsic:

- **The input side.** `elementGetter.getElementOrNull(array, i)` returns `Object`, so a primitive
  element is boxed before `FunctionData#apply` can accept it — 16 of the 36 bytes.
- **The ABI itself.** The evaluator's `eval` signature is derived from each argument's
  `getConversionClass`, and for internal data that is `LogicalTypeUtils#toInternalConversionClass`,
  which is boxed by definition. Yet immediately inside the generated `eval`, `argMappingCode`
  converts the boxed argument to `primitiveTypeTermForType` with a separate `$0IsNull` flag, the
  body computes on the primitive, and `genToExternalConverterAll` boxes the result again on the way
  out. **The body is already primitive; the boxing is boundary bookkeeping on both sides of it.**
- **The output side.** The result box, the `Object[]` slot and the `GenericArrayData` around them,
  plus a `resultElementSerializer.copy` per element that is a no-op for immutable element types.

Both hooks needed to remove this already exist. A `BuiltInScalarFunction` is specialized per call
site — `ArrayTransformFunction`'s constructor already inspects `getArgumentDataTypes()` to build its
element getter, and can just as well select a primitive loop and a `BinaryArrayWriter` there, with
no planner involvement whatsoever. And the lambda's interface is whatever the `FUNCTION` type's
conversion class names, through
`LambdaFactorySpec(paramFields.size, lambdaDataType.getConversionClass)`, so a built-in can nominate
an `@Internal` primitive interface without touching `FunctionData` or
`java.util.function.Function` on the public user-defined surface. The evaluator generator is already
primitive-aware — `externalResultCasting` handles a primitive `externalResultClass` through
`primitiveToWrapper` — so `eval(int)` returning `int` is generatable as the code stands. The single
blocker is `generateBindLambda`, which hard-codes `public Object apply(Object arg0)`;
`LambdaFactorySpec` would have to carry the target signature rather than only the interface.

Two things gate it, and they are why it is a follow-up rather than part of this FLIP:

- **Nullability is the design question**, not the machinery: internal conversion classes are boxed
  precisely so that a null has a representation. Either the fast path is restricted to `NOT NULL`
  element and body types — array literals qualify, many real sources do not — or the interface gains
  a null channel mirroring code generation's own `(resultTerm, nullTerm)` pair, which makes the
  function object stateful in a path that has to survive nesting and re-entrancy.
- **The matrix does not stay small.** One interface per arity per primitive type is tolerable for
  the one-parameter iterators; `ARRAY_REDUCE`'s `(acc, element)` shape multiplies two independently
  typed parameters out.

The payoff splits about evenly between the two halves, which is worth knowing before building
either: at 1,024 elements with a trivial body, removing the input box, the interface call, the cast
and the `copy` is worth 3.64 ns and 16 bytes per element, and removing the result box, the
`Object[]` and the `GenericArrayData` is worth 2.13 ns and 20 bytes. A half-measure buys about half.
Correctness needs no new gate: the runtime class is the executable reference semantics, so a
specialization is an internal swap that must leave every existing test result unchanged.

### A4. Spark bug found in the cross-engine comparison

Establishing the cross-engine comparison in [*Known limitations*](#known-limitations) turned up a
genuine bug in Spark, recorded here because it is the evidence behind one half of that comparison.
Verified by running Spark 3.5.3 locally (`bin/spark-shell --master 'local[1]'`, Java 17):

```sql
CREATE OR REPLACE TEMP VIEW t AS
  SELECT * FROM VALUES (array(1,2,3), 'a', 10), (array(1,2,3), 'a', 20) AS t(a, g, base);

SELECT transform(a, x -> sum(x)) FROM t;                    -- AnalysisException [MISSING_GROUP_BY]
SELECT transform(a, x -> sum(x)) FROM t GROUP BY a;         -- NullPointerException at runtime
SELECT transform(a, x -> sum(x + base)) FROM t GROUP BY a;  -- NullPointerException at runtime
```

Aggregating over a lambda **parameter** is accepted by the analyzer and then fails in the executor
with `java.lang.NullPointerException: Cannot invoke "java.lang.Integer.intValue()"` — a task
failure, not an analysis error. The analyzer plan shows the aggregate left sitting inside the lambda
(`Aggregate [transform(a#39, lambdafunction(sum(lambda x#45), lambda x#45, false)) …]`), i.e. Spark
treats `sum(<lambda parameter>)` as an aggregate of the outer query over a variable that only exists
per array element. This is a bug rather than a missing feature, for the reason given in
[*Known limitations*](#known-limitations): an aggregate folds rows, while a lambda parameter is an
array element within a single row, so the expression has no reading to implement — folding an
array's own elements is what `aggregate`/`reduce` is for. It should be rejected in the analyzer, as
Trino does; `CheckAnalysis.scala` carries no such rule and
`sql-tests/inputs/higher-order-functions.sql` has no case for it. The first message compounds it:
`MISSING_GROUP_BY` invites the user to add a `GROUP BY`, which converts an analysis error into a
runtime crash.

Everything else Spark does here is correct, and is the behaviour this FLIP matches: aggregates over
*outer* columns in a lambda body, `COUNT(*)`, `OVER` windows, plain and correlated scalar
sub-queries, and outer aggregates inside a *nested* lambda.

This is noted for reporting to the Spark project; nothing in this FLIP depends on it. Flink rejects
the parameter case cleanly during validation, so the comparison in
[*Known limitations*](#known-limitations) — Flink at the permissive end for the outer-column class,
and stricter than Spark for the parameter class — holds either way.

### A5. Why the Calcite changes cannot be applied through hooks or subclasses

The body states which changes live outside the vendored copies and that the remainder has no
reachable extension point (see
[*Why the changes cannot be applied through hooks or subclasses*](#why-the-changes-cannot-be-applied-through-hooks-or-subclasses-and-what-they-cost)).
The modifiers that decide it, verified against `calcite-core-1.41.0`, follow.

**What is reachable from outside.** `SqlLambdaScope` is public and non-final and its three
resolution methods (`fullyQualify`, `resolveColumn`, `isParameter`) are public, while
`DelegatingScope.parent`/`.validator` are `protected final` and `DelegatingScope.getParent()` and
`SqlLambdaScope.getParameterTypes()` are public — hence `FlinkSqlLambdaScope`. `SqlValidatorImpl`'s
`validateLambda`, `.getLambdaScope` and `.newValidationError` are public — hence the
`FlinkCalciteSqlValidator` override.

**What is not**, change by change:

| Change | Blocker |
| --- | --- |
| Create `FlinkSqlLambdaScope` instead of `SqlLambdaScope`; register lambda operands under the lambda scope | `SqlValidatorImpl.registerFrom` and `.registerOperandSubQueries` are **private**, and there is no factory or SPI for scope creation |
| `convertLambda` blackboard mirroring and cumulative parameter indices; `Blackboard.lookupExp` resolution | `SqlToRelConverter.convertLambda` is **private**; `Blackboard.lookupExp` is package-private and reached only from it |
| Lambda-aware correlation de-duplication | `SqlToRelConverter.getCorrelationUse` is **private** and `DeduplicateCorrelateVariables`'s shuttle is a **private static** nested class |
| `RelDecorrelator` lambda descent and type preservation | `DecorrelateRexShuttle`, `RemoveCorrelationRexShuttle` and `CorelMapBuilder` are **private static** nested classes; `RexShuttle.visitLambda`'s behaviour is fixed in the base class in 1.41.0 (CALCITE-7497 changes it in 1.42.0, but only in the base class — so on the pinned version there is still no override point outside the copy) |
| `RexUtil.isDeterministic` lambda descent | **public static** method with an anonymous visitor, called from Calcite's own rules — a Flink-side alternative would not be consulted |

### A6. Vendoring cost

Five vendored Calcite classes in `flink-table-planner` are patched: `SqlValidatorImpl`,
`SqlToRelConverter`, `RelDecorrelator`, `RexUtil` and `SqlCastFunction`. All five are **already
vendored** in Flink today. Six files under `org/apache/calcite/**` change in total; the sixth,
`ExtraCalciteResource`, is a Flink-owned resource interface rather than a vendored copy, and the
distinction matters because only vendored copies carry a rebase cost at the next Calcite upgrade.

Blocks are counted as matched `FLINK MODIFICATION` `BEGIN`/`END` pairs. Every block this change
adds uses the standard spelling `// ----- FLINK MODIFICATION BEGIN -----`; one pre-existing block in
`SqlValidatorImpl` (FLINK-40937, the `UPDATE` `SET` source-expression lookup) is spelled
`// FLINK MODIFICATION BEGIN` without the dashes. Both spellings contain `FLINK MODIFICATION BEGIN`,
so a grep for that string reproduces the "Total" column exactly; a grep for `FLINK MODIFICATION`
counts marker *lines* and roughly doubles the answer.

| File | Pre-existing blocks | Added here | Total | Deleted at 1.42.0 | Deleted at 1.43.0 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `SqlValidatorImpl` | 16 | 2 | 18 | 0 | 2 |
| `SqlToRelConverter` | 16 | 7 | 23 | 1 | 2 |
| `RelDecorrelator` | 4 | 6 | 10 | 2 | 1 |
| `RexUtil` | 0 | 1 | 1 | 0 | 1 |
| `SqlCastFunction` | 0 | 1 | 1 | 0 | 0 |
| **Total** | **36** | **17** | **53** | **3** | **6** |

**The cost argument uses the "Added here" column, 17** — `A6` is about the marginal cost this change
imposes, not the cumulative fork.

The marker style in these files is **not** uniform, and this is worth recording as a fork-hygiene
finding in its own right: the three primary files carry `FLINK MODIFICATION BEGIN`/`END` blocks,
while `RexUtil`, `SqlCastFunction` and `ExtraCalciteResource` carried no markers at
all before this change (it adds them to the first two, `RexUtil` and `SqlCastFunction`, but not to
`ExtraCalciteResource`, whose additions are message declarations in a Flink-owned interface). Raw
`BEGIN`/`END` pair counts also do not
balance in every vendored file, because prose mentions of the markers inside comments match the same
grep; the imbalances in `SimpleCalciteSchema.java` (0/2) and `MultiJoin.java` (5/4) are pre-existing
and unrelated to this change. A reversed spelling, `// BEGIN FLINK MODIFICATION`, also exists
elsewhere in the repository — four occurrences across `SqlMapTypeNameSpec.java`,
`LogicalTableScan.java` and `SqlMapValueConstructor.java` — but in files this change does not touch
and A6 does not count, so it has no effect on the numbers above.

`ExtraCalciteResource` is not one of the five. It sits under `org.apache.calcite.*` but is a
Flink-owned file rather than a Calcite copy; it only gains the four new validation messages, and it
carries no `FLINK MODIFICATION` markers because there is no upstream text to fence off. Of the five
that *are* copies, `SqlCastFunction` is already a heavily Flink-modified one, and
its one-line change — accepting a placeholder type while an enclosing lambda parameter is unbound —
is specific to Flink's deferred operand binding and has no upstream analogue.

### A7. Further PTF composition examples

The body keeps one of the three verified compositions (see
[*7. Function kinds, and process table functions*](#7-function-kinds-and-process-table-functions)).
All three, as executed:

```sql
-- as a PTF scalar argument
SELECT * FROM my_ptf(t => TABLE t, threshold => ARRAY_REDUCE(ARRAY[1,2,3], 0, (acc, x) -> acc + x));

-- inside the query behind a PTF table argument
CREATE VIEW v AS SELECT name, ARRAY_REDUCE(arr, 0, (acc, x) -> acc + x) AS s FROM t;
SELECT * FROM my_ptf(t => TABLE v, threshold => 1);

-- projected over the relation a PTF produces
SELECT ARRAY_TRANSFORM(ARRAY[1, 2], x -> x + 1) FROM my_ptf(t => TABLE t, threshold => 1);
```
