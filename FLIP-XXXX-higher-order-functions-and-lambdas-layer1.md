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

# FLIP-XXXX: Lambda Expressions in Flink SQL and Table API (Foundation)

| | |
|---|---|
| **Discussion thread** | To be started. The combined proposal was discussed in https://lists.apache.org/thread/ky7kq96nv1wd7n1od8xytqcbkw44d7mk |
| **Vote thread** | — |
| **JIRA** | [FLINK-31207](https://issues.apache.org/jira/browse/FLINK-31207) |
| **Release** | — |
| **Status** | Draft |

Please keep the discussion on the mailing list rather than commenting on the wiki (wiki
discussions get unwieldy fast).

> **About this FLIP.** This FLIP is the first delivery, called *layer 1*, of the combined proposal
> *Higher-order functions and lambda expressions in Flink SQL and Table API* (see the discussion
> thread above). It covers the shared lambda support and four built-in array functions in SQL, the
> Java Table API and PyFlink. Map functions and lambda arguments for user-defined functions are
> planned for follow-up FLIPs.
>
> This proposal has been prototyped and validated in a
> [reference implementation](https://github.com/swisscom-bigdata/flink/tree/flink-31207-higher-order-functions-and-lambdas-layer-flip1),
> including unit, integration and restore tests.

1. [Motivation](#motivation)
2. [Public Interfaces](#public-interfaces)\
   2.1 [Lambdas in SQL](#lambdas-in-sql)\
   2.2 [The array functions](#the-array-functions)\
   2.3 [Java and Python API](#java-and-python-api)\
   2.4 [Types and type inference](#types-and-type-inference)\
   2.5 [Compiled plan format](#compiled-plan-format)
3. [Proposed Changes](#proposed-changes)\
   3.1 [Design principles](#design-principles)\
   3.2 [How it works](#how-it-works)\
   3.3 [What a lambda body can use](#what-a-lambda-body-can-use)\
   3.4 [What `EXPLAIN` shows](#what-explain-shows)\
   3.5 [For function authors](#for-function-authors)\
   3.6 [Where errors are reported](#where-errors-are-reported)\
   3.7 [Known limitations](#known-limitations)\
   3.8 [Calcite dependency](#calcite-dependency)
4. [Compatibility, Deprecation, and Migration Plan](#compatibility-deprecation-and-migration-plan)
5. [Test Plan](#test-plan)
6. [Rejected Alternatives](#rejected-alternatives)
7. [Future Work](#future-work)
8. [References](#references)

## Motivation

Flink SQL currently cannot apply an expression to each element of an array inline. Users either
write a user-defined function for each computation, or flatten the array into rows with `UNNEST`,
apply the computation, and rebuild the array with an aggregate:

```sql
SELECT t.id, ARRAY_AGG(u.v * 10)
FROM t CROSS JOIN UNNEST(t.vals) AS u(v)
GROUP BY t.id;
```

This has several problems:

- It is hard to read and easy to get wrong.
- Rows with an empty or `NULL` array are dropped, because `CROSS JOIN UNNEST` produces no rows for
  them.
- The original rows are rebuilt by grouping, so the query needs a unique key. Rows that share an
  `id` are merged.
- The order of the rebuilt array cannot be guaranteed, because `ARRAY_AGG` does not support
  `ORDER BY`.
- In streaming, an inherently stateless projection is turned into a stateful aggregation that
  emits updates.

Writing a user-defined function avoids this rewrite, but then each computation needs its own Java
or Python class, packaged and registered before the query runs, and every column it reads must be
passed as an extra argument.

Comparable engines, such as Spark, Trino, Snowflake and ksqlDB, already solve this with **lambda
expressions**: small, unnamed functions written inline and passed to **higher-order functions**
that apply them. With lambdas, the query above becomes a plain projection:

```sql
SELECT id, ARRAY_TRANSFORM(vals, x -> x * 10) FROM t;   -- [1, 2, 3] -> [10, 20, 30]
```

The `x -> body` syntax is not part of the SQL standard. It is used by systems such as Calcite,
Spark SQL, Trino and Snowflake.

Supporting lambdas needs two parts: **shared lambda support** that every higher-order function
uses (parsing, validation, a `FUNCTION` type, captures, code generation, compiled plans, the Table
API and PyFlink), and the **higher-order functions** themselves, such as `ARRAY_TRANSFORM`. Once
the shared part exists, each function is a small runtime class.

This FLIP proposes the shared lambda support together with four built-in array functions, in SQL,
the Java Table API and PyFlink. Shipping the shared part without any function would add public API
that no query could use; the four functions test it end to end.

Calcite 1.41, the version Flink uses, already parses lambdas and has planner nodes for them
(CALCITE-3679), but a lambda body may only read its own parameters. This FLIP lets a body also read
the columns of the query and the parameters of enclosing lambdas, as in
`ARRAY_TRANSFORM(prices, p -> p * rate)` (see [Calcite dependency](#calcite-dependency)).

### Goals

1. Support lambda expressions as function arguments in Flink SQL, the Java Table API and PyFlink.
2. Provide one interface, `LambdaInputTypeStrategy`, through which a function declares the
   parameter types of its lambda. Also provide a documented contract describing how the function
   calls the lambda at runtime. In this FLIP only built-in functions use them. They are designed so
   that user-defined functions can use them later without breaking changes.
3. Let a lambda body use values from outside the lambda (**captures**): columns, parameters of
   enclosing lambdas, aggregates, `OVER` windows, and scalar sub-queries (plain and correlated),
   nesting to any depth.
4. Run lambdas as generated code inside the existing operators. Store them in compiled plans, so
   that a job that uses lambdas can be restored from a savepoint.
5. Keep the behavior of queries without lambdas.
6. Add four built-in array functions, `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE` and
   `ARRAY_ZIP_WITH`, documented and tested end to end. They are the first users of the design.

### Non-Goals

- **Lambdas as values.** A lambda cannot be stored in a column, returned, cast, or kept in state.
- **Lambda arguments for user-defined functions.** A user-defined function that declares a lambda
  argument is rejected with a validation error (see [For function authors](#for-function-authors)).
  Calling a user-defined function inside a lambda body is supported. Lambda arguments for
  user-defined functions are planned for a follow-up FLIP.
- **Further higher-order functions**, such as the map functions. They are planned for a
  follow-up FLIP (see [Future Work](#future-work)).
- **Lambda arguments for aggregate, table-aggregate and asynchronous functions.** These are not
  planned. An aggregate call has no place to carry a lambda into its accumulator. An asynchronous
  function finishes its work on another thread after `eval` has returned, when the lambda object
  is no longer valid.
- **Function references** such as `ARRAY_TRANSFORM(a, my_udf)`. A lambda must be written out:
  `ARRAY_TRANSFORM(a, x -> my_udf(x))`.
- **More than four lambda parameters.** Captured values do not count. More can be added later
  without breaking changes.
- **A Scala API.** The deprecated Scala Table API inherits the new methods from `BaseExpressions`,
  but per FLIP-265 nothing is written, tested or documented for it.
- **A Calcite upgrade.** The work targets the Calcite version Flink currently uses (1.41.0).

## Public Interfaces

All new API is `@PublicEvolving`, like the other logical types (`VARIANT`, `BITMAP`, `UUID`) and
the type inference interfaces it extends. `@Experimental` is not needed: the follow-up FLIPs only
add to this API, for example default methods on `LambdaInputTypeStrategy`. Classes that are not
listed here are `@Internal`.

| Kind | Items |
|---|---|
| **SQL lambdas** | A function argument can be a lambda, such as `x -> x * 2` or `(acc, x) -> acc + x`. Its body can use the query's columns and the parameters of outer lambdas. |
| **SQL functions** | `ARRAY_TRANSFORM`, `ARRAY_FILTER`, `ARRAY_REDUCE`, `ARRAY_ZIP_WITH` (new reserved built-in names) |
| **New types (15)** | `FunctionType`; `Function0`…`Function4`; `FunctionData0`…`FunctionData4`; `LambdaInputTypeStrategy`; `LambdaInfo`; `UnresolvedLambdaExpression`; `LambdaExpression` |
| **Additions to existing types** | `LogicalTypeRoot.FUNCTION`; `LogicalTypeVisitor#visit(FunctionType)`; `CallContext#getLambdaArgument(int)`; `DataTypes.FUNCTION(int)`; four `BuiltInFunctionDefinitions` constants |
| **Promotion** | `LocalReferenceExpression`: `@Internal` → `@PublicEvolving`, because `LambdaExpression#getParameters()` returns it. Its members are unchanged. |
| **Table API** | `BaseExpressions`: `arrayTransform`, `arrayFilter`, `arrayReduce`, `arrayZipWith` |
| **PyFlink** | `Expression`: `array_transform`, `array_filter`, `array_reduce`, `array_zip_with` |
| **Compiled plan format** | Two new JSON node kinds, `LAMBDA` and `LAMBDA_REF` (see [Compiled plan format](#compiled-plan-format)) |
| **Type strings** | `FUNCTION(n)` in `LogicalTypeParser`, which makes `FUNCTION` a keyword there |
| **Behavior** | A lambda can only be an argument of a built-in function. How a function may use the lambda at runtime is defined by [the lambda object contract](#the-lambda-object-contract), which is part of the public Javadoc of `LambdaInputTypeStrategy`. |

### Lambdas in SQL

Calcite's grammar already parses the lambda syntax; no parser change is needed in Flink.

```sql
x -> expression
(x, y) -> expression
```

- **Only as an argument of a built-in function.** `SELECT x -> x + 1` fails to parse, and a lambda
  passed to a user-defined function is rejected (see [For function authors](#for-function-authors)).
- **Parameters have no written types.** The function derives them from its other arguments: in
  `ARRAY_TRANSFORM(a, x -> x + 1)`, `x` gets the element type of `a`.
- **Names.** A name in a body is looked up as the lambda's own parameter, then as a parameter of
  an enclosing lambda, then as a column of the query. A parameter therefore hides a column or table
  alias of the same name, and an inner parameter hides an outer one: for `arr = [1, 2, 3]`,
  `ARRAY_TRANSFORM(arr, x -> ARRAY_REDUCE(arr, 0, (acc, x) -> acc + x))` returns `[6, 6, 6]`.
  Calcite 1.43 rejects such an inner parameter (CALCITE-6242); Flink keeps accepting it after the
  upgrade (see [Calcite dependency](#calcite-dependency)).
- **Reserved names.** Parameter names beginning with `cap$` are reserved for captures (see
  [Compiled plan format](#compiled-plan-format)).
- **`() -> …`** parses, and the type system supports it (`FUNCTION(0)`), but no function accepts
  it yet.

### The array functions

These four functions are the first to take lambdas. They cover one- and two-parameter lambdas,
parameter types taken from one or two arrays, and an accumulator whose type may depend on the body
(`ARRAY_REDUCE`, see the [widening rule](#accumulator-widening)).

| SQL | Java Table API | PyFlink | Result |
|---|---|---|---|
| `ARRAY_TRANSFORM(a, x -> …)` | `$("a").arrayTransform(x -> …)` | `col("a").array_transform(lambda x: …)` | `ARRAY<R>`: the lambda applied to every element, in order |
| `ARRAY_FILTER(a, x -> …)` | `$("a").arrayFilter(x -> …)` | `col("a").array_filter(lambda x: …)` | `ARRAY<E>`: the elements for which the lambda returns `TRUE`, in order |
| `ARRAY_REDUCE(a, init, (acc, x) -> …)` | `$("a").arrayReduce(init, (acc, x) -> …)` | `col("a").array_reduce(init, lambda acc, x: …)` | The accumulator after folding from left to right, starting from `init` |
| `ARRAY_ZIP_WITH(a, b, (x, y) -> …)` | `$("a").arrayZipWith($("b"), (x, y) -> …)` | `col("a").array_zip_with(col("b"), lambda x, y: …)` | `ARRAY<R>`: the lambda applied at each position; the shorter array is padded with `NULL` |

```sql
ARRAY_TRANSFORM(ARRAY[1, 2, 3], x -> x * 10)                                -- [10, 20, 30]
ARRAY_FILTER(ARRAY[1, 2, 3], x -> x > 1)                                    -- [2, 3]
ARRAY_REDUCE(ARRAY[1, 2, 3], 0, (acc, x) -> acc + x)                        -- 6
ARRAY_ZIP_WITH(ARRAY[1, 2], ARRAY[3, 4, 5], (x, y) -> COALESCE(x, 0) + y)   -- [4, 6, 5]
```

A `NULL` array gives `NULL` (for `ARRAY_ZIP_WITH`, if either array is `NULL`). For
`ARRAY_TRANSFORM` and `ARRAY_FILTER`, an empty array gives `[]`; for `ARRAY_REDUCE`, it gives the
initial value. `ARRAY_ZIP_WITH` gives `[]` only if both arrays are empty; otherwise it pads the
shorter array with `NULL` up to the length of the longer one. Both of its lambda parameters are
therefore nullable, even if both arrays have `NOT NULL` elements. `NULL` elements are passed to the
lambda. Each element produces its own result, also when the body builds an array or a row.

<a id="accumulator-widening"></a>**`ARRAY_REDUCE` accumulator typing.** The accumulator starts with
the type of the initial value, but it may be widened during type inference, after the reducer is
resolved. At runtime, the accumulator keeps that one type in every iteration. For example, `''`
starts as `CHAR(0)` and may be widened to `STRING`. Similarly, adding `0` to a `DECIMAL(3, 2)`
produces a `DECIMAL(13, 2)`. The rule is:

1. **Keep** the initial value's type if it can hold the lambda result without loss. Nullability is
   handled separately in step 4.
2. Otherwise **widen once** to a type large enough for every further iteration:

   | Accumulator and result | Widened to | Example |
   |---|---|---|
   | Character strings | `STRING` | `CHAR(0)` → `STRING` |
   | Binary strings | `BYTES` | `BINARY(1)` → `BYTES` |
   | Decimals, or an integer with a decimal | `DECIMAL(38, s)`, keeping the scale | `INT` → `DECIMAL(38, 2)` |
   | Any other type | The smallest type that holds both | `INT` → `BIGINT`, `TIMESTAMP(0)` → `TIMESTAMP(3)` |
   | `ARRAY`, `MULTISET`, `MAP`, `ROW` | Widened per element, key, value or field by the rows above | `ARRAY<CHAR(1)>` → `ARRAY<STRING>` |

   Strings and decimals go straight to their maximum because they grow with every iteration: each
   concatenation adds length, each addition adds a digit. Other types do not grow, so the smallest
   common type is enough. Widening stays within one family (character strings, binary strings,
   exact numerics or approximate numerics), or else within the same type root. So `INT` may become
   `DECIMAL`, but not `DOUBLE`.
3. **Reject** the call with a cast hint if no such widening exists, or if the body still does not
   fit after it.
4. If the body is nullable, the accumulator becomes nullable.

The call returns the final accumulator type.

```sql
ARRAY_REDUCE(ARRAY['a', 'b', 'c'], '', (acc, x) -> acc || x)              -- 'abc'  STRING
ARRAY_REDUCE(ARRAY[1.25, 2.50], 0, (acc, x) -> acc + x)                   -- 3.75   DECIMAL(38, 2)
ARRAY_REDUCE(ARRAY[1, 2], 0, (acc, x) -> acc + x * CAST(0.5 AS DOUBLE))   -- rejected: INT vs DOUBLE
```

This is the most complex typing rule in this FLIP. Other rules that were considered are listed
under [Other alternatives](#other-alternatives).

### Java and Python API

```java
// org.apache.flink.table.api.internal.BaseExpressions<InType, OutType>
public OutType arrayTransform(Function1<OutType, OutType> lambdaFunction);
public OutType arrayFilter(Function1<OutType, OutType> lambdaFunction);
public OutType arrayReduce(
        InType initialValue, Function2<OutType, OutType, OutType> lambdaFunction);
public OutType arrayZipWith(InType array, Function2<OutType, OutType, OutType> lambdaFunction);

// org.apache.flink.table.expressions
public final class UnresolvedLambdaExpression implements Expression {
    public UnresolvedLambdaExpression(List<String> parameterNames, Expression body);
    public List<String> getParameterNames();
    public Expression getBody();
}

public final class LambdaExpression implements ResolvedExpression { // output type FUNCTION(n)
    @Internal
    public LambdaExpression(List<LocalReferenceExpression> parameters, ResolvedExpression body);
    public List<LocalReferenceExpression> getParameters();
    public ResolvedExpression getBody();
}
```

```python
# pyflink.table.expression.Expression
def array_transform(self, transformation: Callable[['Expression'], 'Expression']) -> 'Expression'
def array_filter(self, predicate: Callable[['Expression'], 'Expression']) -> 'Expression'
def array_reduce(self, initial_value,
                 reducer: Callable[['Expression', 'Expression'], 'Expression']) -> 'Expression'
def array_zip_with(self, array,
                   combiner: Callable[['Expression', 'Expression'], 'Expression']) -> 'Expression'
```

- **The Java or Python lambda builds the body.** The DSL method calls it once, while the query is
  built, with one expression per parameter, for example
  `$("a").arrayTransform(x -> x.plus($("base")))`. It is never called per element. In PyFlink,
  Python's own `if` therefore does not work in a body: write `if_then_else(x > 0, x * 2, 0)`.
  Python UDFs cannot be called in a body, because they run in a separate Python process.
- **Parameter names.** The DSL methods give each parameter a unique generated name, such as
  `$lambdaParam$0`, so that it never hides a column. A lambda can also be built as an
  `UnresolvedLambdaExpression`, which keeps the names it is given, and passed to `call(...)`.

### Types and type inference

```java
// org.apache.flink.table.types.logical
public enum LogicalTypeRoot { ..., FUNCTION(LogicalTypeFamily.EXTENSION) }

public interface LogicalTypeVisitor<R> { ...; default R visit(FunctionType functionType) { ... } }

public final class FunctionType extends LogicalType {
    public static final int MAX_CONVERTIBLE_PARAMETER_COUNT = 4;
    public FunctionType(int parameterCount); // 0 to 4
    public int getParameterCount();
}

// org.apache.flink.table.api.DataTypes; the default conversion class is FunctionN
public static DataType FUNCTION(int parameterCount);

// org.apache.flink.util.function: Function0 to Function4, here for one parameter
public interface Function1<T1, R> extends java.util.function.Function<T1, R> {}

// org.apache.flink.table.data: FunctionData0 to FunctionData4, over internal data structures
public interface FunctionData1 {
    Object apply(Object arg0);
}

// org.apache.flink.table.types.inference
public interface LambdaInputTypeStrategy extends InputTypeStrategy {
    // 0 to 4 types, derived from the arguments before the lambda; empty if not a lambda
    Optional<List<DataType>> getExpectedLambdaParameterTypes(
            CallContext callContext, int argumentPos);
}

public interface CallContext { ...; default Optional<LambdaInfo> getLambdaArgument(int pos) {...} }

public final class LambdaInfo {
    public LambdaInfo(
            @Nullable Expression body,
            List<DataTypes.Field> parameterFields,
            DataType returnDataType);
    public Optional<Expression> getBody(); // only while a function is being specialized
    public List<DataTypes.Field> getParameterFields();
    public DataType getReturnDataType();
}
```

- **`FUNCTION(n)` is only for lambda arguments.** It cannot be the type of a column, literal,
  function result or state (see [Where errors are reported](#where-errors-are-reported)), and SQL
  has no syntax for it. It carries only the parameter count; the parameter and result types are in
  `LambdaInfo`. Inside the planner, a lambda's type also counts its captures, so `LogicalTypeParser`
  accepts any count; such a type never reaches a function.
- **Nullability does not apply to `FUNCTION(n)`.** A lambda is never a value, so it cannot be
  `NULL`. Flink always marks the type as nullable, and requesting `NOT NULL`, as in
  `DataTypes.FUNCTION(1).notNull()` or `FUNCTION(1) NOT NULL`, leaves it unchanged.
- **A function receives a lambda** as `FunctionN` (the default) or `FunctionDataN`, picked through
  the conversion class of its `FUNCTION(n)` argument. The four built-ins use `FunctionData1` and
  `FunctionData2`. `Function0` to `Function2` extend `Supplier`, `Function` and `BiFunction`, so a
  received lambda works with Java APIs. `Function3` and `Function4` have no counterpart in the JDK
  and extend nothing. In the Table API, `Function1` and `Function2` type the Java lambda that
  builds the body.
- **0 to 4 parameters**, so that every `FUNCTION(n)` has a Java class. No built-in higher-order
  function in Spark or Trino takes more than three; layer 2 uses three (`MAP_ZIP_WITH`), and
  user-defined functions in layer 3 may use any of them (see [Future Work](#future-work)).

### Compiled plan format

A lambda is stored as two new JSON node kinds:

- `LAMBDA`: the lambda, with its parameters (`parameters`) and its body (`expr`).
- `LAMBDA_REF`: a parameter, with its name, index and type. It declares a parameter in `parameters`
  and refers to it in `expr`, as `INPUT_REF` refers to an input column.

The plan stores the call after capture lifting (see [How it works](#how-it-works)): a value that
the body reads from outside the lambda becomes an extra parameter, a *capture*, that the call fills
with an extra last operand. For `SELECT ARRAY_TRANSFORM(a, x -> x + base) FROM t`, with `a` in
column 0 and `base` in column 1:

```text
Query:  ARRAY_TRANSFORM(a, x -> x + base)
Lifted: ARRAY_TRANSFORM(a, (x, cap$1) -> (x + cap$1), base)
```

```json
{
  "kind": "CALL",
  "internalName": "$ARRAY_TRANSFORM$1",
  "operands": [
    { "kind": "INPUT_REF", "inputIndex": 0, "type": "ARRAY<INT>" },
    {
      "kind": "LAMBDA",
      "parameters": [
        { "kind": "LAMBDA_REF", "name": "x", "index": 0, "type": "INT" },
        { "kind": "LAMBDA_REF", "name": "cap$1", "index": 1000000, "type": "INT" }
      ],
      "expr": {
        "kind": "CALL",
        "syntax": "BINARY",
        "internalName": "$+$1",
        "operands": [
          { "kind": "LAMBDA_REF", "name": "x", "index": 0, "type": "INT" },
          { "kind": "LAMBDA_REF", "name": "cap$1", "index": 1000000, "type": "INT" }
        ],
        "type": "INT"
      }
    },
    { "kind": "INPUT_REF", "inputIndex": 1, "type": "INT" }
  ],
  "type": "ARRAY<INT>"
}
```

The lambda declares its own parameter `x` and the capture `cap$1`, which replaces `base` in the
body. The call passes `base` (`INPUT_REF` 1) as an extra last operand, which fills `cap$1` for each
row. With several captures, the extra operands fill the capture parameters in the same order.

- **Indexing.** A lambda's own parameters are numbered 0, 1, … in the order written; captures are
  numbered from 1,000,000. The index, not the name, tells Flink which parameters are captures (see
  [Other alternatives](#other-alternatives)). Before lifting, nested lambdas number their parameters
  in one sequence across all levels, so the capture range starts high enough never to meet them.
- **Naming.** The name only makes the plan readable: `cap$` followed by where the value comes from,
  such as a column index (`cap$1`), an outer lambda's parameter (`cap$x`), or an `OVER` window, a
  sub-query, a correlation variable, `CURRENT_WATERMARK`, `PROCTIME()` or a current time function,
  numbered among the lambda's captures (`cap$over$0`, `cap$sub$0`, `cap$cor$0`, `cap$wm$0`,
  `cap$proctime$0`, `cap$time$0`).
- **Nested lambdas.** Each lambda numbers its parameters separately (see the example below).
- **Restore checks.** When a plan is restored, Flink checks that each lambda numbers its own
  parameters from 0 and its captures from 1,000,000, and that the body uses only declared
  parameters, each with the same name and type as declared, and no column or correlation
  variable. Only the indices mark captures; the `cap$` names are not checked. The numbering is
  therefore part of the plan format: changing the 1,000,000 base later would break existing plans.
- **No new `ExecNode`.** A lambda is stored inside an existing node, such as `Calc`; no node is
  added and no node version changes. The lambda's `FUNCTION` type is not written, because Flink
  rebuilds it from the parameters and the body.
- **`EXPLAIN`.** A lambda is shown as written, not in this lifted form (see
  [What `EXPLAIN` shows](#what-explain-shows)).

For nested lambdas, take `ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(b, y -> x + y))`, with `a` in
column 0 and `b` in column 1. Lifting starts with the inner lambda:

1. **Inner lambda.** It declares only `y`, so `x` is a capture. `x` becomes the parameter `cap$x`,
   and the inner call passes `x` as an extra last operand:
   `ARRAY_TRANSFORM(b, (y, cap$x) -> (cap$x + y), x)`.
2. **Outer lambda.** It declares `x`, so the `x` passed to the inner call stays as it is. The
   column `b` is a capture: it becomes `cap$1`, and the outer call passes `b`.

```text
Lifted: ARRAY_TRANSFORM(a, (x, cap$1) -> ARRAY_TRANSFORM(cap$1, (y, cap$x) -> (cap$x + y), x), b)
```

An index refers to the nearest enclosing `LAMBDA`, so the same index can mean different parameters:

| Reference | Index | Nearest `LAMBDA` | Means |
|---|---|---|---|
| `cap$1`, first operand of the inner call | 1,000,000 | Outer | The column `b` |
| `x`, last operand of the inner call | 0 | Outer | The outer parameter `x`, which fills `cap$x` |
| `y` in the inner body | 0 | Inner | An element of `b` |
| `cap$x` in the inner body | 1,000,000 | Inner | The value of `x` |

A column that only the inner body reads is passed in through every level. With
`y -> x + y + base`, where `base` is column 2, both lambdas get the capture `cap$2` with index
1,000,001. The outer call reads it from the input row and the inner call from the outer lambda:

```text
Lifted: ARRAY_TRANSFORM(a, (x, cap$1, cap$2) ->
            ARRAY_TRANSFORM(cap$1, (y, cap$x, cap$2) -> ((cap$x + y) + cap$2), x, cap$2),
        b, base)
```

## Proposed Changes

### Design principles

1. **A lambda is an argument, never a value.** This removes serializers, state migration and
   function equality from the problem.
2. **Reuse of Calcite nodes.** Calcite's syntax and its `RexLambda`/`RexLambdaRef` nodes are reused;
   the compiled plan only adds JSON kinds for them. There is no new operator, `ExecNode` or runtime
   state; a higher-order call lives in a `Calc`.
3. **The function owns its typing.** A query does not declare lambda parameter types. The
   function's `LambdaInputTypeStrategy` derives them from the other arguments, for example the
   array's element type. SQL and the Table API call the same method, and the planner has no
   per-function code, so a new higher-order function needs no planner change.
4. **Make captures explicit early.** Right after conversion, captured values become call operands.
   From then on a lambda body reads only its parameters, and existing rules, code generation and
   plan serde need no capture logic. This includes lambdas in sub-queries: a reference to the
   enclosing query becomes a correlated call operand, which decorrelation rewrites like any other.
   Only `EXPLAIN` puts the captures back into the body, for reading.
5. **Queries without lambdas keep their plans and results.** Most changes to code that all queries
   run first check for a lambda, such as a `RexLambda` or a lambda parameter whose type is not
   known yet. The capture-lifting passes are not isolated in this way: they visit every query, after
   conversion, in each join condition and after decorrelation. They only rewrite higher-order calls,
   though, so a query without lambdas comes out unchanged. The one change in behavior for such
   queries is the narrowed [`RAW` guard](#raw-guard).

### How it works

- **Validation.** When the validator reaches a higher-order call, it asks the function's
  `LambdaInputTypeStrategy` for the parameter types, binds them and validates the body. Flink's
  lambda scope lets the body read the columns of the query and the parameters of enclosing
  lambdas. The Table API resolver asks the same strategy.
- **Capture lifting.** Right after conversion, each value that a body reads from outside the lambda
  becomes a capture: `x -> x + base` becomes `(x, cap$1) -> x + cap$1`, and the call gets `base` as
  an extra last operand. This applies to columns, parameters of enclosing lambdas, `OVER` windows,
  sub-queries, and values that the operator computes once per row: `CURRENT_WATERMARK`,
  `PROCTIME()` and the current time functions such as `CURRENT_TIMESTAMP`. Nested lambdas are
  lifted from the inside out. From then on, the optimizer, code generation and plan serialization
  see an ordinary scalar call.
- **Code generation and runtime.** The body is compiled into a class of its own, with Flink's
  expression evaluator, and opened together with the operator. For each row, the captured values
  are boxed into an array and bound into a new function object: two small allocations per
  higher-order call, plus one for each captured primitive value. For each element, the function
  calls that object once through its interface, which passes the element and the result as
  objects, so primitive values are boxed. A nested call is part of the enclosing body, so it binds
  a new function object for each element of the enclosing array.
- **Streaming.** A higher-order call is evaluated in a stateless `Calc`, so it changes no changelog
  mode, upsert key or state. A non-deterministic body makes the call non-deterministic.

### What a lambda body can use

A body is a scalar expression evaluated per element. It may use its own parameters, the parameters
of all enclosing lambdas, and anything the enclosing query computes per row. With
`orders(customer, ts, prices ARRAY<INT>, discount)` and the rows `('alice', 1, [10, 20], 1)`,
`('alice', 2, [30], 3)` and `('bob', 3, [5, 50], 4)`:

| Capture | Example | Result |
|---|---|---|
| Column | `ARRAY_TRANSFORM(prices, p -> p - discount)` | `[9, 19]`, `[27]`, `[1, 46]` |
| Enclosing parameter | `ARRAY_TRANSFORM(prices, p -> ARRAY_FILTER(prices, q -> q > p))` | `[[20], []]`, `[[]]`, `[[50], []]` |
| Aggregate | `ARRAY_FILTER(ARRAY_AGG(discount), d -> d > AVG(discount)) … GROUP BY customer` | alice `[3]`, bob `[]` |
| `OVER` window | `ARRAY_TRANSFORM(prices, p -> p - SUM(discount) OVER (PARTITION BY customer ORDER BY ts))` | `[9, 19]`, `[26]`, `[1, 46]` |
| Scalar sub-query | `ARRAY_TRANSFORM(prices, p -> p - (SELECT MAX(discount) FROM orders))` | `[6, 16]`, `[26]`, `[1, 46]` |

Each one also works in the Table API, except scalar sub-queries, which are not supported there.
Lambdas can be nested to any depth. A lambda body can call any scalar function, built-in or
user-defined, and those functions follow the same lifecycle as in any other query.
`CURRENT_WATERMARK`, `PROCTIME()` and the current time functions in a body are computed once per
row, so every element of a row sees the same values. Like the other captured values, they are
evaluated outside the lambda. The rest of the body is evaluated each time the function calls the
lambda, following the usual SQL rules for conditional evaluation: for example, only the matching
branch of a `CASE` is evaluated. Flink does not move a part that does not use the lambda's
parameters out of the per-element loop (see [Known limitations](#known-limitations)). A later
version may do so, but only for a part that is deterministic and cannot fail, so results and errors
stay the same.

A higher-order call is a regular scalar expression, so it works in `WHERE`, `GROUP BY`, `HAVING`,
`ORDER BY`, join conditions, computed columns, watermarks, inside aggregate arguments and in the
argument of `UNNEST` or `LATERAL TABLE`. It cannot be used in the clauses of `MATCH_RECOGNIZE` (see
[Known limitations](#known-limitations)).

### What `EXPLAIN` shows

`EXPLAIN` shows each lambda as written: the values that a body reads from the query appear in the
body, and the extra operands are left out. The compiled plan stores the lifted form, and the
planner compares plans by it.

| Query | `EXPLAIN` (optimized plan) |
|---|---|
| `ARRAY_TRANSFORM(a, x -> x + base)` | `ARRAY_TRANSFORM(a, x -> (x + base))` |
| `ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(b, y -> x + y + base))` | `ARRAY_TRANSFORM(a, x -> ARRAY_TRANSFORM(b, y -> ((x + y) + base)))` |
| `ARRAY_TRANSFORM(a, c -> c + t.c)` | `ARRAY_TRANSFORM(a, c_1 -> (c_1 + c))` |

A parameter that has the name of a value the body reads is shown renamed, with the next free
number, so that the column keeps its name. Spark and Trino avoid such clashes differently: Spark
prints every name with a unique ID (`lambda x#12`), and Trino gives every name in a plan a unique
name (`x_1`). In the Table API and PyFlink, `EXPLAIN` shows the generated parameter names (see
[Java and Python API](#java-and-python-api)).

### For function authors

A function accepts a lambda argument only if its input type strategy implements
`LambdaInputTypeStrategy`; there is no separate flag. In this FLIP, only built-in functions do. A
lambda passed to a user-defined function is rejected, and a user-defined function cannot use the
`FUNCTION` type in its signature. It can still be called in a body or around a higher-order call,
as in `ARRAY_TRANSFORM(my_udf(a), x -> my_other_udf(x))`.

- **Parameter types** come only from the arguments before the lambda, because the Table API
  resolves arguments from left to right. The lambda must declare exactly as many parameters as the
  strategy returns. A later version may relax both rules through new default methods, for example
  for an optional element index. SQL and the Table API call the same method.
- **Checking the lambda.** `CallContext#getLambdaArgument` returns a `LambdaInfo` with the
  parameter names and types and the body's type, for example to check that `ARRAY_FILTER`'s body
  returns `BOOLEAN`, or to compute `ARRAY<body type>`. `LambdaInfo#getBody()` is empty during type
  inference, which keeps type inference the same in SQL and the Table API. A lambda argument is
  never wrapped in an implicit `CAST`.

#### The lambda object contract

The rules for the function object that a function receives are part of the public Javadoc of
`LambdaInputTypeStrategy`. The most important ones:

| Rule | Meaning |
|---|---|
| Lifetime | The object is valid only during the `eval` call. It must not be stored, returned or serialized. Flink creates a new one for every call. |
| Threads | The function must apply the lambda on the thread that called `eval`, before `eval` returns. The object is not thread-safe. |
| Buffered results | With `FunctionDataN`, the next `apply` may overwrite the memory of the previous result. A function that keeps a result must copy it first. |
| Exceptions | An unchecked exception of the body propagates unchanged; a checked one is wrapped in a `FlinkRuntimeException`. The function lets it propagate. |

The Javadoc also covers identity, captured values, `NULL` arguments, nesting and the lifecycle.
Flink checks at planning time which functions may take a lambda and the number and types of its
parameters. It does not check the lifetime and thread rules, because that would cost time for
every element.

### Where errors are reported

Every error except a failure inside the body is raised before the job runs. Examples:

| Problem | Example | When | Message (abridged) |
|---|---|---|---|
| Function takes no lambda | `ARRAY_CONTAINS(a, x -> x)` | validation | `Invalid lambda expression at position 1. Function 'ARRAY_CONTAINS' does not accept a lambda expression at any position.` |
| Lambda passed to a user-defined function | `my_udf(a, x -> x)`, where `my_udf` declares a lambda argument | validation | `Invalid lambda expression at position 1. Lambda arguments are currently only supported by built-in functions, but 'my_udf' is a user-defined function.` |
| Wrong parameter count | `ARRAY_TRANSFORM(a, (x, y) -> x)` | validation | `The lambda expression at position 1 expects 1 parameter(s) but 2 were provided.` |
| Wrong body type | `ARRAY_FILTER(a, x -> x + 1)` | validation | `The lambda expression at position 1 must return BOOLEAN, but its body returns INT.` |
| Accumulator cannot be [widened](#accumulator-widening) | `ARRAY_REDUCE(a, 0, (acc, x) -> acc + x * CAST(0.5 AS DOUBLE))` | validation | `The reducer of ARRAY_REDUCE must return a type assignable to the accumulator type INT NOT NULL, but its body returns DOUBLE. … Cast the initial value to DOUBLE …` |
| `FUNCTION` as a column type | a `Schema` column `f` of type `FUNCTION(1)` | table creation | `Invalid data type 'FUNCTION(1)' for column 'f'. …` |
| `CURRENT_WATERMARK` on a parameter | `ARRAY_TRANSFORM(ARRAY[ts], x -> CURRENT_WATERMARK(x))` | validation | `CURRENT_WATERMARK over a lambda parameter is not supported in the body of a lambda expression. 'x' is a lambda parameter, …` |
| Damaged plan | a `LAMBDA` node with edited parameter indices | plan restore | `Invalid lambda expression in the compiled plan: …` |
| Body fails at runtime | `ARRAY_TRANSFORM(a, x -> my_throw(x))` | runtime | The job fails with the body's exception as the root cause. |

A wrong body type and an accumulator that cannot be widened are reported as the cause of the
general `Invalid function call` error, as for any argument that does not fit a function's type
inference.

Validation also rejects duplicate and reserved parameter names, aggregates, windows and sub-queries
over a parameter, table, asynchronous and Python functions in a body, a select-list alias in an
`ORDER BY` body, and lambdas in `MATCH_RECOGNIZE`. A lambda outside a function argument is a parse
error in SQL and a validation error in the Table API. `FUNCTION` is rejected as a result, state,
static argument or literal type. Each message names the cause.

<a id="raw-guard"></a>**A narrower check for `RAW` types.** While a nested lambda is validated, a
parameter that is not bound yet has the placeholder type `ANY`. To find a common type, for example
for the branches of a `CASE`, `FlinkTypeFactory.leastRestrictive` used to fail for any `ANY`. This
check was meant for Flink's legacy `RAW` type, which Calcite sees as `ANY`. It now fails only for a
legacy `RAW` type, and any other `ANY` is left to Calcite's default rules. This is the only change
that also affects queries without lambdas: an `ANY` type that Calcite creates itself no longer
fails with a misleading `RAW` error.

### Known limitations

| Limitation | Reason and workaround |
|---|---|
| A body cannot apply an aggregate, an `OVER` window or a sub-query to a parameter, as in `x -> SUM(x)`. | A parameter is a single element, not a set of rows, so such a call has no meaning. To fold an array, use `ARRAY_REDUCE`. Applied to a column of the query, these are allowed. |
| A body cannot call a table, asynchronous or Python function. | These functions do not return one value inside the generated Java code: a table function returns rows, and asynchronous and Python functions run in operators of their own. |
| In a sub-query inside a body, a name that matches a parameter means the parameter. | In `x -> x + (SELECT MAX(x) FROM u)`, `x` is the parameter even if `u` has a column `x`, so the query is rejected. Qualify the column (`u.x`) or rename the parameter. |
| In SQL, a body cannot be only a sub-query, as in `x -> (SELECT MAX(base) FROM u)` or `x -> EXISTS (SELECT 1 FROM u)`. | Calcite parses a lambda body as an expression that is not a query, so this is a parse error. Make the sub-query part of a larger expression, for example `COALESCE((SELECT MAX(base) FROM u), 0)` or `TRUE AND EXISTS (SELECT 1 FROM u)`. |
| In `ORDER BY`, a body cannot use a select-list alias of a different expression. | In `SELECT base * 2 AS base FROM t ORDER BY ARRAY_TRANSFORM(a, x -> x * base)[1]`, the meaning of `base` is unclear, so the query is rejected. Write the expression itself, or qualify the column (`t.base`). |
| A column merged by `USING` or `NATURAL JOIN` must be qualified in a body. | Flink turns such a column into `COALESCE(t1.base, t2.base)` only outside lambda bodies, so inside a body `base` is ambiguous. Write `t1.base` or `COALESCE(t1.base, t2.base)`. |
| Lambdas cannot be used in the clauses of `MATCH_RECOGNIZE` (`PARTITION BY`, `ORDER BY`, `MEASURES`, `DEFINE`). | Calcite does not set up lambda parameters there. Compute the value in the input table of `MATCH_RECOGNIZE`, for example in a sub-query, and use its column. |
| `ARRAY_REDUCE` widens its accumulator at most once, and only within the same type family. | Some folds need a `CAST`, for example an `INT` accumulator with a `DOUBLE` body; the error message says which. A `DECIMAL` sum with more than 6 digits after the decimal point is rejected unless its result is cast, because Flink's decimal addition then drops a digit. |
| In SQL, `FLOAT + DOUBLE` returns `FLOAT`; in the Table API, it returns `DOUBLE`. | This is existing Flink arithmetic, but it can give `ARRAY_REDUCE` a different accumulator type in SQL and in the Table API. `CAST(acc AS DOUBLE)` gives the same type in both. |
| A filter that contains a lambda is not pushed into a source. | The filter runs in the `Calc` operator instead. Partition pruning still works: Flink evaluates the condition against the listed partitions itself. |
| `CURRENT_WATERMARK` cannot be applied to a lambda parameter, as in `ARRAY_TRANSFORM(ARRAY[ts], x -> CURRENT_WATERMARK(x))`. | `CURRENT_WATERMARK` needs the context of the operator, which a body does not have. Applied to a column, it is evaluated once per row outside the body, so apply it to the column instead: `CURRENT_WATERMARK(ts)`. |
| A part of a body that does not use the parameters, such as `base * 2` in `x -> x + base * 2` or the constant `UPPER('abc')`, is evaluated each time the lambda is called. | Flink does not yet move such parts out of the per-element loop. Compute the value in a sub-query or CTE column and use that column in the body: `SELECT ARRAY_TRANSFORM(a, x -> x + b2) FROM (SELECT a, base * 2 AS b2 FROM t)`. |

### Calcite dependency

Flink stays on Calcite 1.41. Most of the lambda support is in Flink's own classes, such as its
lambda scope, validator and converter. The rest changes Calcite code that a subclass cannot reach.
It is patched into four Calcite classes that Flink already keeps copies of (`SqlValidatorImpl`,
`SqlToRelConverter`, `RexUtil`, `SqlCastFunction`), with about 430 added lines including comments.
The new error messages are added to `ExtraCalciteResource`, Flink's own interface in a Calcite
package (54 lines). No new Calcite class is copied, and decorrelation needs no patch. Each patch is
marked `FLINK MODIFICATION` and names the Calcite issue that makes it unnecessary, or says why it
stays.

- **CALCITE-7497 (Calcite 1.42)** keeps the rewritten body of a lambda when Calcite rewrites an
  expression. It removes the patches that work around this.
- **CALCITE-6242 (Calcite 1.43, unreleased)** lets a body read columns of the query and parameters
  of enclosing lambdas, and makes Calcite's visitors look inside lambda bodies. It removes most of
  the other patches.
- **Stricter rules in Calcite 1.43.** Calcite's main branch for 1.43 also rejects five things
  that Flink allows: a nested lambda parameter with the name of an enclosing one; sub-queries,
  aggregates and `OVER` windows in a lambda body; and a lambda in `ORDER BY` that reads anything
  other than its parameters. Flink allows sub-queries, aggregates and `OVER` windows in a body as
  long as they do not use a lambda parameter, and lets a lambda in `ORDER BY` read the columns of
  the `FROM` clause (see [What a lambda body can use](#what-a-lambda-body-can-use)). After the
  upgrade, Flink needs a patch for each rule, in code that it already changes for lambdas: the
  registration of the lambda scope and the `ORDER BY` expansion, which it patches, and
  `validateLambda`, which its validator overrides.
- **What stays** is specific to Flink: the name rules of its lambda scope, the numbering of nested
  parameters, converting a body against the query's aggregation, sub-queries and value lists such
  as `x IN (1, 2)`, keeping a capture of an outer query a correlation, accepting a cast of a
  parameter that is not bound yet, and the patches for the stricter rules above.
- **Why inner parameters may hide outer ones.** Flink keeps accepting them after the upgrade, for
  two reasons. First, it is the usual scoping rule: an inner name hides an outer one, as in Python
  and Scala lambdas, and as a column of a SQL sub-query hides a column of the same name in the
  outer query. A lambda can therefore be read on its own and copied into another lambda without
  renaming its parameters. Second, it keeps stored queries working: this FLIP ships on Calcite
  1.41, and views and materialized tables store their query as SQL text that is parsed again on
  every read. Adopting Calcite's rule at the upgrade would make such a query fail after a Flink
  upgrade, without any change by the user.

### Documentation

The user documentation, in English and Chinese, gets a new category "Higher-Order Functions" under
the built-in functions. Its page explains lambdas, and each of the four functions gets a page of
its own, with examples. The data types page and the PyFlink API reference are updated as well.

## Compatibility, Deprecation, and Migration Plan

The change is additive and nothing is deprecated.

1. **Reserved names.** An unqualified call to a catalog function named `ARRAY_TRANSFORM`,
   `ARRAY_FILTER`, `ARRAY_REDUCE` or `ARRAY_ZIP_WITH` now binds to the built-in. Any of three
   remedies works: rename the function, call it fully qualified, or register a temporary system
   function with that name.
2. **Narrowed `RAW` guard.** Only an `ANY` type that Calcite creates itself behaves differently,
   and no existing query reaches this case (see [A narrower check for `RAW` types](#raw-guard)).
3. **`LogicalTypeRoot.FUNCTION`.** Binary-compatible. On recompilation, a `switch` *expression*
   listing every constant without a `default` fails to compile, as it did when `DESCRIPTOR`,
   `VARIANT`, `BITMAP` and `UUID` were added. The SQL Gateway REST specs gain one enum value each.
4. **`FUNCTION` is a type-parser keyword.** A hand-written type string with an unquoted field or
   type name `function` (`ROW<function INT>`) now fails. Quote it: ``ROW<`function` INT>``.
   Persisted types are unaffected, because `asSerializableString()` already quotes.
5. **New default methods** (`LogicalTypeVisitor#visit(FunctionType)`,
   `CallContext#getLambdaArgument`) are source-compatible. Flink's own wrapping contexts delegate.
6. **`LocalReferenceExpression` promotion.** No member changes, and its constructor stays
   package-private. ArchUnit verifies that no `@PublicEvolving` API leaks an `@Internal` type.
7. **Compiled plans** gain `LAMBDA` and `LAMBDA_REF`. Existing plans are unaffected. A plan that
   contains a lambda cannot be restored by an older Flink version, which does not know these
   nodes.
8. **Opening lambdas to user-defined functions later** (layer 3, see
   [Future Work](#future-work)) only widens what is accepted, so it is compatible.

**Unchanged:**

- **Configuration.** No new option and no kill switch. A query without lambdas still passes through
  the capture-lifting passes, but they return it unchanged, so its plan and results stay the same.
  The narrowed `RAW` guard is the one change in behavior.
- **SQL parser.** `flink-sql-parser` is unchanged.
- **Catalog.** No API change, and no table schema can contain `FUNCTION`. Views and materialized
  tables store their query as SQL text, in which lambda parameters are not prefixed with a table
  name.
- **State and savepoints.** A lambda is never part of operator state, and no operator or
  `ExecNode` version changes, so existing savepoints restore as before.
- **REST and connectors.** No endpoint changes, and no result column can have the `FUNCTION` type.
  A connector never sees a lambda, because a filter that contains one is not pushed into a source.

## Test Plan

Tests check computed results in SQL and, where the same call can be written, in the Table API, in
batch and streaming mode.

- **Functions**: `NULL` and empty arrays, `NULL` elements, nested lambdas, parameters that hide a
  column or another parameter, and results that the body builds in a reused buffer.
- **Captures**: columns, parameters of enclosing lambdas, aggregates, `OVER` windows, sub-queries,
  `CURRENT_WATERMARK`, `PROCTIME()` and the current time functions in a body, and a body that
  returns a time attribute. Lambdas are also tested in each query construct with its own planning
  or code generation: joins (including lookup, delta, temporal, interval and multi-way joins),
  aggregations, process table functions, sub-queries, `UNNEST`, computed columns, watermarks and
  source push-down.
- **Built-in functions in a body**: about 90 calls of built-in functions, from string, JSON and date
  functions to array functions with evaluators of their own and comparisons with a list of values,
  give the same result in a body as outside a lambda.
- **Errors**: every validation, planning and restore error that this FLIP adds, and every rejection
  of the `FUNCTION` type. A call that can be written in both SQL and the Table API is checked in
  both.
- **Compiled plans and `EXPLAIN`**: six restore tests (one per array function, one with a capture
  and one with nested lambdas) in batch and streaming mode, the rejection of damaged plans, and
  plan tests that pin the lifted form and show each lambda as written in `EXPLAIN`.
- **Type system and determinism**: `FUNCTION(n)` is read back from its string and gets the right
  Java class; a non-deterministic body makes the call non-deterministic wherever Flink checks it.
- **Completeness**: each built-in function that takes a lambda must have a runtime class, tests, a
  Table API method and documentation. The tests find these functions from their definitions.
- **Runtime and PyFlink**: large bodies are split into smaller methods, functions called in a body
  report their metrics, and PyFlink is tested end to end.

## Rejected Alternatives

### First-class function values

**Goal:** make a lambda a value of its own, like a number or a string. It could then be stored in a
column, returned from a function, cast or kept in state, and be defined once and used in several
places.

**Why it does not work:** a value that is stored or sent needs a serializer with a stable format,
state that stays compatible across Flink versions, and a rule for when two functions are equal.
This is a large design of its own, and none of the proposed higher-order functions needs it.

**Instead:** a lambda can only be an argument of a higher-order function, and any other use is
rejected during validation. Allowing more uses later would not break existing queries.

### A planner-internal lambda type

**Goal:** keep the lambda type inside the planner, like `FunctionRelDataType`, the type that
Calcite sees for a lambda. The public API would not change, and the type could be changed freely in
later releases.

**Why it does not work:** code outside the planner needs the type. The type strategies of the array
functions find the lambda argument by its `FUNCTION` type; a function receives a lambda as a
`FunctionN` or `FunctionDataN` object, which Flink picks from the argument's type; and in the
Table API, a lambda has a type like any other expression. Lambda arguments for user-defined
functions would need a public type as well.

**Instead:** this FLIP adds a public `FUNCTION(n)` type and keeps it small. It records only how many
parameters a lambda has, and Flink rejects it wherever a lambda would be stored, so a lambda never
outlives the call it is passed to.

### A full signature in the type: `FUNCTION<(T…) -> R>`

**Goal:** describe the whole lambda in its type, for example `FUNCTION<(INT) -> INT>`, the way
`ARRAY<INT>` describes its elements.

**Why it does not work:** nothing needs the signature in the type, and it makes the public type
larger. The function decides the parameter types itself, so until it does, the signature would hold
a placeholder. The built-in strategies read the result type from the lambda itself. The type parser
would need a `->` token and rules for when two signatures are the same type, and all of this would
have to stay compatible.

**Instead:** `FUNCTION(n)` holds only the number of parameters, like the existing helper types
`DESCRIPTOR` and `SYMBOL`, which carry no inner types either. A function gets the parameter and
result types through `CallContext#getLambdaArgument`.

### A `LAMBDA` argument trait

**Goal:** mark a lambda argument with a new trait, `StaticArgumentTrait.LAMBDA`, instead of giving
it a type, as a `MODEL` argument is declared. Flink would then not need the public `FUNCTION` type.

**Why it does not work:** a trait does not remove the need for a type. During type inference, every
argument has a data type, and a Table API lambda expression has a type like any other expression.
Traits also only exist on static arguments, but the built-in functions declare their lambda
arguments in an input type strategy.

**Instead:** a lambda argument has the type `FUNCTION(n)`, and Flink picks the Java class that the
function receives from it.

### Other alternatives

| Alternative | Why it was not taken |
|---|---|
| Parameter types written by the user, as in `(x INT) -> x + 1` | Users would repeat types that Flink can derive, and Flink's parser would change. The function would still have to check them. None of the compared engines requires them. |
| Keeping captured columns in the body until code generation | On Calcite 1.41, the optimizer neither sees the columns a body uses nor keeps its own changes to a body, so it could drop a captured column or read the wrong one without an error. |
| An `isCapture` flag on `RexLambdaRef` instead of a number range | Calcite compares parameter references by index and type and merges lambdas that compare as equal. A flag would either be ignored or require changing how a Calcite class defines equality. |
| Loops inlined by the planner instead of a function object | Each function would need its own operator, operand checker, Table API mapping and code generation, for a small gain: the inlined loop boxed every element too. |
| Other Java interfaces: the JDK's `Function` and `BiFunction`, one family for both data formats, or up to 22 parameters | Mixed names for one concept; the planner picks the data format from the Java class alone, so one family could not tell the formats apart; and 46 interfaces instead of 10, for functions that do not exist. |
| `Function3` and `Function4` extending Flink's `TriFunction` and `QuadFunction` | `Function0` to `Function2` extend the JDK's interfaces so that a received lambda can be passed to any Java API. Flink's `TriFunction` and `QuadFunction` give no such benefit, because no library outside Flink accepts them. Extending them would mix JDK and Flink interfaces in one family and tie the lambda API to general utilities that change for other reasons. |
| Waiting for Calcite 1.43 or upstream fixes | Not all patches go away: Calcite 1.43 rejects sub-queries, aggregates and `OVER` windows in a lambda body, which this FLIP supports (see [Calcite dependency](#calcite-dependency)). The feature would also wait for a Calcite release, a Flink-wide upgrade and new Calcite extension points, for an unknown time. |
| A configuration kill switch | A query without lambdas keeps its plan and results: the capture-lifting passes visit it but change nothing, and the narrowed `RAW` guard only affects an `ANY` type that Calcite creates itself. A switch would therefore mainly turn lambdas off, which not writing a lambda already does, and it would add a second, less-tested path through the validator and the planner. |
| Bare names: `TRANSFORM`, `FILTER`, `REDUCE` | `TRANSFORM` clashes with the script `TRANSFORM` of the Hive dialect, and `FILTER` with the aggregate `FILTER (WHERE …)` clause. The `ARRAY_` prefix matches Flink's existing array functions. |
| Other `ARRAY_REDUCE` typing rules | Always the initial type rejects `''` with string concatenation; always a large type changes the result type of folds that need no widening; widening after every step never ends for strings; widening across families makes exact values approximate; Calcite's own rule disagrees with the Table API. |

## Future Work

As suggested on the dev mailing list
([discussion](https://lists.apache.org/thread/9c0cst85mh3tyq0h4sochyd5nb86opry)), the combined
proposal is split into three layers. This FLIP is layer 1, the lambda foundation. Layers 2 and 3
are follow-up FLIPs. They build on layer 1 and do not depend on each other, each can be released
on its own, and both only add to what this FLIP makes public.

- **Layer 2: more built-in functions.** The map functions `MAP_FILTER`, `MAP_TRANSFORM_KEYS`,
  `MAP_TRANSFORM_VALUES` and `MAP_ZIP_WITH` (the first with a three-parameter lambda), the
  quantifiers `ARRAY_EXISTS` and `ARRAY_FORALL`, and a comparator for `ARRAY_SORT`. They use this
  foundation unchanged; their names and semantics are decided there. Optional arguments for the
  four functions of this FLIP, such as a finishing lambda for `ARRAY_REDUCE` (as in Spark's
  `aggregate` and Trino's `reduce`) or an element index as a second parameter (as in Spark's
  `transform` and `filter`), can be added there without changing existing calls.
- **Layer 3: lambda arguments for user-defined functions.** Users declare lambda arguments on their
  own scalar, table and process table functions, with their own `LambdaInputTypeStrategy`, and
  receive `FunctionN` or `FunctionDataN` objects. This removes the restriction to built-in
  functions, accepts a top-level `FUNCTION` static argument, and may add default methods to
  `LambdaInputTypeStrategy`, for example to pass the number of parameters the user wrote for an
  optional element index. It also decides what a process table function's lambda may do.
- **Lambdas in `MATCH_RECOGNIZE`.** This needs lambda parameters in Calcite's validation of
  `MATCH_RECOGNIZE` and capture lifting for pattern references. Since such a lambda is rejected
  today, adding this later does not break existing queries.
- **Optimizations** that change no public interface need no FLIP and are handled in separate
  tickets.

## References

- JIRA: [FLINK-31207](https://issues.apache.org/jira/browse/FLINK-31207)
- Reference implementation:
  [swisscom-bigdata/flink](https://github.com/swisscom-bigdata/flink/tree/flink-31207-higher-order-functions-and-lambdas-layer-flip1)
- Calcite: [CALCITE-3679](https://issues.apache.org/jira/browse/CALCITE-3679) (lambda syntax),
  [CALCITE-7497](https://issues.apache.org/jira/browse/CALCITE-7497) (1.42.0),
  [CALCITE-6242](https://issues.apache.org/jira/browse/CALCITE-6242) (1.43.0, unreleased)
- [FLIP-265: Deprecate and remove Scala API support](https://cwiki.apache.org/confluence/display/FLINK/FLIP-265+Deprecate+and+remove+Scala+API+support)
