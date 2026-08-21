---
title: "Higher-Order"
weight: 10
type: docs
sqlFunctions: higherorder
---
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

# Higher-Order Functions

Higher-order functions take a *lambda expression* as an argument and apply it to the elements or
entries of an `ARRAY` or `MAP`.

A lambda is an anonymous function written `parameter -> body` (or `(parameter1, parameter2) -> body`
for more than one parameter). Its parameter types are derived from the collection the function is
applied to, so they are never declared:

```sql
SELECT ARRAY_TRANSFORM(vals, x -> x * 10) FROM t;   -- x is bound to the element type of vals
```

The parameters of one lambda must have unique names. A nested lambda may reuse the name of an
enclosing lambda's parameter, which then shadows it.

The fields of a `ROW`-typed parameter are accessed with the usual dot notation. A parameter also
shadows a table alias of the same name, so `o.amount` below is always the field of the element:

```sql
SELECT ARRAY_TRANSFORM(o.items, o -> o.amount * o.price) FROM orders AS o;
```

A lambda body is an ordinary expression: besides its own parameters it may reference (*capture*) any
column of the enclosing query, and it may call built-in or user-defined functions, including other
higher-order functions. Nested lambdas can capture the parameters of the enclosing lambdas:

```sql
-- capture the column `base`
SELECT ARRAY_TRANSFORM(vals, x -> x + base) FROM t;

-- nested: the inner lambda captures `a`, the parameter of the outer lambda
SELECT ARRAY_TRANSFORM(nested_vals, a -> ARRAY_FILTER(a, x -> x > a[1])) FROM t;
```

A body may capture any value the enclosing query produces for the current row, including an
aggregate, an `OVER` window or a sub-query — each is evaluated once per row or group, and the body
sees its result:

```sql
SELECT ARRAY_TRANSFORM(vals, x -> x + SUM(base)) FROM t GROUP BY vals;
SELECT ARRAY_TRANSFORM(vals, x -> x + SUM(base) OVER ()) FROM t;
SELECT ARRAY_TRANSFORM(vals, x -> x + (SELECT MAX(base) FROM t)) FROM t;
```

Those constructs may not be evaluated over a lambda *parameter*, however: a parameter is a single
element, not a set of rows, so `ARRAY_TRANSFORM(vals, x -> SUM(x))` is a validation error. Use
`ARRAY_REDUCE` to fold an array's own elements. A table function cannot appear in a body either,
since it produces rows rather than a value, nor can an asynchronous scalar function, whose result
only arrives after the body has been evaluated. The Table API follows the same rules (it has no
sub-query expression).

Lambdas are only valid as arguments of higher-order functions; using one anywhere else is a
validation error. They are not values: a lambda cannot be stored in a column, assigned to a
variable, or returned from a query.

In the Table API the same functions are available as expression methods that take a host-language
lambda. It receives the expression(s) for the lambda parameter(s) and returns the body expression:

{{< tabs "higher-order-functions" >}}
{{< tab "Java" >}}
```java
table.select(
    $("vals").arrayTransform(x -> x.times(10)),
    $("vals").arrayFilter(x -> x.isGreater(1)),
    $("vals").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
    // a lambda body may capture other columns
    $("vals").arrayTransform(x -> x.plus($("base"))));
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table.select(
    col("vals").array_transform(lambda x: x * 10),
    col("vals").array_filter(lambda x: x > 1),
    col("vals").array_reduce(lit(0), lambda acc, x: acc + x),
    # a lambda body may capture other columns
    col("vals").array_transform(lambda x: x + col("base")))
```
{{< /tab >}}
{{< /tabs >}}

{{< sql_functions "higherorder" >}}
