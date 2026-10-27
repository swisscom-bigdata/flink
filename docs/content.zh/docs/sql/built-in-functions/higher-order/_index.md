---
title: "高阶"
weight: 10
type: docs
bookCollapseSection: true
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

# 高阶函数

高阶函数（higher-order function）接收一个 *lambda 表达式* 作为参数，并把它应用到 `ARRAY` 的元素上。

lambda 是一个匿名函数，写作 `parameter -> body`（多个参数时写作 `(parameter1, parameter2) -> body`）。
它的参数类型由所应用的集合推导得出，因此无需声明：

```sql
SELECT ARRAY_TRANSFORM(vals, x -> x * 10) FROM t;   -- x 绑定到 vals 的元素类型
```

同一个 lambda 的参数名必须唯一。嵌套的 lambda 可以复用外层 lambda 的参数名，此时会遮蔽外层参数。

`ROW` 类型参数的字段通过常规的点号访问。参数同样会遮蔽同名的表别名，因此下例中的 `o.amount` 始终是元素的字段：

```sql
SELECT ARRAY_TRANSFORM(o.items, o -> o.amount * o.price) FROM orders AS o;
```

lambda 的函数体是一个普通表达式：除了自己的参数以外，它还可以引用（*捕获*）外层查询的任意列，也可以调用
内置函数或自定义函数，包括其它高阶函数。嵌套的 lambda 可以捕获外层 lambda 的参数：

```sql
-- 捕获列 `base`
SELECT ARRAY_TRANSFORM(vals, x -> x + base) FROM t;

-- 嵌套：内层 lambda 捕获了外层 lambda 的参数 `a`
SELECT ARRAY_TRANSFORM(nested_vals, a -> ARRAY_FILTER(a, x -> x > a[1])) FROM t;
```

函数体可以捕获外层查询为当前行产生的任意值，包括聚合函数、`OVER` 窗口和子查询——它们各自按行或按组
计算一次，函数体看到的是其结果：

```sql
SELECT ARRAY_TRANSFORM(vals, x -> x + SUM(base)) FROM t GROUP BY vals;
SELECT ARRAY_TRANSFORM(vals, x -> x + SUM(base) OVER ()) FROM t;
SELECT ARRAY_TRANSFORM(vals, x -> x + (SELECT MAX(base) FROM t)) FROM t;
```

函数体中的 `CURRENT_WATERMARK`、`PROCTIME()` 以及 `CURRENT_TIMESTAMP` 等当前时间函数同样按行计算一次，
因此同一行的所有元素看到相同的值。`CURRENT_WATERMARK` 必须作用于查询的列，而不能作用于 lambda 参数。

但这些构造不能作用于 lambda 的*参数*：参数是单个元素而不是一组行，因此
`ARRAY_TRANSFORM(vals, x -> SUM(x))` 会导致校验错误。若要折叠数组自身的元素，请使用 `ARRAY_REDUCE`。
在函数体的子查询中，与 lambda 参数同名的非限定名称始终引用该参数，即使子查询自身有同名列也是如此；
请用表别名限定子查询自身的列（例如 `q.x`），或重命名参数。
表函数同样不能出现在函数体中，因为它产生的是行而不是值；异步标量函数也不能，因为它的结果在函数体求值完成之后才会返回。
Table API 遵循相同的规则（它没有子查询表达式）。

在 `ORDER BY` 中，函数体看到的是 `FROM` 子句的列，而不是 select 列表的别名。如果函数体中的某个名称是
select 列表中的别名，且该别名代表的不是同名的列本身，则会导致校验错误；请改写为别名对应的表达式，
或用表别名限定该列（例如 `t.base`）。

由 `USING` 或 `NATURAL` 连接合并的列在函数体中必须加以限定，即使函数体位于 select 列表中也是如此（在 select
列表中、函数体之外，该列可以不加限定）。不加限定时会报告该列有歧义。请使用所需那一侧的列（例如 `t1.base`），`FULL` 连接后则使用
`COALESCE(t1.base, t2.base)`。

`MATCH_RECOGNIZE` 的子句（`PARTITION BY`、`ORDER BY`、`MEASURES` 和 `DEFINE`）中不支持 lambda。
请改为在 `MATCH_RECOGNIZE` 的输入表中计算该值（例如在子查询中），并在这些子句中使用对应的列。

lambda 只能作为高阶函数的参数使用。在 SQL 中，lambda 出现在函数参数以外的位置会导致解析错误；
传给在该参数位置不接受 lambda 的函数则会导致校验错误。在 Table API 中，这两种情况都会导致校验错误。
lambda 不是值：它不能存储在列中，不能赋值给变量，也不能作为查询结果返回。

`EXPLAIN` 按书写形式显示 lambda，函数体中直接显示它从查询中读取的值。如果某个参数与这样的值同名
（例如 `c -> c + t.c` 中的参数 `c` 和列 `c`），计划会将该参数重命名后显示：
`ARRAY_TRANSFORM(a, c_1 -> (c_1 + c))`。

在 Table API 中，这些函数以表达式方法的形式提供，接收宿主语言的 lambda。该 lambda 接收 lambda 参数对应的
表达式，并返回函数体表达式：

{{< tabs "higher-order-functions" >}}
{{< tab "Java" >}}
```java
table.select(
    $("vals").arrayTransform(x -> x.times(10)),
    $("vals").arrayFilter(x -> x.isGreater(1)),
    $("vals").arrayReduce(lit(0), (acc, x) -> acc.plus(x)),
    // lambda 函数体可以捕获其它列
    $("vals").arrayTransform(x -> x.plus($("base"))));
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table.select(
    col("vals").array_transform(lambda x: x * 10),
    col("vals").array_filter(lambda x: x > 1),
    col("vals").array_reduce(lit(0), lambda acc, x: acc + x),
    # lambda 函数体可以捕获其它列
    col("vals").array_transform(lambda x: x + col("base")))
```
{{< /tab >}}
{{< /tabs >}}

{{< sql_functions_zh "higherorder" >}}
