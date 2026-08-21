################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import unittest

from pyflink.common import Row
from pyflink.table.expressions import col, lit
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkBatchTableTestCase, \
    PyFlinkStreamTableTestCase


class HigherOrderFunctionTests(object):
    """
    End-to-end execution tests for the higher-order function Table API DSL when driven from Python.

    Unlike ``test_expression.py`` (which only checks the rendered expression string), these tests
    actually execute the resulting job so that the Python lambda adapters (``_LambdaFunction1``,
    ``_LambdaFunction2``, ``_LambdaFunction3``), the outer-column capture lifting and the
    input-type validation are all exercised through a real ``TableEnvironment``.

    Results are collected as rows (rather than via ``to_pandas``) so that the assertions do not
    depend on Arrow's pandas conversion, which cannot represent a nullable map key.
    """

    def _array_source(self):
        return self.t_env.from_elements(
            [([1, 2, 3],), ([4, 5],)],
            DataTypes.ROW([DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT()))]))

    def _collect(self, table):
        with table.execute().collect() as results:
            return [row for row in results]

    def _assert_rows(self, table, expected):
        # Compare on the string representation so that rows carrying arrays or maps (which are not
        # orderable) can still be matched irrespective of the collection order.
        actual = self._collect(table)
        self.assertEqual(sorted(map(str, actual)), sorted(map(str, expected)))

    def test_array_transform_unary_lambda(self):
        # ARRAY_TRANSFORM with a single-parameter lambda (java.util.function.Function adapter).
        table = self._array_source() \
            .select(col("a").array_transform(lambda x: x + 1).alias("b"))
        self._assert_rows(table, [Row([2, 3, 4]), Row([5, 6])])

    def test_array_reduce_binary_lambda(self):
        # ARRAY_REDUCE with a two-parameter lambda (java.util.function.BiFunction adapter). The
        # accumulator type is the type of the initial value (INT), so the result is INT too.
        table = self._array_source() \
            .select(col("a").array_reduce(lit(0), lambda acc, x: acc + x).alias("b"))
        self._assert_rows(table, [Row(6), Row(9)])

    def test_map_zip_with_ternary_lambda(self):
        # MAP_ZIP_WITH with a three-parameter lambda (org.apache.flink.util.function.TriFunction
        # adapter).
        t = self.t_env.from_elements(
            [({1: 10, 2: 20}, {1: 1, 2: 2},)],
            DataTypes.ROW([
                DataTypes.FIELD("a", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                DataTypes.FIELD("b", DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))]))
        table = t \
            .select(col("a").map_zip_with(col("b"), lambda k, v1, v2: v1 + v2).alias("c"))
        self._assert_rows(table, [Row({1: 11, 2: 22})])

    def test_lambda_captures_outer_column(self):
        # The lambda body references the outer column ``factor``, which must be lifted into a
        # trailing call operand so column trimming keeps it (see HigherOrderFunctionUtil).
        t = self.t_env.from_elements(
            [([1, 2, 3], 10), ([4, 5], 100)],
            DataTypes.ROW([
                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD("factor", DataTypes.INT())]))
        table = t \
            .select(col("a").array_transform(lambda x: x + col("factor")).alias("b"))
        self._assert_rows(table, [Row([11, 12, 13]), Row([104, 105])])

    def test_map_zip_with_disjoint_keys(self):
        # Disjoint key sets: the result is the union of both key sets and every entry sees a NULL on
        # exactly one side, so each map contributes its own value untouched.
        t = self.t_env.from_elements(
            [({1: 10, 2: 20}, {3: 30, 4: 40},)],
            DataTypes.ROW([
                DataTypes.FIELD("a", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                DataTypes.FIELD("b", DataTypes.MAP(DataTypes.INT(), DataTypes.INT()))]))
        table = t \
            .select(col("a").map_zip_with(col("b"), lambda k, v1, v2: v1.if_null(v2)).alias("c"))
        self._assert_rows(table, [Row({1: 10, 2: 20, 3: 30, 4: 40})])

    def test_nested_lambdas(self):
        # A lambda nested in another lambda, whose body captures the enclosing lambda's parameter.
        # The capture is lifted out of the inner lambda and rebound per iteration of the outer one,
        # which the Python lambda adapters have to survive as well.
        t = self.t_env.from_elements(
            [([[1, 2], [3]],)],
            DataTypes.ROW([
                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))]))
        table = t.select(
            col("a").array_transform(
                lambda inner: inner.array_transform(lambda x: x + inner.at(1))).alias("b"))
        self._assert_rows(table, [Row([[2, 3], [6]])])

    def test_array_filter_rejects_non_boolean_predicate(self):
        # The filter predicate must return BOOLEAN; returning INT fails input-type validation during
        # expression resolution, surfaced to Python as an exception whose message names the
        # requirement.
        with self.assertRaises(Exception) as context:
            self._array_source() \
                .select(col("a").array_filter(lambda x: x + 1).alias("b"))
        self.assertIn("must return BOOLEAN", str(context.exception))


class BatchHigherOrderFunctionITTests(HigherOrderFunctionTests, PyFlinkBatchTableTestCase):
    pass


class StreamHigherOrderFunctionITTests(HigherOrderFunctionTests, PyFlinkStreamTableTestCase):
    pass


if __name__ == '__main__':
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
