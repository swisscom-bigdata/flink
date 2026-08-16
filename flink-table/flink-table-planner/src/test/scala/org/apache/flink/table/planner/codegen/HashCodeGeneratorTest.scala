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
package org.apache.flink.table.planner.codegen

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.data.{GenericArrayData, GenericMapData, GenericRowData, StringData}
import org.apache.flink.table.types.logical.{ArrayType, BigIntType, DoubleType, FloatType, IntType, MapType, MultisetType, RowType, VarBinaryType, VarCharType}

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.Test

import scala.collection.JavaConversions.mapAsJavaMap

/** Test for [[HashCodeGenerator]]. */
class HashCodeGeneratorTest {

  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testRowHash(): Unit = {
    val hashFunc1 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new IntType(), new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)),
        "name",
        Array(1, 0)
      )
      .newInstance(classLoader)

    val hashFunc2 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new IntType(), new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)),
        "name",
        Array(1, 2, 0)
      )
      .newInstance(classLoader)

    val row = GenericRowData.of(ji(5), jl(8), Array[Byte](1, 5, 6))
    assertEquals(637, hashFunc1.hashCode(row))
    assertEquals(136516167, hashFunc2.hashCode(row))

    // test row with nested array and map type
    val hashFunc3 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(
          new IntType(),
          new ArrayType(new IntType()),
          new MultisetType(new IntType()),
          new MapType(new IntType(), new VarCharType())),
        "name",
        Array(1, 2, 0, 3)
      )
      .newInstance(classLoader)

    val row3 = GenericRowData.of(
      ji(5),
      new GenericArrayData(Array(1, 5, 7)),
      new GenericMapData(Map(1 -> null, 5 -> null, 10 -> null)),
      new GenericMapData(
        Map(1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny"), 10 -> null))
    )
    assertEquals(1356875190, hashFunc3.hashCode(row3))
  }

  @Test
  def testArrayHash(): Unit = {
    // test primitive type
    val hashFunc1 = HashCodeGenerator
      .generateArrayHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        new IntType(),
        "name")
      .newInstance(classLoader)

    val array1 = new GenericArrayData(Array(1, 5, 7))
    val array2 = new GenericArrayData(Array(1, 5, 7))
    val array3 = new GenericArrayData(Array[AnyRef](null, ji(1), ji(5), null, ji(7)))
    assertEquals(hashFunc1.hashCode(array1), hashFunc1.hashCode(array2))
    assertNotEquals(hashFunc1.hashCode(array1), hashFunc1.hashCode(array3))

    // test complex map type of element
    val hashFunc2 = HashCodeGenerator
      .generateArrayHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        new MapType(new IntType(), new VarCharType()),
        "name")
      .newInstance(classLoader)

    val map1 = new GenericMapData(
      Map(1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny"), 10 -> null))
    val map2 = new GenericMapData(
      Map(2 -> StringData.fromString("jark"), 7 -> StringData.fromString("leonard")))
    val array4 = new GenericArrayData(Array[AnyRef](map1, map2))
    val array5 = new GenericArrayData(Array[AnyRef](map1, map2))
    val array6 = new GenericArrayData(Array[AnyRef](map2, map1))
    assertEquals(hashFunc2.hashCode(array4), hashFunc2.hashCode(array5))
    assertNotEquals(hashFunc2.hashCode(array4), hashFunc2.hashCode(array6))

    // test complex row type of element
    val hashFunc3 = HashCodeGenerator
      .generateArrayHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new IntType(), new BigIntType()),
        "name")
      .newInstance(classLoader)

    val row1 = GenericRowData.of(ji(5), jl(8))
    val row2 = GenericRowData.of(ji(25), jl(52))
    val array7 = new GenericArrayData(Array[AnyRef](row1, row2))
    val array8 = new GenericArrayData(Array[AnyRef](row1, row2))
    val array9 = new GenericArrayData(Array[AnyRef](null, row2, row1))
    assertEquals(hashFunc3.hashCode(array7), hashFunc3.hashCode(array8))
    assertNotEquals(hashFunc3.hashCode(array7), hashFunc3.hashCode(array9))
  }

  @Test
  def testMapHash(): Unit = {
    // test primitive type
    val hashFunc1 = HashCodeGenerator
      .generateMapHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        new IntType(),
        new VarCharType(),
        "name")
      .newInstance(classLoader)

    val map1 = new GenericMapData(
      Map(1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny"), 10 -> null))
    val map2 = new GenericMapData(
      Map(10 -> null, 1 -> StringData.fromString("ron"), 5 -> StringData.fromString("danny")))
    val map3 = new GenericMapData(
      Map(7 -> null, 2 -> StringData.fromString("ron"), 3 -> StringData.fromString("danny")))
    assertEquals(hashFunc1.hashCode(map1), hashFunc1.hashCode(map2))
    assertNotEquals(hashFunc1.hashCode(map1), hashFunc1.hashCode(map3))

    // test complex row type of value
    val hashFunc2 = HashCodeGenerator
      .generateMapHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        new IntType(),
        RowType.of(new IntType(), new BigIntType()),
        "name")
      .newInstance(classLoader)

    val map4 = new GenericMapData(
      Map(1 -> GenericRowData.of(ji(5), jl(8)), 5 -> GenericRowData.of(ji(54), jl(78)), 10 -> null))
    val map5 = new GenericMapData(
      Map(7 -> GenericRowData.of(ji(5), jl(8)), 5 -> GenericRowData.of(ji(54), jl(78)), 10 -> null))
    assertNotEquals(hashFunc2.hashCode(map4), hashFunc2.hashCode(map5))

    // test complex array type of value
    val hashFunc3 = HashCodeGenerator
      .generateMapHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        new IntType(),
        new ArrayType(new IntType()),
        "name")
      .newInstance(classLoader)

    val map6 = new GenericMapData(
      Map(
        1 -> new GenericArrayData(Array(1, 5, 7)),
        5 -> new GenericArrayData(Array(2, 4, 8)),
        10 -> null))
    val map7 = new GenericMapData(
      Map(
        1 -> new GenericArrayData(Array(1, 5, 7)),
        10 -> null,
        5 -> new GenericArrayData(Array(2, 4, 8))))
    assertEquals(hashFunc3.hashCode(map6), hashFunc3.hashCode(map7))
  }

  @Test
  def testHashWithIndependentNameCounter(): Unit = {
    val hashFunctionCode1 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new IntType(), new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)),
        "name",
        Array(1, 0)
      )
      .getCode

    val hashFunctionCode2 = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new IntType(), new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)),
        "name",
        Array(1, 0)
      )
      .getCode

    assertEquals(hashFunctionCode1, hashFunctionCode2)
  }

  @Test
  def testFloatingSignedZeroHash(): Unit = {
    // 0.0 and -0.0 are equal under SQL equality (primitive ==), so their generated logical hashes
    // must agree even though Float/Double#hashCode differ for the two signed zeros. See the FLOAT
    // and DOUBLE cases in CodeGenUtils#hashCodeForType.
    val floatHash = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new FloatType()),
        "name",
        Array(0))
      .newInstance(classLoader)
    assertEquals(
      floatHash.hashCode(GenericRowData.of(jf(0.0f))),
      floatHash.hashCode(GenericRowData.of(jf(-0.0f))))

    val doubleHash = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new DoubleType()),
        "name",
        Array(0))
      .newInstance(classLoader)
    assertEquals(
      doubleHash.hashCode(GenericRowData.of(jd(0.0d))),
      doubleHash.hashCode(GenericRowData.of(jd(-0.0d))))

    // the same normalization must apply to a signed-zero leaf nested inside an array ...
    val arrayHash = HashCodeGenerator
      .generateArrayHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        new DoubleType(),
        "name")
      .newInstance(classLoader)
    assertEquals(
      arrayHash.hashCode(new GenericArrayData(Array(0.0d, 1.0d))),
      arrayHash.hashCode(new GenericArrayData(Array(-0.0d, 1.0d))))

    // ... and inside a row
    val rowHash = HashCodeGenerator
      .generateRowHash(
        new CodeGeneratorContext(new Configuration, classLoader),
        RowType.of(new DoubleType(), new IntType()),
        "name",
        Array(0, 1))
      .newInstance(classLoader)
    assertEquals(
      rowHash.hashCode(GenericRowData.of(jd(0.0d), ji(7))),
      rowHash.hashCode(GenericRowData.of(jd(-0.0d), ji(7))))

    // NaN: SQL equality treats NaN as unequal to itself (primitive ==), so equal hashes are not
    // required. This pins the current contract: distinct NaN bit patterns still hash identically
    // because Double#hashCode canonicalizes NaN via doubleToLongBits.
    val nonCanonicalNaN = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
    assertEquals(
      doubleHash.hashCode(GenericRowData.of(jd(Double.NaN))),
      doubleHash.hashCode(GenericRowData.of(jd(nonCanonicalNaN))))
  }

  def ji(i: Int): Integer = {
    new Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }

  def jf(f: Float): java.lang.Float = {
    java.lang.Float.valueOf(f)
  }

  def jd(d: Double): java.lang.Double = {
    java.lang.Double.valueOf(d)
  }
}
