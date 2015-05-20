package com.xpatterns.jaws.data.utils

import org.apache.spark.sql.catalyst.types.{ DataType, StructField, StructType }
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.types.StructField
import scala.util.Try
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import collection.JavaConversions._
import scala.collection.convert.Wrappers

@RunWith(classOf[JUnitRunner])
class AvroConverterTest extends FunSuite {
  val intField = new StructField("int", DataType("IntegerType"), false)
  val strField = new StructField("str", DataType("StringType"), true)
  val arrOfStrField = new StructField("arr", DataType("ArrayType(StringType,false)"), true)
  val arrOfStrFieldNotNullable = new StructField("arr", DataType("ArrayType(StringType,false)"), false)
  val arrOfRecField = new StructField("arrOfRec", DataType("ArrayType(StructType(List(StructField(str,StringType,true))),false)"), true)
  val mapOfStrField = new StructField("map", DataType("MapType(StringType,StringType,true)"), false)
  val mapOfRecField = new StructField("mapOfRec", DataType("MapType(StringType,StructType(List(StructField(str,StringType,true))),true)"), false)

  val structType = new StructType(Seq(intField, strField))
  val structTypeRow = Array(Row.fromSeq(Seq(1, "a")), Row.fromSeq(Seq(2, "b")))

  val recordType = new StructField("record", structType, false)
  val byteField = new StructField("byte", DataType("ByteType"), false)
  val binaryField = new StructField("binary", DataType("BinaryType"), true)
  val arrOfArrString = new StructField("arrOfArrayString", DataType("ArrayType(ArrayType(StringType,false),false)"), true)
  val mapOfMap = new StructField("mapOfMap", DataType("MapType(StringType, MapType(StringType,StringType,true),true)"), false)
  val arrOfMapofArr = new StructField("arrOfMapOfArr", DataType("ArrayType(MapType(StringType, ArrayType(ArrayType(StringType,false),false),true),false)"), true)
  val decimalField = new StructField("decimal", DataType("DecimalType"), true)

  test("simple schema") {
    val result = AvroConverter.getAvroSchema(structType)
    assert(result.toString() == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"str\",\"type\":[\"string\",\"null\"]}]}")

  }

  test("schema with record") {
    val complexStructType = new StructType(Seq(strField, recordType))
    val result = AvroConverter.getAvroSchema(complexStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"str\",\"type\":[\"string\",\"null\"]}," +
      "{\"name\":\"record\",\"type\":{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"str\",\"type\":[\"string\",\"null\"]}]}}]}")
  }

  test("schema with list of strings") {
    val arrStructType = new StructType(Seq(intField, arrOfStrField))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"arr\",\"type\":[{\"type\":\"array\",\"items\":\"string\"},\"null\"]}]}")
  }

  test("schema with list of strings only") {
    val arrStructType = new StructType(Seq(arrOfStrField))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[" +
      "{\"name\":\"arr\",\"type\":[{\"type\":\"array\",\"items\":\"string\"},\"null\"]}]}")
  }

  test("schema with list of strings not nullable") {
    val arrStructType = new StructType(Seq(intField, arrOfStrFieldNotNullable))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}")
  }

  test("schema with list of records") {
    val arrStructType = new StructType(Seq(intField, arrOfRecField))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"arrOfRec\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\"," +
      "\"name\":\"items\",\"fields\":[{\"name\":\"str\",\"type\":[\"string\",\"null\"]}]}},\"null\"]}]}")
  }

  test("schema with map of strings") {
    val mapStructType = new StructType(Seq(intField, mapOfStrField))
    val result = AvroConverter.getAvroSchema(mapStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":[\"string\",\"null\"]}}]}")
  }

  test("schema with map of records") {
    val mapStructType = new StructType(Seq(intField, mapOfRecField))
    val result = AvroConverter.getAvroSchema(mapStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"mapOfRec\",\"type\":{\"type\":\"map\",\"values\":[{\"type\":\"record\"," +
      "\"name\":\"values\",\"fields\":[{\"name\":\"str\",\"type\":[\"string\",\"null\"]}]},\"null\"]}}]}")
  }

  test("schema with byte type") {
    val result = AvroConverter.getAvroSchema(new StructType(Seq(byteField)))
    assert(result.toString() == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"byte\",\"type\":\"int\"}]}")
  }

  test("schema with list of list of strings") {
    val arrStructType = new StructType(Seq(arrOfArrString))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"arrOfArrayString\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"string\"}},\"null\"]}]}")
  }

  test("schema with map of map") {
    val mapStructType = new StructType(Seq(mapOfMap))
    val result = AvroConverter.getAvroSchema(mapStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"mapOfMap\",\"type\":{\"type\":\"map\",\"values\":[{\"type\":\"map\",\"values\":[\"string\",\"null\"]},\"null\"]}}]}")
  }

  test("schema with list of Maps of lists") {
    val arrStructType = new StructType(Seq(arrOfMapofArr))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"arrOfMapOfArr\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"map\",\"values\":[{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"string\"}},\"null\"]}},\"null\"]}]}")
  }

  test("schema with lot of fields") {
    var lots = Seq[StructField]()
    for (i <- 0 until 10000) {
      lots = lots ++ Seq(new StructField(s"str$i", DataType("StringType"), true))
    }
    val lotsStruct = new StructType(lots)
    val tryGetLogs = Try(AvroConverter.getAvroSchema(lotsStruct))
    assert(true, tryGetLogs.isSuccess)
  }

  test("schema with decimal type") {
    val result = AvroConverter.getAvroSchema(StructType(Seq(decimalField)))
    assert(result.toString() == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"decimal\",\"type\":[\"string\",\"null\"]}]}")

  }

  // *************** results *************
  test("simple results") {
    val result = AvroConverter.getAvroResult(structTypeRow, structType)
    val expected1 = new GenericData.Record(AvroConverter.getAvroSchema(structType));
    expected1.put("int", 1)
    expected1.put("str", "a")
    val expected2 = new GenericData.Record(AvroConverter.getAvroSchema(structType));
    expected2.put("int", 2)
    expected2.put("str", "b")

    assert(result.size === 2, "different number of rows")
    assert(result.sameElements(Array(expected1, expected2)), "different elements")

  }

  test("results with record") {
    val complexStructType = new StructType(Seq(strField, recordType))
    val complexStructTypeRow = Array(Row.fromSeq(Seq("aa", Row.fromSeq(Seq(1, "a")))), Row.fromSeq(Seq("bb", Row.fromSeq(Seq(2, "b")))))
    val schema = AvroConverter.getAvroSchema(complexStructType)
    val recordSchema = schema.getField("record").schema();
    val result = AvroConverter.getAvroResult(complexStructTypeRow, complexStructType)

    val expected1 = new GenericData.Record(schema)
    expected1.put("str", "aa")
    val expectedRecord1 = new GenericData.Record(recordSchema)
    expectedRecord1.put("int", 1)
    expectedRecord1.put("str", "a")
    expected1.put("record", expectedRecord1)

    val expected2 = new GenericData.Record(schema);
    expected2.put("str", "bb")
    val expectedRecord2 = new GenericData.Record(recordSchema)
    expectedRecord2.put("int", 2)
    expectedRecord2.put("str", "b")
    expected2.put("record", expectedRecord2)

    val expectedResult = Array(expected1, expected2)

    assert(result.size === 2, "different number of rows")
    assert(result.sameElements(expectedResult), "different elements")

  }

  test("results with list of strings") {
    val arrStructType = new StructType(Seq(intField, arrOfStrField))
    val arrStructTypeRow = Array(Row.fromSeq(Seq(1, Row.fromSeq(Seq("a", "b", "c")))), Row.fromSeq(Seq(2, Row.fromSeq(Seq("d", "e", "f")))))
    val schema = AvroConverter.getAvroSchema(arrStructType)
    val arraySchema = schema.getField("arr").schema().getTypes.get(0);
    val result = AvroConverter.getAvroResult(arrStructTypeRow, arrStructType)

    val expected1 = new GenericData.Record(schema)
    expected1.put("int", 1)
    val expectedArray1 = new GenericData.Array(arraySchema, List("a", "b", "c"))
    expected1.put("arr", expectedArray1)

    val expected2 = new GenericData.Record(schema);
    expected2.put("int", 2)
    val expectedArray2 = new GenericData.Array(arraySchema, List("d", "e", "f"))

    expected2.put("arr", expectedArray2)

    val expectedResult = Array(expected1, expected2)

    assert(result.size === 2, "different number of rows")
    assert(result.sameElements(expectedResult), "different elements")

  }

  test("results with list of strings not nullable") {
    val arrStructType = new StructType(Seq(intField, arrOfStrFieldNotNullable))
    val arrStructTypeRow = Array(Row.fromSeq(Seq(1, Row.fromSeq(Seq("a", "b", "c")))), Row.fromSeq(Seq(2, Row.fromSeq(Seq("d", "e", "f")))))
    val schema = AvroConverter.getAvroSchema(arrStructType)
    val arraySchema = schema.getField("arr").schema();
    val result = AvroConverter.getAvroResult(arrStructTypeRow, arrStructType)

    val expected1 = new GenericData.Record(schema)
    expected1.put("int", 1)
    val expectedArray1 = new GenericData.Array(arraySchema, List("a", "b", "c"))
    expected1.put("arr", expectedArray1)

    val expected2 = new GenericData.Record(schema);
    expected2.put("int", 2)
    val expectedArray2 = new GenericData.Array(arraySchema, List("d", "e", "f"))

    expected2.put("arr", expectedArray2)

    val expectedResult = Array(expected1, expected2)

    assert(result.size === 2, "different number of rows")
    assert(result.sameElements(expectedResult), "different elements")

  }

  test("results with list of records") {
    val arrStructType = new StructType(Seq(intField, arrOfRecField))
    val arrStructTypeRow = Array(
      Row.fromSeq(
        Seq(1, Row.fromSeq(
          Seq(Row.fromSeq(
            Seq("a")),
            Row.fromSeq(
              Seq("b")))))),
      Row.fromSeq(
        Seq(2, Row.fromSeq(
          Seq(Row.fromSeq(
            Seq("c")), Row.fromSeq(
            Seq("d")))))))

    val schema = AvroConverter.getAvroSchema(arrStructType)
    val arraySchema = schema.getField("arrOfRec").schema().getTypes.get(0);
    val recordSchema = arraySchema.getElementType

    val result = AvroConverter.getAvroResult(arrStructTypeRow, arrStructType)

    // !! first row
    val expected1 = new GenericData.Record(schema)
    expected1.put("int", 1)

    val expectedRec11 = new GenericData.Record(recordSchema)
    expectedRec11.put("str", "a")
    val expectedRec12 = new GenericData.Record(recordSchema)
    expectedRec12.put("str", "b")

    val expectedArray1 = new GenericData.Array(arraySchema, List(expectedRec11, expectedRec12))
    expected1.put("arrOfRec", expectedArray1)

    // !! second row
    val expected2 = new GenericData.Record(schema)
    expected2.put("int", 2)

    val expectedRec21 = new GenericData.Record(recordSchema)
    expectedRec21.put("str", "c")
    val expectedRec22 = new GenericData.Record(recordSchema)
    expectedRec22.put("str", "d")

    val expectedArray2 = new GenericData.Array(arraySchema, List(expectedRec21, expectedRec22))
    expected2.put("arrOfRec", expectedArray2)

    val expectedResult = Array(expected1, expected2)

    assert(result.size === 2, "different number of rows")
    assert(result.sameElements(expectedResult), "different elements")

  }

  test("result with map of strings") {
    val mapStructType = new StructType(Seq(intField, mapOfStrField))

    val mapStructTypeRow = Array(Row.fromSeq(
      Seq(1, Map("a" -> "b", "c" -> "d"))),
      Row.fromSeq(
        Seq(2, Map("d" -> "e", "f" -> "g"))))

    val schema = AvroConverter.getAvroSchema(mapStructType)
    val mapSchema = schema.getField("map").schema()
    val result = AvroConverter.getAvroResult(mapStructTypeRow, mapStructType)

    // !! first row
    val expected1 = new GenericData.Record(schema)
    expected1.put("int", 1)
    expected1.put("map", mapAsJavaMap(Map("a" -> "b", "c" -> "d")))

    // !! second row
    val expected2 = new GenericData.Record(schema)
    expected2.put("int", 2)
    expected2.put("map", mapAsJavaMap(Map("d" -> "e", "f" -> "g")))
    val expectedResult = Array(expected1, expected2)

    assert(result.size === 2, "different number of rows")
    assert(result.sameElements(expectedResult), "different elements")
  }

  test("results with map of records") {
    val mapStructType = new StructType(Seq(intField, mapOfRecField))
    val mapStructTypeRow = Array(Row.fromSeq(
      Seq(1, Map("a" -> Row.fromSeq(Seq("b")), "c" -> Row.fromSeq(Seq("d"))))),
      Row.fromSeq(
        Seq(2, Map("d" -> Row.fromSeq(Seq("e")), "f" -> Row.fromSeq(Seq("g"))))))

    val schema = AvroConverter.getAvroSchema(mapStructType)
    val mapSchema = schema.getField("mapOfRec").schema()
    val recSchema = mapSchema.getValueType().getTypes.get(0)

    val result = AvroConverter.getAvroResult(mapStructTypeRow, mapStructType)
    val actualMap1 = result(0).get(1).asInstanceOf[Wrappers.MapWrapper[String, GenericData.Record]]
    val actualMap2 = result(1).get(1).asInstanceOf[Wrappers.MapWrapper[String, GenericData.Record]]

    // !! first row
    val expectedRec11 = new GenericData.Record(recSchema)
    expectedRec11.put("str", "b")
    val expectedRec12 = new GenericData.Record(recSchema)
    expectedRec12.put("str", "d")
    val expectedMap1 = mapAsJavaMap(Map("a" -> expectedRec11, "c" -> expectedRec12))

    // !! second row
    val expectedRec21 = new GenericData.Record(recSchema)
    expectedRec21.put("str", "e")
    val expectedRec22 = new GenericData.Record(recSchema)
    expectedRec22.put("str", "g")
    val expectedMap2 = mapAsJavaMap(Map("d" -> expectedRec21, "f" -> expectedRec22))

    assert(result.size === 2, "different number of rows")
    assert(result(0).get(0) == 1, "different integer on row1")
    assert(actualMap1.keys.sameElements(expectedMap1.keys), "different keys in expected map on row1")
    val actualMapIterator1 = actualMap1.values().iterator()
     assert(actualMapIterator1.next().get("str") === "b", "different first record value 1 in expected map on row1")
     assert(actualMapIterator1.next().get("str") === "d", "different first record value 2 in expected map on row1")
    
    
    assert(result(1).get(0) == 2, "different integer on row2")
    assert(actualMap2.keys.sameElements(expectedMap2.keys), "different keys in expected map on row2")
    val actualMapIterator2 = actualMap2.values().iterator()
     assert(actualMapIterator2.next().get("str") === "e", "different second record value 1 in expected map on row2")
     assert(actualMapIterator2.next().get("str") === "g", "different second record value 2 in expected map on row2")

  }
}