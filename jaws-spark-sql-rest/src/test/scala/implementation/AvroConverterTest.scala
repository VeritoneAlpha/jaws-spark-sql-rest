package implementation


import org.apache.spark.sql.catalyst.types.{DataType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.catalyst.types.ByteType


@RunWith(classOf[JUnitRunner])
class AvroConverterTest extends FunSuite {
  val intField = new StructField("int", DataType("IntegerType"), false)
  val strField = new StructField("str", DataType("StringType"), true)
  val arrOfStrField = new StructField("arr", DataType("ArrayType(StringType,false)"), true)
  val arrOfRecField = new StructField("arrOfRec", DataType("ArrayType(StructType(List(StructField(str,StringType,true))),false)"), true)
  val mapOfStrField = new StructField("map", DataType("MapType(StringType,StringType,true)"), false)
  val mapOfRecField = new StructField("mapOfRec", DataType("MapType(StringType,StructType(List(StructField(str,StringType,true))),true)"), false)
  val structType = new StructType(Seq(intField, strField))
  val recordType = new StructField("record", structType, false)
  val byteField = new StructField("byte", DataType("ByteType"), false)
  val binaryField = new StructField("binary", DataType("BinaryType"), true)

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

  test("schema with list of records") {
    val arrStructType = new StructType(Seq(intField, arrOfRecField))
    val result = AvroConverter.getAvroSchema(arrStructType)
    assert(result.toString == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"int\",\"type\":\"int\"}," +
      "{\"name\":\"arrOfRec\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\"," +
      "\"name\":\"arrOfRec\",\"fields\":[{\"name\":\"str\",\"type\":[\"string\",\"null\"]}]}},\"null\"]}]}")
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
      "\"name\":\"mapOfRec\",\"fields\":[{\"name\":\"str\",\"type\":[\"string\",\"null\"]}]},\"null\"]}}]}")
  }

  test("schema with byte type") {
    val result = AvroConverter.getAvroSchema(new StructType(Seq(byteField)))
    assert(result.toString() == "{\"type\":\"record\",\"name\":\"RECORD\",\"fields\":[{\"name\":\"byte\",\"type\":\"int\"}]}")
  }
}