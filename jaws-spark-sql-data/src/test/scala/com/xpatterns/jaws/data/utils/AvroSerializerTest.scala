package com.xpatterns.jaws.data.utils

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import scala.util.Try
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import collection.JavaConversions._
import scala.collection.convert.Wrappers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.sql.types._
import org.apache.avro.SchemaBuilder
import com.xpatterns.jaws.data.DTO.AvroResult

@RunWith(classOf[JUnitRunner])
class AvroSerializerTest extends FunSuite {
  val intField = new StructField("int", IntegerType, false)
  val intFieldRow = Array(Row.fromSeq(Seq(123)), Row.fromSeq(Seq(23)))

  val doubleField = new StructField("double", DoubleType, false)
  val doubleFieldRow = Array(Row.fromSeq(Seq(123.toDouble)), Row.fromSeq(Seq(23.toDouble)))

  val timestampField = new StructField("double", TimestampType, false)
  val timestampFieldRow = Array(Row.fromSeq(Seq(new Timestamp(new Date().getTime()), Row.fromSeq(Seq(new Timestamp(new Date().getTime()))))))

  val longField = new StructField("long", LongType, false)
  val longFieldRow = Array(Row.fromSeq(Seq(123.toLong)), Row.fromSeq(Seq(23.toLong)))

  val shortField = new StructField("short", ShortType, false)
  val shortFieldRow = Array(Row.fromSeq(Seq(123.toShort)), Row.fromSeq(Seq(23.toShort)))

  val floatField = new StructField("long", FloatType, true)
  val floatFieldRow = Array(Row.fromSeq(Seq(123.toFloat)), Row.fromSeq(Seq(23.toFloat)))

  val booleanField = new StructField("boolean", BooleanType, true)
  val booleanFieldRow = Array(Row.fromSeq(Seq(true)), Row.fromSeq(Seq(false)))

  val strField = new StructField("str", StringType, true)
  val arrOfStrField = new StructField("arr", ArrayType(StringType, false), true)
  val arrOfStrFieldNotNullable = new StructField("arr", ArrayType(StringType, false), false)
  val arrOfRecField = new StructField("arr", ArrayType(StructType(List(StructField("str", StringType, true))), true), true)
  val mapOfStrField = new StructField("map", MapType(StringType, StringType, true), true)
  val mapOfRecField = new StructField("mapOfRec", MapType(StringType, StructType(List(StructField("str", StringType, true))), true), true)

  val structType = new StructType(Array(intField, strField))
  val structTypeRow = Array(Row.fromSeq(Seq(1, "a")), Row.fromSeq(Seq(2, "b")))

  val recordType = new StructField("rrecord", structType, true)
  val binaryField = new StructField("binary", BinaryType, true)
  val binaryFieldRow = Array(Row.fromSeq(Seq(Array(192, 168, 1, 1).map(_.toByte))), Row.fromSeq(Seq(Array(123, 54, 3, 1).map(_.toByte))))

  val byteField = new StructField("byte", ByteType, true)
  val byteFieldRow = Array(Row.fromSeq(Seq(123.toByte, 45.toByte)), Row.fromSeq(Seq(123.toByte, 45.toByte)))

  val arrOfArrString = new StructField("arrOfArrayString", ArrayType(ArrayType(StringType, false), false), true)
  val mapOfMap = new StructField("mapOfMap", MapType(StringType, MapType(StringType, StringType, true), true), false)
  val arrOfMapofArr = new StructField("arrOfMapOfArr", ArrayType(MapType(StringType, ArrayType(ArrayType(StringType, false), false), true), false), true)
  val decimalField = new StructField("decimal", DecimalType(), true)

  // *************** results *************

    test("simple byte results") {
      val conv = new ResultsConverter(new StructType(Array(byteField)), byteFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple timestamp results") {
      val conv = new ResultsConverter(new StructType(Array(timestampField)), timestampFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple binary results") {
      val conv = new ResultsConverter(new StructType(Array(binaryField)), binaryFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple short results") {
      val conv = new ResultsConverter(new StructType(Array(shortField)), shortFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple int results") {
      val conv = new ResultsConverter(new StructType(Array(intField)), intFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple long results") {
      val conv = new ResultsConverter(new StructType(Array(longField)), longFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple float results") {
      val conv = new ResultsConverter(new StructType(Array(floatField)), floatFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple double results") {
      val conv = new ResultsConverter(new StructType(Array(doubleField)), doubleFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("simple boolean results") {
      val conv = new ResultsConverter(new StructType(Array(booleanField)), booleanFieldRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("results with list of strings") {
      val arrStructType = new StructType(Array(intField, arrOfStrField))
      val arrStructTypeRow = Array(Row.fromSeq(Seq(1, Row.fromSeq(Seq("a", "b", "c")))), Row.fromSeq(Seq(2, Row.fromSeq(Seq("d", "e", "f")))))
  
      val conv = new ResultsConverter(arrStructType, arrStructTypeRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("results with list of strings not nullable") {
      val arrStructType = new StructType(Array(intField, arrOfStrFieldNotNullable))
      val arrStructTypeRow = Array(Row.fromSeq(Seq(1, Row.fromSeq(Seq("a", "b", "c")))), Row.fromSeq(Seq(2, Row.fromSeq(Seq("d", "e", "f")))))
      val conv = new ResultsConverter(arrStructType, arrStructTypeRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result.sameElements(des))
    }
  
    test("result with map of strings") {
      val mapStructType = new StructType(Array(intField, mapOfStrField))
  
      val mapStructTypeRow = Array(Row.fromSeq(
        Seq(1, Map("a" -> "b", "c" -> "d"))),
        Row.fromSeq(
          Seq(2, Map("d" -> "e", "f" -> "g"))))
  
      val conv = new ResultsConverter(mapStructType, mapStructTypeRow)
      val avro = conv.toAvroResults()
      val seri = avro.serializeResult()
      val des = AvroResult.deserializeResult(seri, avro.schema)
      assert(avro.result(0).get(0) == des(0).get(0))
      assert(avro.result(1).get(0) == des(1).get(0))
  
    }

  test("complex imbricated schema") {

    val objType: StructType = new StructType(Array(StructField("myString_RENAMED", StringType, false), StructField("myInteger_RENAMED", IntegerType, false)))
    val objTypeComplex: StructType = new StructType(Array[StructField](new StructField("myObj_RENAMED", objType, false), new StructField("myMap_RENAMED", new MapType(StringType, new MapType(StringType, new MapType(StringType, IntegerType, false), false), false), false)))

    val fields = Array(StructField("myLong_RENAMED", LongType, true), 
        StructField("myTimestamp_RENAMED", TimestampType, true), 
        StructField("myString_RENAMED", StringType, true), 
        StructField("myInteger_RENAMED", IntegerType, true), 
        StructField("myByte_RENAMED", ByteType, true), 
        StructField("myShort_RENAMED", ShortType, true), 
        StructField("myFloat_RENAMED", FloatType, true), 
        StructField("myDouble_RENAMED", DoubleType, true), 
        StructField("myBoolean_RENAMED", BooleanType, true), 
        StructField("myByteArray_RENAMED", BinaryType, true), 
        StructField("myObj_RENAMED", objType, false), 
        StructField("myMap_RENAMED", new MapType(StringType, IntegerType, false), false), 
        StructField("myMap1_RENAMED", new MapType(StringType, new MapType(StringType, IntegerType, false), false), false), 
        StructField("myMap2_RENAMED", new MapType(StringType, new MapType(StringType, new MapType(StringType, IntegerType, false), false), false), false), 
        StructField("myMap3_RENAMED", new MapType(StringType, objType, false), false), 
        StructField("mySequence_RENAMED", new ArrayType(IntegerType, false), false), 
        StructField("myObjSequence_RENAMED", new ArrayType(objType, false), false), 
        StructField("myMapSequence_RENAMED", new ArrayType(new MapType(StringType, objType, false), false), false), 
        StructField("mySequenceSequence_RENAMED", new ArrayType(new ArrayType(IntegerType, false), false), false), 
        StructField("myObjComplex_RENAMED", objTypeComplex, false))

    val structType = new StructType(fields)

    val tryConvert = Try {
      val result = AvroConverter.getAvroSchema(structType, "RECORD")
      //    val result = Test.convertStructToAvro(structType, SchemaBuilder.record("RECORD"), "RECORD")
      //    System.out.println(result.toString);
    }

    assert(tryConvert.isSuccess)
  }
}