package com.xpatterns.jaws.data.utils

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import com.xpatterns.jaws.data.DTO.AvroResult

case class Obj(myString: String, myInteger: Int)
case class ObjString(s1: String, s2: String)
case class ComplexObj(s: String, obj: Obj)

case class CompleteObj(
  myByte: Byte,
  myShort: Short,
  myLong: Long,
  myFloat: Float,
  myDouble: Double,
  myBoolean: Boolean,
  myObj: Obj,
  myMap: Map[String, Int],
  mySequence: Seq[Int],
  myObjSequence: Seq[Obj],
  myByteArray: Array[Byte])

case class CompleteObj2(
  myByte: Byte,
  myShort: Short,
  myLong: Long,
  myFloat: Float,
  myDouble: Double,
  myObj: Obj,
  myMap: Map[String, Int],
  mySequence: Seq[Int],
  myObjSequence: Seq[Obj],
  myByteArray: Array[Byte])
case class ComplObject(
  myString: String,
  myInt: Int,
  myByte: Byte,
  myShort: Short,
  myLong: Long,
  myFloat: Float,
  myDouble: Double,
  myBoolean: Boolean,
  myObj: Obj,
  myMap1: Map[String, Obj],
  myMap2: Map[String, ObjString],
  myMap3: Map[String, ComplexObj],
  mySequence: Seq[Seq[Array[Seq[Array[Array[Seq[CompleteObj]]]]]]],
  myArray: Array[ComplexObj])

case class Obj1(array: Array[Array[Array[Obj]]])
case class Obj4(array: Seq[Seq[Seq[Obj]]])
case class Obj2(map: Map[String, Map[String, Map[String, Obj]]])
case class Obj3(map: Map[String, Map[String, Obj]])

@RunWith(classOf[JUnitRunner])
class AvroConverterComplexTest extends FunSuite {

  def newObj(i: Int) = new Obj("s1" + i, i)
  def newObjString(i: Int) = new ObjString("s1_" + i, "s2_" + i)
  def newComplexObj(i: Int) = new ComplexObj("s_" + i, newObj(i))
  def newCompleteObj(i: Int) = new CompleteObj(
    Byte.MaxValue,
    Short.MaxValue,
    i,
    0.3f,
    0.6d,
    false,
    newObj(i),
    Map(("key1", i), ("key2", i + 1)),
    List(i, i + 1, i + 2),
    List(newObj(i + 1), newObj(i + 2)),
    Array((65 + i).toByte, (66 + i).toByte))

  def newComplObj(i: Int) = new ComplObject(
    "newComplObj " + i,
    Int.MaxValue,
    Byte.MinValue,
    Short.MinValue,
    Long.MaxValue,
    Float.PositiveInfinity,
    Double.MinPositiveValue,
    i % 2 == 0,
    newObj(i + 100),
    Map(("str11", newObj(i + 10)), ("str12", newObj(i + 11)), ("str13", newObj(i + 12)), ("str14", newObj(i + 13)), ("str15", newObj(i + 14)), ("str16", newObj(i + 15)), ("str17", newObj(i + 16)), ("str18", newObj(i + 17))),
    Map(("str21", newObjString(i + 20)), ("str22", newObjString(i + 21)), ("str23", newObjString(i + 22)), ("str24", newObjString(i + 23)), ("str25", newObjString(i + 24)), ("str26", newObjString(i + 25))),
    Map(("str31", newComplexObj(i + 30)), ("str32", newComplexObj(i + 31)), ("str33", newComplexObj(i + 32)), ("str34", newComplexObj(i + 33)), ("str35", newComplexObj(i + 34))),
    Seq(Seq(Array(Seq(Array(Array(Seq(newCompleteObj(i), newCompleteObj(i + 1), newCompleteObj(i + 2), newCompleteObj(i + 3), newCompleteObj(i + 4), newCompleteObj(i + 5)),
      Seq(newCompleteObj(i + 7), newCompleteObj(i + 8), newCompleteObj(i + 9))),
      Array(Seq(newCompleteObj(i + 10), newCompleteObj(11)),
        Seq(newCompleteObj(i + 12), newCompleteObj(i + 13)))),
      Array(Array(Seq(newCompleteObj(i), newCompleteObj(i + 1), newCompleteObj(i + 2), newCompleteObj(i + 3), newCompleteObj(i + 4), newCompleteObj(i + 5)),
        Seq(newCompleteObj(i + 7), newCompleteObj(i + 8), newCompleteObj(i + 9))),
        Array(Seq(newCompleteObj(i + 10), newCompleteObj(11)),
          Seq(newCompleteObj(i + 12), newCompleteObj(i + 13)))))))),
    Array(newComplexObj(i), newComplexObj(i / 2), newComplexObj(i / 3)))

  test("complex") {
    val listInt = (1 to 10).toList

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
  
    
    val df = sc.parallelize(listInt.map(newComplObj(_))).toDF()

    val result = AvroConverter.getAvroResult(df.collect, df.schema)
    val schema = AvroConverter.getAvroSchema(df.schema)
    val avroResult = new AvroResult(schema, result)
    val serialized = avroResult.serializeResult
    val deserialized = AvroResult.deserializeResult(serialized, schema)
    
  }
}