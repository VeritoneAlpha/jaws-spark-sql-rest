package com.xpatterns.jaws.data.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.xpatterns.jaws.data.utils.ResultsConverter
import com.xpatterns.jaws.data.DTO.AvroResult
//import org.apache.spark.sql.parquet.SparkParquetUtility._

case class Positions(
  start: Int,
  end: Int)

case class Terms(
  name: String,
  score: Double,
  positions: Seq[Positions])

case class AnnotatedTerms(
  name: String,
  category: String,
  score: Double,
  positions: Seq[Positions])

case class Categories(
  name: String,
  score: Double)

case class DocMetainfo(
  categories: Seq[Categories],
  annotated_terms: Seq[AnnotatedTerms],
  terms: Seq[Terms])

case class NewPubmed(
  authors: Seq[String],
  body: String,
  category: String,
  documentId: String,
  doc_metainfo: DocMetainfo,
  publicationDate: String,
  publicationYear: Int,
  title: String)

@RunWith(classOf[JUnitRunner])
class AvroConverterCustomTest extends FunSuite {

  test("result with map of strings") {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pbList = List(1)
    val df = sc.parallelize(pbList).map(_ => new NewPubmed(
      List("ana", "ion"),
      "body",
      "category",
      "documentId",
      new DocMetainfo(
        List(new Categories("name", 1.1)),
        List(new AnnotatedTerms("", "category", 1.3, Seq(new Positions(1, 1)))),
        List(new Terms("name", 1.5, List(new Positions(1, 2))))),
      "publicationDate",
      2015,
      "title")).toDF

    val values =  df.collect
    val result = AvroConverter.getAvroResult(values, df.schema)
    val schema = AvroConverter.getAvroSchema(df.schema)
    val ar = new AvroResult(schema, result)
    val serialized = ar.serializeResult()
    val deserialized = AvroResult.deserializeResult(serialized, schema)

    sc.stop()
    print("done")
  }

}