package com.xpatterns.jaws.data.utils

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory
import spray.json.DefaultJsonProtocol._
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult
import com.xpatterns.jaws.data.DTO.Column
import spray.json._
import com.google.gson.GsonBuilder
import java.sql.Timestamp
import collection.JavaConversions._
import com.xpatterns.jaws.data.DTO.AvroBinaryResult
import org.apache.spark.sql.types.StructType

class ResultsConverter(val schema: StructType, val result: Array[Row]) {

  def toAvroResults(): AvroResult = {
    val avroSchema = AvroConverter.getAvroSchema(schema)
    val avroResults = AvroConverter.getAvroResult(result, schema)
    new AvroResult(avroSchema, avroResults)
  }

  def toCustomResults(): CustomResult = {
    val gson = new GsonBuilder().create()
    val customSchema = CustomConverter.getCustomSchema(schema)

    new CustomResult(customSchema, CustomConverter.getCustomResult(result, schema))
  }

  def toAvroBinaryResults(): AvroBinaryResult = {
    new AvroBinaryResult(toAvroResults())
  }

}