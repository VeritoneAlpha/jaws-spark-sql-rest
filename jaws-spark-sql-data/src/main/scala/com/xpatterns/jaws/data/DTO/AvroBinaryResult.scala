package com.xpatterns.jaws.data.DTO
import spray.json.DefaultJsonProtocol._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayInputStream
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import spray.json.RootJsonFormat
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory

case class AvroBinaryResult(schema: Schema, result: Array[Byte]) {
  def this() = {
    this(null, Array.empty)
  }
  
  def this(avroResult : AvroResult) = {
    this(avroResult.schema, avroResult.serializeResult())
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    Option(result) match {
      case None => result = prime * result + 0
      case _    => result = prime * result + result.hashCode()
    }
    Option(schema) match {
      case None => result = prime * result + 0
      case _    => result = prime * result + schema.hashCode()
    }

    result
  }

  override def equals(other: Any): Boolean = {

    other match {

      case that: AvroBinaryResult =>
        (that canEqual this) &&
          result.deep == that.result.deep &&
          schema == that.schema

      case _ => false
    }
  }

}