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
import scala.collection.mutable.ArrayBuffer
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.file.FileReader
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.file.DataFileWriter

case class AvroResult(schema: Schema, result: Array[GenericRecord]) {
  def this() = {
    this(null, Array.empty)
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

      case that: AvroResult =>
        (that canEqual this) &&
          result.deep == that.result.deep &&
          schema == that.schema

      case _ => false
    }
  }

  def serializeResult(): Array[Byte] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val baos = new ByteArrayOutputStream()
    val fileWriter = new DataFileWriter[GenericRecord](datumWriter)
    fileWriter.create(schema, baos)
    val binaryResults = result map (row => {
      fileWriter.append(row)
    })

    fileWriter.close()
    baos.toByteArray()
  }
}
object AvroResult {

  def deserializeResult(byteArray: Array[Byte], schema: Schema): Array[GenericRecord] = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val in = new SeekableByteArrayInput(byteArray)
    var dfr: FileReader[GenericRecord] = null
    val records = ArrayBuffer[GenericRecord]()
    try {
      dfr = DataFileReader.openReader(in, reader);
      while (dfr.hasNext()) {
        records += dfr.next()
      }

    } finally {
      if (dfr != null) {
        dfr.close();
      }
    }

    records.toArray
  }
}