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
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(resultToBinary())
    oos.close
    baos.toByteArray()
  }

  def resultToBinary() = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)

    val binaryResults = result map (row => {
      val baos = new ByteArrayOutputStream()
      val directBinaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null)
      datumWriter.write(row, directBinaryEncoder)
      baos.toByteArray()
    })

    binaryResults
  }
}

object AvroResult {

  def deserializeResult(byteArray: Array[Byte], schema: Schema): Array[GenericRecord] = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(byteArray))
    val resultAsArrayOfBytes = ois.readObject().asInstanceOf[Array[Array[Byte]]]
    ois.close()
    resultFromBinary(resultAsArrayOfBytes, schema)
  }

  private def resultFromBinary(resultByte: Array[Array[Byte]], schema: Schema): Array[GenericRecord] = {

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    resultByte map (genericData => {
      val directBinaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(genericData), null)
      datumReader.read(null, directBinaryDecoder)
    })

  }

}