package com.xpatterns.jaws.data.utils

import org.apache.avro.SchemaBuilder.{ BaseFieldTypeBuilder, BaseTypeBuilder, FieldAssembler, RecordDefault }
import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.avro.SchemaBuilder.ArrayDefault
import org.apache.avro.SchemaBuilder.FieldDefault
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import java.sql.Timestamp
import java.util.HashMap
import org.apache.avro.generic.GenericData.Record
import collection.JavaConversions._
import java.nio.ByteBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRow

object AvroConverter {

  private def callMethod(builder: Object, method: String) = {
    builder.getClass.getMethod(method).invoke(builder)
  }
  private def callMethodWithNoDefaults(builder: Object, method: String) = {
    val result = callMethod(builder, method)
    callMethod(result, "noDefault")
  }

  private def addFields[R](dataType: DataType, fieldName: String, typeBuilder: Object, namespace : String) {

    dataType match {
      case ByteType | ShortType | IntegerType => callMethodWithNoDefaults(typeBuilder, "intType")
      case BinaryType                         => callMethodWithNoDefaults(typeBuilder, "bytesType")
      case StringType | DecimalType ()          => callMethodWithNoDefaults(typeBuilder, "stringType")
      case LongType | TimestampType           => callMethodWithNoDefaults(typeBuilder, "longType")
      case FloatType                          => callMethodWithNoDefaults(typeBuilder, "floatType")
      case DoubleType                         => callMethodWithNoDefaults(typeBuilder, "doubleType")
      case BooleanType                        => callMethodWithNoDefaults(typeBuilder, "booleanType")
      case NullType                           => callMethodWithNoDefaults(typeBuilder, "nullType")
      case fieldType: StructType => {
        val recordBuilder = typeBuilder.getClass.getMethod("record", classOf[String]).invoke(typeBuilder, fieldName)
        val fieldAssembler = callMethod(recordBuilder.getClass.getMethod("namespace", classOf[String]).invoke(recordBuilder, namespace), "fields")
        addStructType(fieldType, fieldAssembler.asInstanceOf[FieldAssembler[R]], fieldName)
        callMethodWithNoDefaults(fieldAssembler, "endRecord")

      }
      case arrayType: ArrayType => {
        val arrayBuilder = callMethod(typeBuilder, "array")
        var arrayItemsBuilder = callMethod(arrayBuilder, "items")
        if (arrayType.containsNull)
          arrayItemsBuilder = callMethod(arrayItemsBuilder, "nullable")
        addFields(arrayType.elementType, fieldName, arrayItemsBuilder, fieldName)
      }
      case mapType: MapType => {
        val mapBuilder = callMethod(typeBuilder, "map")
        var mapValuesBuilder = callMethod(mapBuilder, "values")
        if (mapType.valueContainsNull)
          mapValuesBuilder = callMethod(mapValuesBuilder, "nullable")
        mapType.keyType match {
          case StringType => callMethod(mapValuesBuilder, "stringType")
          case _          => throw new IllegalArgumentException("Avro schema map key has to be String")
        }

        addFields(mapType.valueType, fieldName, mapValuesBuilder, fieldName)

      }
      case unsupportedType => throw new IllegalArgumentException(s"Unhandled Avro schema type $unsupportedType")

    }

  }

  private def addStructType[R](structType: StructType, recordAssembler: FieldAssembler[R], namespace : String) {
    structType.fields foreach {
      field =>
        val fieldAssembler =
          if (field.nullable) recordAssembler.name(field.name).`type`().nullable()
          else recordAssembler.name(field.name).`type`()
        addFields(field.dataType, field.name, fieldAssembler, namespace)
    }
  }

  def getAvroSchema(structType: StructType, structName: String = "RECORD"): Schema = {
    var recordAssembler = SchemaBuilder.record(structName).fields()
    addStructType(structType, recordAssembler, structName)
    recordAssembler.endRecord()
  }

  def getAvroResult(result: Array[Row], schema: StructType): Array[GenericRecord] = {

    val converter = createConverter(schema, "RECORD", "")
    result map (row => converter(row).asInstanceOf[GenericRecord])
  }

  /**
   * This function constructs converter function for a given sparkSQL datatype. These functions
   * will be used to convert dataFrame to avro format.
   */
  def createConverter(
    dataType: DataType,
    structName: String,
    recordNamespace: String): (Any) => Any = {
    dataType match {
      case  IntegerType | LongType | FloatType | DoubleType | StringType |
        BooleanType =>
        (item: Any) => item

        case ShortType =>
        (item: Any) => if (item == null) null else item.asInstanceOf[Short].toInt
        
        case ByteType =>
        (item: Any) => if (item == null) null else item.asInstanceOf[Byte].toInt

        case BinaryType =>
        (item: Any) => if (item == null) null else ByteBuffer.wrap(item.asInstanceOf[Array[Byte]])

        case DecimalType() =>
        (item: Any) => if (item == null) null else item.toString

        case TimestampType =>
        (item: Any) => {
          if (item == null) null else item.asInstanceOf[Timestamp].getTime
        }

        case ArrayType(elementType, _) =>
        val elementConverter = createConverter(elementType, "items", recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
          } else {

            val schema = getAvroSchema(new StructType(Array(new StructField("array", dataType, false))), "arrayStruct")
            val sourceArray = if (item.isInstanceOf[Seq[Any]]) item.asInstanceOf[Seq[Any]] else item.asInstanceOf[GenericRow].toSeq

            val destination = sourceArray map { element => elementConverter(element) }
            val arrayRecord = new GenericData.Array(schema.getField("array").schema(), destination)

            arrayRecord
          }
        }

        case MapType(StringType, valueType, _) =>
        val valueConverter = createConverter(valueType, "values", recordNamespace)

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val smap = item.asInstanceOf[Map[String, Any]] map {
              case (key, value) =>
                (key -> valueConverter(value))
            }
            mapAsJavaMap(smap)
          }
        }

        case structType: StructType =>
        val schema: Schema = getAvroSchema(structType, structName)
        val fieldConverters = structType.fields.map(field =>
          createConverter(field.dataType, field.name, recordNamespace))

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }
    
    

}
