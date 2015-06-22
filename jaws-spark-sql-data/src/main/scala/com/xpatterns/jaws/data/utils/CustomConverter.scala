package com.xpatterns.jaws.data.utils

import org.apache.spark.sql.catalyst.expressions.Row
import spray.json.DefaultJsonProtocol._
import com.xpatterns.jaws.data.DTO.CustomResult
import com.xpatterns.jaws.data.DTO.Column
import com.google.gson.GsonBuilder
import java.sql.Timestamp
import collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRow

object CustomConverter {

  def getCustomSchema(schema: StructType): Array[Column] = {
    schema.fields map (field => getCustomSchema(field.dataType, field.name)) toArray
  }

  private def getCustomSchema(fieldType: DataType, fieldName: String): Column = {
    fieldType match {
      case ArrayType(elementType, _)         => new Column(fieldName, "ArrayType", "", Array(getCustomSchema(elementType, "items")))
      case MapType(StringType, valueType, _) => new Column(fieldName, "MapType", "", Array(getCustomSchema(valueType, "values")))
      case structType: StructType            => new Column(fieldName, "StructType", "", structType.fields map (field => getCustomSchema(field.dataType, field.name)) toArray)
      case _                                 => new Column(fieldName, fieldType.toString(),"", Array.empty)
    }
  }

  def getCustomResult(result: Array[Row], schema: StructType) = {
    val converter = createConverter(schema)
    result map (row => converter(row).asInstanceOf[Array[Any]])
  }

  private def createConverter(
    dataType: DataType): (Any) => Any = {
    dataType match {
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | StringType |
        BinaryType | BooleanType =>
        (item: Any) => item

        case DecimalType() =>
        (item: Any) => if (item == null) null else item.toString

        case TimestampType =>
        (item: Any) => {
          if (item == null) null else item.asInstanceOf[Timestamp].getTime
        }

        case ArrayType(elementType, _) =>
        val elementConverter = createConverter(elementType)
        (item: Any) => {
          if (item == null) {
            null
          } else {

            val sourceArray = item.asInstanceOf[GenericRow].toSeq
            val destination = sourceArray map { element => elementConverter(element) }
            destination.toArray
          }
        }

        case MapType(StringType, valueType, _) =>
        val valueConverter = createConverter(valueType)

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
        val fieldConverters = structType.fields.map(field =>
          createConverter(field.dataType))

        (item: Any) => {
          if (item == null) {
            null
          } else {

            val row = item.asInstanceOf[Row].toSeq
            val valueConverter = row zip fieldConverters
            valueConverter map (value => value match {
              case (field, converter) => converter(field)
            }) toArray
          }
        }
    }
  }

}