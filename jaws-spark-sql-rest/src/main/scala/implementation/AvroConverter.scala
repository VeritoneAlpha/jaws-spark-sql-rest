package implementation

import org.apache.avro.SchemaBuilder.{ BaseFieldTypeBuilder, BaseTypeBuilder, FieldAssembler, RecordDefault }
import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.spark.sql.catalyst.types._
import org.apache.avro.SchemaBuilder.ArrayDefault
import org.apache.avro.SchemaBuilder.FieldDefault

object AvroConverter {

  private def callMethod(builder: Object, method: String) = {
    builder.getClass.getMethod(method).invoke(builder)
  }
  private def callMethodWithNoDefaults(builder: Object, method: String) = {
    val result = callMethod(builder, method)
    callMethod(result, "noDefault")
  }

  private def addFields[R](dataType: DataType, fieldName: String, typeBuilder: Object) {

    dataType match {
      case ByteType | ShortType | IntegerType => callMethodWithNoDefaults(typeBuilder, "intType")
      case BinaryType                         => callMethodWithNoDefaults(typeBuilder, "bytesType")
      case StringType                         => callMethodWithNoDefaults(typeBuilder, "stringType")
      case LongType | TimestampType           => callMethodWithNoDefaults(typeBuilder, "longType")
      case FloatType                          => callMethodWithNoDefaults(typeBuilder, "floatType")
      case DoubleType                         => callMethodWithNoDefaults(typeBuilder, "doubleType")
      case BooleanType                        => callMethodWithNoDefaults(typeBuilder, "booleanType")
      case NullType                           => callMethodWithNoDefaults(typeBuilder, "nullType")
      case fieldType: StructType => {
        val recordBuilder = typeBuilder.getClass.getMethod("record", classOf[String]).invoke(typeBuilder, fieldName)
        val fieldAssembler = callMethod(recordBuilder, "fields")
        addStructType(fieldType, fieldAssembler.asInstanceOf[FieldAssembler[R]])
        callMethodWithNoDefaults(fieldAssembler, "endRecord")

      }
      case arrayType: ArrayType => {
        val arrayBuilder = callMethod(typeBuilder, "array")
        var arrayItemsBuilder = callMethod(arrayBuilder, "items")
        if (arrayType.containsNull)
          arrayItemsBuilder = callMethod(arrayItemsBuilder, "nullable")
        addFields(arrayType.elementType, fieldName, arrayItemsBuilder)
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

        addFields(mapType.valueType, fieldName, mapValuesBuilder)

      }
      case unsupportedType => throw new IllegalArgumentException(s"Unhandled Avro schema type $unsupportedType")

    }

  }

  private def addStructType[R](structType: StructType, recordAssembler: FieldAssembler[R]) {
    structType.fields foreach {
      field =>
        val fieldAssembler =
          if (field.nullable) recordAssembler.name(field.name).`type`().nullable()
          else recordAssembler.name(field.name).`type`()
        addFields(field.dataType, field.name, fieldAssembler)
    }
  }

  def getAvroSchema(structType: StructType): Schema = {
    var recordAssembler = SchemaBuilder.record("RECORD").fields()
    addStructType(structType, recordAssembler)
    recordAssembler.endRecord()
  }

}
