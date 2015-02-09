package implementation

import org.apache.avro.SchemaBuilder.{BaseFieldTypeBuilder, BaseTypeBuilder, FieldAssembler, RecordDefault}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.types._


/**
 * Created by robertb on 12.12.2014.
 */
object AvroConverter {

  def getAvroSchema(structType: StructType): Schema = {

    def addFields(structType: StructType, anyRecordAssembler: Any) {

      val recordAssembler = anyRecordAssembler.asInstanceOf[FieldAssembler[RecordDefault[Schema]]]

      structType.fields foreach {
        (field: StructField) =>

          val fieldAssembler =
            if (field.nullable) recordAssembler.name(field.name).`type`().nullable()
            else recordAssembler.name(field.name).`type`()


          field.dataType match {
            case BinaryType | ByteType => fieldAssembler.bytesType().noDefault()
            case IntegerType => fieldAssembler.intType().noDefault()
            case StringType => fieldAssembler.stringType().noDefault()
            case LongType => fieldAssembler.longType().noDefault()
            case FloatType => fieldAssembler.floatType().noDefault()
            case DoubleType => fieldAssembler.doubleType().noDefault()
            case BooleanType => fieldAssembler.booleanType().noDefault()
            case TimestampType => fieldAssembler.longType().noDefault()
            case StructType(_) => back(field.dataType.asInstanceOf[StructType], field.name, fieldAssembler)
            case ArrayType(_, _) => {
              val arrayType = field.dataType.asInstanceOf[ArrayType]
              val arrayFieldAssembler =
                if (arrayType.containsNull) fieldAssembler.array().items().nullable()
                else fieldAssembler.array().items()

              arrayType.elementType match {
                case IntegerType => arrayFieldAssembler.intType().noDefault()
                case StringType => arrayFieldAssembler.stringType().noDefault()
                case LongType => arrayFieldAssembler.longType().noDefault()
                case FloatType => arrayFieldAssembler.floatType().noDefault()
                case DoubleType => arrayFieldAssembler.doubleType().noDefault()
                case BooleanType => arrayFieldAssembler.booleanType().noDefault()
                case StructType(_) => back2(arrayType.elementType.asInstanceOf[StructType], field.name, arrayFieldAssembler)
                case _ => throw new IllegalArgumentException("Avro schema array item with unhandeled type:" + arrayType)
              }
            }
            case MapType(_, _, _) => {
              val mapType = field.dataType.asInstanceOf[MapType]

              //              recordAssembler.name().`type`().map().values().record("").fields().requiredInt().endRecord().noDefault()
              val mapFieldAssembler =
                if (mapType.valueContainsNull) fieldAssembler.map().values().nullable()
                else fieldAssembler.map().values()

              mapType.keyType match {
                case StringType =>
                case _ => throw new IllegalArgumentException("Avro schema map key has to be String")
              }

              field.dataType.asInstanceOf[MapType].valueType match {
                case IntegerType => mapFieldAssembler.intType().noDefault()
                case StringType => mapFieldAssembler.stringType().noDefault()
                case LongType => mapFieldAssembler.longType().noDefault()
                case FloatType => mapFieldAssembler.floatType().noDefault()
                case DoubleType => mapFieldAssembler.doubleType().noDefault()
                case BooleanType => mapFieldAssembler.booleanType().noDefault()
                case StructType(_) => back2(mapType.valueType.asInstanceOf[StructType], field.name, mapFieldAssembler)
                case _ => throw new IllegalArgumentException("Avro schema map value with unhandled type: " + field)
              }
            }
            case _ => throw new IllegalArgumentException("!!! StructType with unhandled type: " + field)
          }
      }

    }

    def back(structType: StructType, fieldName: String, anyRecordAssembler: Any): FieldAssembler[Schema] = {

      val recordAssembler = anyRecordAssembler.asInstanceOf[BaseFieldTypeBuilder[Schema]].record(fieldName).fields()

      addFields(structType, recordAssembler)

      recordAssembler.endRecord().noDefault()
    }

    def back2(structType: StructType, fieldName: String, anyRecordAssembler: Any): FieldAssembler[Schema] = {

      val recordAssembler = anyRecordAssembler.asInstanceOf[BaseTypeBuilder[RecordDefault[Schema]]].record(fieldName).fields()

      addFields(structType, recordAssembler)

      recordAssembler.endRecord().noDefault()
    }

    var recordAssembler = SchemaBuilder.record("RECORD").fields()

    addFields(structType, recordAssembler)
    recordAssembler.endRecord()

  }

}
