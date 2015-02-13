package implementation

import org.apache.avro.SchemaBuilder.{ BaseFieldTypeBuilder, BaseTypeBuilder, FieldAssembler, RecordDefault }
import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.spark.sql.catalyst.types._
import org.apache.avro.SchemaBuilder.ArrayDefault
import org.apache.avro.SchemaBuilder.FieldDefault

object AvroConverter {
  
  private def addFields[R](structType: StructType, recordAssembler: FieldAssembler[R]) {
    structType.fields foreach {
      field =>
        val fieldAssembler =
          if (field.nullable) recordAssembler.name(field.name).`type`().nullable()
          else recordAssembler.name(field.name).`type`()
        field.dataType match {
          case ByteType | ShortType | IntegerType => fieldAssembler.intType().noDefault()
          case BinaryType => fieldAssembler.bytesType().noDefault()
          case StringType => fieldAssembler.stringType().noDefault()
          case LongType | TimestampType => fieldAssembler.longType().noDefault()
          case FloatType => fieldAssembler.floatType().noDefault()
          case DoubleType => fieldAssembler.doubleType().noDefault()
          case BooleanType => fieldAssembler.booleanType().noDefault()
          case NullType => fieldAssembler.nullType().noDefault()
          case fieldType: StructType => {
            val recAss = fieldAssembler.record(field.name).fields()
            addFields(fieldType, recAss)
            recAss.endRecord().noDefault()
          }
          case arrayType: ArrayType => {
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
              case elementType: StructType =>
                {
                  val recAss = arrayFieldAssembler.record(field.name).fields()
                  addFields(elementType, recAss)
                  recAss.endRecord().noDefault()
                }
              case _ => throw new IllegalArgumentException("Avro schema array item with unhandeled type:" + arrayType)
            }
          }
          case mapType: MapType => {
            val mapFieldAssembler =
              if (mapType.valueContainsNull) fieldAssembler.map().values().nullable()
              else fieldAssembler.map().values()

            mapType.keyType match {
              case StringType => mapFieldAssembler.stringType()
              case _ => throw new IllegalArgumentException("Avro schema map key has to be String")
            }

            mapType.valueType match {
              case IntegerType => mapFieldAssembler.intType().noDefault()
              case StringType => mapFieldAssembler.stringType().noDefault()
              case LongType => mapFieldAssembler.longType().noDefault()
              case FloatType => mapFieldAssembler.floatType().noDefault()
              case DoubleType => mapFieldAssembler.doubleType().noDefault()
              case BooleanType => mapFieldAssembler.booleanType().noDefault()
              case valueType: StructType =>
                {
                  val recAss = mapFieldAssembler.record(field.name).fields()
                  addFields(valueType, recAss)
                  recAss.endRecord().noDefault()
                }
              case _ => throw new IllegalArgumentException("Avro schema map value with unhandled type: " + field)
            }
          }
          case _ => throw new IllegalArgumentException("StructType with unhandled type: " + field)
        }
    }
  }

  def getAvroSchema(structType: StructType): Schema = {
    var recordAssembler = SchemaBuilder.record("RECORD").fields()
    addFields(structType, recordAssembler)
    recordAssembler.endRecord()
  }

}
