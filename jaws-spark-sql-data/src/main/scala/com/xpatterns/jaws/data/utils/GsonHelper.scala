package com.xpatterns.jaws.data.utils

import java.lang.reflect.Type
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import javax.xml.bind.DatatypeConverter
import org.apache.avro.util.Utf8

object GsonHelper {

  val customGson = new GsonBuilder().registerTypeHierarchyAdapter(classOf[Array[Byte]],
    new ByteArrayToBase64TypeAdapter())
    .registerTypeHierarchyAdapter(classOf[Utf8],
      new Utf8toStrAdapter()).create();

  class ByteArrayToBase64TypeAdapter extends JsonSerializer[Array[Byte]] with JsonDeserializer[Array[Byte]] {
    def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext) = {
      DatatypeConverter.parseBase64Binary(json.getAsString())
    }

    def serialize(src: Array[Byte], typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(DatatypeConverter.printBase64Binary(src));
    }
  }

  class Utf8toStrAdapter extends JsonSerializer[Utf8] with JsonDeserializer[Utf8] {
    def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext) = {
      new Utf8(json.getAsString)
    }

    def serialize(src: Utf8, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(src.toString());
    }
  }
}