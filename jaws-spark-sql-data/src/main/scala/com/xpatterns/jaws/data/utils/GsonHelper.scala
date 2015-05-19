package com.xpatterns.jaws.data.utils

import java.lang.reflect.Type
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonParseException
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer;
import javax.xml.bind.DatatypeConverter

object GsonHelper {
  val customGson = new GsonBuilder().registerTypeHierarchyAdapter(classOf[Array[Byte]],
    new ByteArrayToBase64TypeAdapter()).create();

  class ByteArrayToBase64TypeAdapter extends JsonSerializer[Array[Byte]] with JsonDeserializer[Array[Byte]] {
    def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext) = {
      DatatypeConverter.parseBase64Binary(json.getAsString())
    }

    def serialize(src: Array[Byte], typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(DatatypeConverter.printBase64Binary(src));
    }
  }
}