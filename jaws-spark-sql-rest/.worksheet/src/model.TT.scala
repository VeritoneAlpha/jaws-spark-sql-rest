package model

import spray.json._
object TT {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(90); 
  println("Welcome to the Scala worksheet");$skip(41); 
  
  val x = new Log("s", "a", 2).toJson;System.out.println("""x  : spray.json.JsValue = """ + $show(x ))}
}
