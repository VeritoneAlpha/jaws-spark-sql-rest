package model

import spray.json._
object TT {
  println("Welcome to the Scala worksheet")
  
  val x = new Log("s", "a", 2).toJson
}