package customs

import spray.json.DefaultJsonProtocol._

case class Files (parentPath : String, files : Array[File])
object Files {
  implicit val filesJson = jsonFormat2(apply)
}