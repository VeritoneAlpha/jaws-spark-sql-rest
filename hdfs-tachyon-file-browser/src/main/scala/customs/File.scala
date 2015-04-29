package customs

import spray.json.DefaultJsonProtocol._

case class File (name : String, isFolder : Boolean)
object File {
  implicit val filesJson = jsonFormat2(apply)
}