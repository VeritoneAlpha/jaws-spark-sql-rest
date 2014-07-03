package model
import java.util.Collection
import com.xpatterns.jaws.data.DTO.LogDTO
import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Logs (logs : Array[Log], status: String)

object Logs {
  implicit val logsJson = jsonFormat2(apply)
  
  
}