package customs

import spray.routing._
import Directives._
import spray.http.StatusCodes.ClientError

object CustomDirectives {

  def validateCondition(condition: Boolean, message: String, rejectStatusCode: ClientError): Directive0 = {
    if (condition == false) {
      complete(rejectStatusCode, message)
    } else
      pass
  }

}