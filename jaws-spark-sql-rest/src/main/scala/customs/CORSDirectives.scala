package customs

import spray.http._
import spray.routing._
import spray.http.HttpHeaders._
import spray.http.HttpMethod
/**
 * Created by emaorhian
 */
trait CORSDirectives { this: HttpService =>
  private def respondWithCORSHeaders(origin: String, rh: Seq[HttpHeader]) = {
    var headers: List[HttpHeader] = List(
      HttpHeaders.`Access-Control-Allow-Origin`(SomeOrigins(List(origin))),
      HttpHeaders.`Access-Control-Allow-Credentials`(true),
      HttpHeaders.`Access-Control-Allow-Headers`("Origin", "X-Requested-With", "Content-Type", "Accept", "apiKey", "affiliationid")
      ) ++ rh.toList

    respondWithHeaders(headers)
  }
  private def respondWithCORSHeadersAllOrigins(rh: Seq[HttpHeader]) = {
    var headers: List[HttpHeader] = List(
      HttpHeaders.`Access-Control-Allow-Origin`(AllOrigins),
      HttpHeaders.`Access-Control-Allow-Credentials`(true),
      HttpHeaders.`Access-Control-Allow-Headers`("Origin", "X-Requested-With", "Content-Type", "Accept", "apiKey", "affiliationid")
    ) ++ rh.toList

    respondWithHeaders(headers)
  }

  def corsFilter(origins: List[String], rh: HttpHeader*)(route: Route) =
    if (origins.contains("*"))
      respondWithCORSHeadersAllOrigins(rh)(route)
    else
      optionalHeaderValueByName("Origin") {
        case None =>
          route
        case Some(clientOrigin) => {
          if (origins.contains(clientOrigin))
            respondWithCORSHeaders(clientOrigin, rh)(route)
          else {
            // Maybe, a Rejection will fit better
            complete(StatusCodes.Forbidden, "Invalid origin")
          }
        }
      }
}