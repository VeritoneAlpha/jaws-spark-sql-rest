package server.api

import customs.CORSDirectives
import server.Configuration
import spray.http.{HttpMethods, HttpHeaders}
import spray.routing.{HttpService, Route}

/**
 * Handles the calls to index page
 */
trait IndexApi extends HttpService with CORSDirectives {
  /**
   * Handles the <b>/jaws/index</b> call. If the server starts successfully, this call returns a proper message.
   */
  def indexRoute: Route = path("index") {
    get {
      corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
        complete {
          "Jaws is up and running!"
        }
      }

    } ~
      options {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
          complete {
            "OK"
          }
        }
      }
  }
}
