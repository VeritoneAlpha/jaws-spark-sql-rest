package server.api

import customs.CORSDirectives
import server.Configuration
import spray.http.{HttpMethods, HttpHeaders}
import spray.routing.{HttpService, Route}

/**
 * Handles the calls to index page
 */
trait IndexApi extends HttpService with CORSDirectives {

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
