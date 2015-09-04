package server.api


import customs.CORSDirectives
import spray.http.{StatusCodes, HttpMethods, HttpHeaders}
import spray.routing._

/**
 * Handles the calls for getting the ui stored in webapp.
 */
trait UIApi extends HttpService with CORSDirectives {
  /**
   * Handles the call <b>/jaws/ui/</b> for getting the ui.
   */
  def uiRoute: Route = pathPrefix("ui") {
    // Handles the call made to /ui/ by returning the index page stored in webapp folder.
    pathSingleSlash {
      get {
        getFromResource("webapp/index.html")
      } ~ options {
        corsFilter(List("*"), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
          complete {
            "OK"
          }
        }
      }
    } ~
    // When a request is made to other resource from ui, the call is redirected to he default path
    pathEnd {
      redirect("ui/", StatusCodes.PermanentRedirect)
    } ~
    get {
      getFromResourceDirectory("webapp")
    } ~
    options {
      corsFilter(List("*"), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
        complete {
          "OK"
        }
      }
    }
  }
}
