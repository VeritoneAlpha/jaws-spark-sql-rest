package traits

import shark.SharkContext
import shark.api.JavaSharkContext

/**
 * Created by emaorhian
 */
trait HiveContext {
  def sharkContext : SharkContext
}