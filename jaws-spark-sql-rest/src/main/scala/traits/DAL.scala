package traits

import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.contracts.TJawsResults

/**
 * Created by emaorhian
 */
trait DAL {

  def loggingDal : TJawsLogging
  def resultsDal : TJawsResults
}