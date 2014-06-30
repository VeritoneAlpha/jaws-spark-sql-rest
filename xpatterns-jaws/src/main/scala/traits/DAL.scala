package traits

import com.xpatterns.jaws.data.contracts.IJawsLogging
import com.xpatterns.jaws.data.contracts.IJawsResults

/**
 * Created by emaorhian
 */
trait DAL {

  def loggingDal : IJawsLogging
  def resultsDal : IJawsResults
}