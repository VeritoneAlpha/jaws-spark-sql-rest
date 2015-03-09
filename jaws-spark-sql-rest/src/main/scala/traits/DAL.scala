package traits

import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.contracts.TJawsResults
import com.xpatterns.jaws.data.contracts.TJawsParquetTables

/**
 * Created by emaorhian
 */
trait DAL {

  def loggingDal : TJawsLogging
  def resultsDal : TJawsResults
  def parquetTableDal : TJawsParquetTables
}