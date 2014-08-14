package implementation

import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.impl.JawsHdfsResults
import com.xpatterns.jaws.data.impl.JawsHdfsLogging
import traits.DAL
import com.xpatterns.jaws.data.contracts.TJawsResults

/**
 * Created by emaorhian
 */
class HdfsDal (configuration : org.apache.hadoop.conf.Configuration ) extends DAL {
  val loggingDal: TJawsLogging =  new JawsHdfsLogging(configuration)
  val resultsDal: TJawsResults = new JawsHdfsResults(configuration)
}