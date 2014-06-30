package implementation

import com.xpatterns.jaws.data.contracts.IJawsLogging
import com.xpatterns.jaws.data.impl.JawsHdfsResults
import com.xpatterns.jaws.data.impl.JawsHdfsLogging
import traits.DAL
import com.xpatterns.jaws.data.contracts.IJawsResults

/**
 * Created by emaorhian
 */
class HdfsDal (configuration : org.apache.hadoop.conf.Configuration ) extends DAL {
  val loggingDal: IJawsLogging =  new JawsHdfsLogging(configuration)
  val resultsDal: IJawsResults = new JawsHdfsResults(configuration)
}