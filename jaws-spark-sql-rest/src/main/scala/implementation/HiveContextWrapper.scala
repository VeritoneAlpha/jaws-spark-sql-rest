package implementation

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext

class HiveContextWrapper(sc: SparkContext) extends HiveContext(sc: SparkContext){
	
	def runMetadataSql(sql: String): Seq[String] = {
			runSqlHive(sql)
	}
  
}