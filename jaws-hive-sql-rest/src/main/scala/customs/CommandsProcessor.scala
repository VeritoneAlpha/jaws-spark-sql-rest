package customs

import scala.collection.mutable.ListBuffer
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.RandomStringUtils
import scala.collection.mutable.ListBuffer

class CommandsProcessor
object CommandsProcessor {
  
  val MORE_THAT_ONE_SELECT_EXCEPTION_MESSAGE = "The query must contain only one select, at the end"
  val QUERY_DELIMITATOR = "_jaws_query_delimitator_"
  
def prepareCommands(script: String, numberOfResults : Int) = {
    val commandList = filterCommands(script)
    val finalScript = commandList flatMap (command => if (command.trim().toLowerCase().startsWith("select")) List(limitQuery(numberOfResults, command), s"select '$QUERY_DELIMITATOR'" ) else List(command,  s"select '$QUERY_DELIMITATOR'"))    
    
    finalScript addString(new StringBuilder, ";") toString
}
  

  def filterCommands(script : String) = {
    val commandsList = ListBuffer("set hive.cli.print.header=true")
    script.split(";").foreach(oneCmd => {
      var command = oneCmd.trim()
      val trimmedCmd = oneCmd.trim()
      if (command.endsWith("\\")) {
        command = StringUtils.chop(command) + ";"
      }

      if (StringUtils.isBlank(command) == false) {
        commandsList += command
      }

    })
    commandsList
  }
  
  def limitQuery(numberOfResults: Long, cmd: String): String = {
    val temporaryTableName = RandomStringUtils.randomAlphabetic(10)
    // take only x results
    return s"select $temporaryTableName.* from ( $cmd ) $temporaryTableName limit $numberOfResults"
  }
}