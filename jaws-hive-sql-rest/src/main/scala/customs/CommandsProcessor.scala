package customs

import scala.collection.mutable.ListBuffer
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.RandomStringUtils

class CommandsProcessor
object CommandsProcessor {
  
  val MORE_THAT_ONE_SELECT_EXCEPTION_MESSAGE = "The query must contain only one select, at the end"
  
def prepareCommands(script: String, numberOfResults : Int) = {
    val commandList = filterCommands(script)
    val commandsNb = commandList.size
    
    val firstCommands = commandList.take(commandsNb - 1) 
    val validScript = firstCommands forall(command => command.trim().toLowerCase().startsWith("select") == false)    
    if (validScript == false){
      throw new Exception(MORE_THAT_ONE_SELECT_EXCEPTION_MESSAGE)
    }
    
    firstCommands += "set hive.cli.print.header=true" 
    val lastCommand = if (commandList(commandsNb -1).trim().toLowerCase().startsWith("select")) limitQuery(numberOfResults, commandList(commandsNb -1)) else commandList(commandsNb -1)
    firstCommands += lastCommand
    firstCommands addString(new StringBuilder, ";") toString
}
  

  def filterCommands(script : String) = {
    val commandsList = ListBuffer.empty[String]
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
    return "select " + temporaryTableName + ".* from ( " + cmd + ") " + temporaryTableName + " limit " + numberOfResults
  }
}