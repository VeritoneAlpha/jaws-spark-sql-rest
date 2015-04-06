import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import customs.CommandsProcessor._
import scala.collection.mutable.ListBuffer
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class CommandsProcessorTest extends FunSuite {

  test("filterCommands : ok") {
    val filteredResults = filterCommands("use databaseName  ;show tables;  ;select * from table")
    
   assert(filteredResults.size === 3, "Different number of commands")
  assert(filteredResults === ListBuffer ("use databaseName", "show tables", "select * from table"))

  }
  
  test("prepareCommands : ok") {
    
   val tryPrepareCommands = Try (prepareCommands("use databaseName  ;show tables;  ;select * from table")) 
   val requiredCommandString = "use databaseName;show tables;set hive.cli.print.header=true;select * from table"
   
   assert(tryPrepareCommands.isSuccess, "Prepare commands failed")
   val returnedCommandString = tryPrepareCommands.get
   assert(requiredCommandString === returnedCommandString, "Commands differ")
  }

  test("prepareCommands : exception") {
    
   val tryPrepareCommands = Try (prepareCommands("use databaseName  ;show tables;  select * from table;select * from table")) 
  
   assert(tryPrepareCommands.isFailure, "Prepare commands succeeded") 
   assert(MORE_THAT_ONE_SELECT_EXCEPTION_MESSAGE === tryPrepareCommands.failed.get.getMessage(), "Different exception message!")
  }
  
}