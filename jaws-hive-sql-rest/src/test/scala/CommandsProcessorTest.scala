import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import customs.CommandsProcessor._
import scala.collection.mutable.ListBuffer
import scala.util.Try
import org.scalatest.Matchers._
@RunWith(classOf[JUnitRunner])
class CommandsProcessorTest extends FunSuite {

  test("filterCommands : ok") {
    val filteredResults = filterCommands("use databaseName  ;show tables;  ;select * from table")

    assert(filteredResults.size === 4, "Different number of commands")
    assert(filteredResults === ListBuffer("set hive.cli.print.header=true", "use databaseName", "show tables", "select * from table"))

  }

  test("test the used regex") {
    val filteredResults = "select\\s+([\\w]+)\\.\\* from \\( select \\* from table \\) ([\\w]+) limit 2"
    "select adda.* from ( select * from table ) adda limit 2" should fullyMatch regex filteredResults

  }

  test("prepareCommands : ok-last command is a select") {

    val tryPrepareCommands = Try(prepareCommands("use databaseName  ;show tables;  ;select * from table", 2))
    val requiredCommandString = s"set hive.cli.print.header=true;select '$QUERY_DELIMITATOR';use databaseName;select '$QUERY_DELIMITATOR';show tables;select '$QUERY_DELIMITATOR';select\\s+([\\w]+)\\.\\* from \\( select \\* from table \\) ([\\w]+) limit 2;select '$QUERY_DELIMITATOR'"
    
    assert(tryPrepareCommands.isSuccess, "Prepare commands failed")
    val returnedCommandString = tryPrepareCommands.get
    returnedCommandString should fullyMatch regex requiredCommandString
  }
  
   test("prepareCommands : ok-last command is not a select") {

    val tryPrepareCommands = Try(prepareCommands("use databaseName  ;show tables;  ;show tables", 2))
    val requiredCommandString = s"set hive.cli.print.header=true;select '$QUERY_DELIMITATOR';use databaseName;select '$QUERY_DELIMITATOR';show tables;select '$QUERY_DELIMITATOR';show tables;select '$QUERY_DELIMITATOR'"
    
    assert(tryPrepareCommands.isSuccess, "Prepare commands failed")
    val returnedCommandString = tryPrepareCommands.get
    returnedCommandString should be (requiredCommandString)
  }

  test("prepareCommands : ok-one command") {

    val tryPrepareCommands = Try(prepareCommands("show databases", 2))
    val requiredCommandString = s"set hive.cli.print.header=true;select '$QUERY_DELIMITATOR';show databases;select '$QUERY_DELIMITATOR'"
    
    assert(tryPrepareCommands.isSuccess, "Prepare commands failed")
    val returnedCommandString = tryPrepareCommands.get
    returnedCommandString should fullyMatch regex requiredCommandString
  }

}