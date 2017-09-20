import org.json.simple.parser.JSONParser
import org.json.simple._

import scala.io.Source._

import Util._

/**
 * A simple scala executable that takes a list of arguments and returns a file that contains a hive create table command
 *	scala -cp jars\json-simple-1.1.1.jar HiveCreateTableScriptGenerator.scala data_samples\json\chqbal_v1.json ./chebal.hql dbName checkbal
 */
object HiveCreateTableScriptGenerator {
	
	def main(args : Array[String]) : Unit = {
		if (args.length < 5){
			println("Usage: HiveCreateTableScriptGenerator.scala -jsonFile=jsonfile -outputfile=outputfile -dbName=dbName -tableName=tblname -hiveDataDir=hiveDataDir")
			System.exit(-1)
		}

		val opts = parseCmdLineArgs(args)

		val isSnapshot =  opts.get("mode") match {
			case Some("delta") => false
			case _ => true
		}

		//generate command
		val hiveCreateTableCmd = generateCreateTableScript(opts, isSnapshot)
		//print out command to the console
		println(hiveCreateTableCmd)
		//print command out to log
		write(hiveCreateTableCmd, opts("outputfile"))
	}

	def generateCreateTableScript(opts : Map[String, String], isSnapshot: Boolean) : String = {
		val content = fromFile(opts("jsonFile")).getLines.toSeq mkString ""
		val mainObj = (new JSONParser().parse(content)).asInstanceOf[JSONObject]
		val fields =
			for {
				item <- (mainObj.get("fields").asInstanceOf[JSONArray]).toArray
				o = item.asInstanceOf[JSONObject]
				name = o.get("name")
				typ = o.get("type")
				typename = if (typ == "integer") "int" else if (typ == "long") "bigint" else if (typ == "short") "smallint" else typ
			} yield "\t" + name + " " + typename

		

		createHiveCmd(opts("dbName"), opts("tableName"), 
			opts("hiveDataDir"), fields mkString ",\n",
			!isSnapshot)
	}

	private def createHiveCmd(dbName: String, tableName:String, 
			hiveDataDir: String, fieldSpec: String, isIncremental: Boolean) : String = {
		val tbl  = if (isIncremental) tableName else tableName +"_snapshots"
		val hiveDirParts = parseDataPath(hiveDataDir)
		val viewStm = if (isIncremental) ""
					else
					   s"create view $tableName as select * from ${tbl} o where year='${hiveDirParts._2}' and month='${hiveDirParts._3}' and day='${hiveDirParts._4}';"

		//generate the command
		s"use $dbName;\n" + 
		s"create external table ${tbl} (\n" + 
			fieldSpec + "\n" + 
			") \n" + 
			"partitioned by (year int, month string, day string) \n" +
			"stored as parquet \n" + 
			s"location '${hiveDirParts._1}';" +
			"\n" + viewStm + "\n\n" +
			s"alter table $tbl add if not exists partition (year=${hiveDirParts._2}, month=${hiveDirParts._3}, day=${hiveDirParts._4}) location '$hiveDataDir';\n"
	}

	def parseDataPath(hiveDataDir : String) : (String, String, String, String) = {
		val REGEXP = """(.+)/([0-9]{4})/([0-9]{2})/([0-9]{2})/*""".r

		hiveDataDir match {
			case REGEXP(basePath, year, month,day) => (basePath, year, month, day)
			case _ => throw new IllegalArgumentException("the hiveDataDir specified is not valid. Should be in the form /..../filename/2017/06/06 ")
		}
	}
}