
import java.io.{FileOutputStream,PrintWriter}

import org.json.simple.parser.JSONParser
import org.json.simple._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import Util._

object HiveTableGeneration {

	def main(args: Array[String]) = {
		var dbName : String = null
		var tblName : String = null
		var hiveDataDir : String = null
		var scriptRepoDir : String = null

		//get command line as map
		val opts = parseCmdLineArgs(args)
		try{
			dbName = opts("dbName")
			tblName = opts("tableName")
			hiveDataDir = opts("hiveDataDir")
			scriptRepoDir = opts("scriptRepoDir")
		} catch {
			case _ : Throwable => {
				println("Usage: HiveTableGeneration.scala -dbName=dbName -tableName=tableName -hiveDataDir=hiveDataDir -scriptRepoDir=scriptRepoDir [-mode=mode]")
				println("mode values: snapshot or delta")
				System.exit(-1)
			}
		}

		//initialize spark SparkConf
		val sparkConf = new SparkConf();
		val sc =  new SparkContext(sparkConf)
		//create hive context
		val hiveContext = new HiveContext(sc)
		hiveContext.sql(s"use $dbName")

		val isSnapshot =  opts.get("mode") match {
			case Some("delta") => false
			case _ => true
		}

		val hiveCreateTableCmd = HiveCreateTableScriptGenerator.
				generateCreateTableScript(opts, isSnapshot)

		println(hiveCreateTableCmd)
		//execute create table
		hiveContext.sql(hiveCreateTableCmd)
		//save the command generated
		write(hiveCreateTableCmd, scriptRepoDir + (s"/create-table-$tblName.hql"))
	}
		
}