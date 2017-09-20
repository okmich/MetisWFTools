package com.absa.utility.dqtool

import com.absa.utility.dqtool.db.HBaseLoader
import com.absa.utility.dqtool.io._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object App {

	/**
	 * 
	 */
	def main(args: Array[String]) : Unit = {
		//parse the command line args
		val opts : Map[String,String] = parseCmdLineArgs(args)
		//inputDir
		if (!opts.contains("rawdir")) throw new IllegalArgumentException("-rawdir is missing in the argument list")
		if (!opts.contains("publishdir")) throw new IllegalArgumentException("-publishdir is missing in the argument list")
		if (!opts.contains("hashtots")) throw new IllegalArgumentException("-hashtots is missing in the argument. Use '-' to indicate not applicable")
		
		//
		val publishFolder = opts("publishdir")

		val fileReader : FileReader = new DefaultFileReader(opts("rawdir"), publishFolder, new DefaultParser)
		val fileName = fileReader.getKey

		val numbers : (Long, Long, Seq[String]) = getPublishDataAttr(publishFolder, opts.getOrElse("hashtots", "-")) //returns (attribute count, record count)
		val dataPathProps = parseDataPath(publishFolder) //returns (product, filename, date)
		val publishStr = s"$fileName|${dataPathProps._3}|${numbers._1}|${numbers._2}|" + (numbers._3 mkString "|")

		// hdfs file contents
		val auditAttrs = fileReader.getAuditAttributes
		val controlAttrs = fileReader.getControlAttributes
		val infoAttrs = fileReader.getInfoAttributes
		val badRecords = fileReader.getBadRecordsFromPublish

		val publishAttrs = createPublishInfo(publishStr, infoAttrs(0))
		val dbKey = s"${dataPathProps._2}|${dataPathProps._3}".toUpperCase
		//write everything to the data store
		val loader = new HBaseLoader(dbKey, opts("zkquorum"), opts("zkclientport"))

		loader.loadProductAndFile(dataPathProps._1, dataPathProps._2)
		loader.loadAuditFile(auditAttrs)
		loader.loadControlFile(controlAttrs)
		loader.loadInfoFile(infoAttrs)
		loader.loadPublishFile(publishAttrs)

		if (badRecords != null) {
			loader.loadBadRecords(badRecords)
		}
		loader.close

		//write publishStr to file
		HDFSFileWriter.write(publishFolder, dataPathProps._2, publishStr)
	}

	/**
	*
	* @param args -
	*/
	def parseCmdLineArgs(args : Array[String]) = {
		println(args mkString "\n")
		args.map(arg => {
			val parts = arg.split(":")
			if (parts.length < 2) throw new IllegalArgumentException("Argument Usage: -rawdir:<rawdir> -publishdir:<publishdir> -hashtots:<column names separated by ,>")

			(parts(0).substring(1), parts(1))
		}).toMap
	}

	/**
	 *
	 * 
	 */
	def getPublishDataAttr(path: String, hashtots : String) : (Long, Long, Seq[String]) = {
		val sc = new SparkContext(new SparkConf)
		val sqlContext = new SQLContext(sc)

		val df : DataFrame = sqlContext.read.parquet(path).cache
		val rowCount = df.count

		val hastTots : Seq[String] = if (!hashtots.isEmpty && !hashtots.equals("-")){
			val colExprs = hashtots.split(",").map(v => (v -> "sum"))
			val aggs = df.groupBy().agg(colExprs.toMap)

			val decimalFormat = new java.text.DecimalFormat("##0.00")

			//format with java decimal format
			//return as a seq
			val rowSeq = ((aggs.take(1))(0)).toSeq map ((a: Any) => new java.math.BigDecimal(a.toString)) map (decimalFormat.format(_))

			//ensure that the size of the seq is at least 2
			if (rowSeq.size < 2) Seq(rowSeq(0), " ") else rowSeq
		} else {
			Seq(" ", " ")
		}

		(df.columns.length, rowCount, hastTots)
	}

	def createPublishInfo(publishStr: String, infoAttr: Seq[(String, String)]) : Seq[(String, String)] = {
		val infoMap = infoAttr.toMap
		val headers = "PUB_FILENAME|PUB_AUDIT_DATE|PUB_ATTR_CNT|PUB_REC_CNT|PUB_HASHTOT1|PUB_HASHTOT2".split("\\|")
		(headers zip publishStr.split("\\|")) ++ Seq(
			("COUNTRY", infoMap.getOrElse("COUNTRY", "")),
			("DATA_OWNER", infoMap.getOrElse("DATA_OWNER", "")),
			("SERVICE_NOW_APPLICATION", infoMap.getOrElse("SERVICE_NOW_APPLICATION", "")),
			("HISTORY_TYPE", infoMap.getOrElse("HISTORY_TYPE", "")),
			("VERSION", infoMap.getOrElse("VERSION", "")),
			("WORKFLOW_NAME", infoMap.getOrElse("WORKFLOW_NAME", "")),
			("DATE_PROCESS_START", infoMap.getOrElse("DATE_PROCESS_START", "")),
			("DATE_PROCESS_END", infoMap.getOrElse("DATE_PROCESS_END", "")),
			("CURRENCY", infoMap.getOrElse("CURRENCY", "")),
			("SOURCE_TYPE", infoMap.getOrElse("SOURCE_TYPE", ""))
			)
	}

	def parseDataPath(str : String) : (String, String, String) = {
		val REGEXP = """(.+)/publish/(.+)/(.+)/(.+)/([0-9]{4})/([0-9]{2})/([0-9]{2})/*""".r

		str match {
			case REGEXP(basePath, product, freq, fileName, year, month,day) => (product, fileName.toUpperCase, year+month+day)
			case _ => throw new IllegalArgumentException("the hiveDataDir specified is not valid. Should be in the form /..../filename/2017/06/06 ")
		}
	}

}
