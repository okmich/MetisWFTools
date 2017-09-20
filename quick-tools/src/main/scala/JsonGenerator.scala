import java.nio.file.{ Files, Paths }

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

import Util._

object JsonGenerator {

	def main(args : Array[String]) :Unit = {
		if (args.length < 4){
			println("Usage JsonGenerator -input_dir=input_dir -output_file=output_file -delimiter=delimiter -headerflag=headerflag")
			System.exit(-1)
		}

		val sparkConf = new SparkConf()
		val sc = new SparkContext(sparkConf)

		val opts = parseCmdLineArgs(args)
		println(opts)

		val sqlContext = new SQLContext(sc)
		try{
			val df = sqlContext.read.format("com.databricks.spark.csv").
					option("header", opts("headerflag")).
					option("inferSchema", "true").
					option("delimiter",  opts("delimiter")).
					option("quote",  (if (opts.contains("quote")) opts("quote") else null)).
					option("comment", null).
					load(opts("input_dir"))

			//remove any # in the entire string
			val schemaJson = df.schema.prettyJson.replace("#", "")
			Files.write(Paths.get(opts("output_file")), schemaJson.getBytes)
		} catch {
			case e : Throwable => e.printStackTrace
		}
	}

}

