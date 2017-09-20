
import java.io.{FileOutputStream,PrintWriter}

object Util {

	def write(content: String, fileName: String) = {
		val writer = new PrintWriter(new FileOutputStream(fileName), true)
		writer.println(content)
		writer.close
	}
	
	/**
	*
	* @param args -
	*/
	def parseCmdLineArgs(args : Array[String]) = {
			println(args mkString "\n")
			args.map(arg => {
				val parts = arg.split("=")
				(parts(0).substring(1), parts(1))
			}).toMap
	}
}