package com.absa.utility.dqtool.io

import com.absa.utility.dqtool.FileType._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.net._

class DefaultFileReader(rawdir: String, publishdir: String, parser: Parser) extends FileReader {
	//
	private val fs : FileSystem = FileSystem.get(new Configuration(true))

	private val publishContent : Array[String] = {
		val pubPath = new Path(publishdir)
		if (!fs.exists(pubPath))
			throw new IllegalArgumentException(s"the publish directory $publishdir does not exist for the rawdir path $rawdir. Ensure there is -publishdir in the argument list")

		val pathItr = fs.listFiles(pubPath, false)		
		var badRecords : String = null
		while (pathItr.hasNext){
			val ipath = pathItr.next.getPath
			if (ipath.getName.endsWith("bad")){
				badRecords = readContent(ipath)
			}
		}

		if (badRecords == null)
				null 
		else badRecords.split("\n")
	}

	/**
	 *
	 */
	private val rawAuditContent : (String, String, String) = {
		val inputPath = new Path(rawdir)
		//confirm existence of the path
		if (!fs.exists(inputPath)) 
			throw new IllegalArgumentException("Input path specified does not exist")
		
		var audDetails : String = null
		var cntlDetails : String = null
		var infoDetails : String = null

		val pathItr = fs.listFiles(inputPath, false)
		while (pathItr.hasNext){
			val ipath = pathItr.next.getPath
			if (ipath.getName.endsWith("aud")){
				audDetails = readContent(ipath)
			}
			if (ipath.getName.endsWith("cntrl")){
				cntlDetails = readContent(ipath)
			}
			if (ipath.getName.endsWith("info")){
				infoDetails = readContent(ipath)
			}
		}

		if (Seq(audDetails, cntlDetails, infoDetails).forall(_ != null)) 
		   (audDetails, cntlDetails, infoDetails) 
		else 
			throw new IllegalArgumentException(s"Not all files read. Confirm that all audit, control and info file exist in $rawdir")
	}

	def exists(path: String) :Boolean = fs.exists(new Path(path))

	def getAuditAttributes : Seq[Seq[(String, String)]] = 
			parser.parse(AUDIT_FILE, rawAuditContent._1)

	def getControlAttributes : Seq[Seq[(String, String)]] = 
			parser.parse(CONTROL_FILE, rawAuditContent._2)

	def getInfoAttributes : Seq[Seq[(String, String)]] = 
			parser.parse(INFO_FILE, rawAuditContent._3)

	def getBadRecordsFromPublish : Array[String] = publishContent

	def getKey : String = {
		val audit = getAuditAttributes
		if (audit.isEmpty) throw new IllegalArgumentException("no audit read")
		audit(0)(0)._2
	}
	
	//
	private def readContent(ipath: Path) : String = {
		val fin: FSDataInputStream = fs.open(ipath)
		val available = fin.available
		val bytes : Array[Byte] = Array.ofDim(available)  
		fin.readFully(0, bytes, 0, available)
		new String(bytes)
	}
}