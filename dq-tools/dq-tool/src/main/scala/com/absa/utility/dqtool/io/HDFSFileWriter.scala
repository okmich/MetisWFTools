package com.absa.utility.dqtool.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

object HDFSFileWriter {

	//
	private val fs : FileSystem = FileSystem.get(new Configuration(true))
	
	def write(writePath: String, fileName: String, payload: String) : Unit = {
		val outputStream = fs.create(new Path(writePath, s"_$fileName.info"), true)
		outputStream.writeUTF(payload)
		outputStream.flush
		outputStream.close
	}
}