package com.absa.utility.dqtool.io


trait FileReader {
	
	/**
	 * returns a string that will be a key for uniquely identifying
	 * all records read in a database
	 * - should typically be date+filename
	 * - example will be 20170617CQDF60
	 */
	def getKey: String
	def getAuditAttributes : Seq[Seq[(String, String)]]
	def getControlAttributes : Seq[Seq[(String, String)]]
	def getInfoAttributes : Seq[Seq[(String, String)]]
	
	def getBadRecordsFromPublish : Array[String]

	def exists(path: String) :Boolean
}