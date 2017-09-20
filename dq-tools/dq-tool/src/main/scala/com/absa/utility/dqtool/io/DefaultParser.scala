package com.absa.utility.dqtool.io

import com.absa.utility.dqtool.FileType._

class DefaultParser extends Parser {

	val INFO_FILE_HEADERS = "AUDIT_FILENAME|AUDIT_DATE|AUDIT_REC_CNT|CNTL_REC_CNT|AUDIT_HASHTOT1|CNTL_HASHTOT1|AUDIT_HASHTOT2|CNTL_HASHTOT2|COUNTRY|DATA_OWNER|SERVICE_NOW_APPLICATION|HISTORY_TYPE|VERSION|WORKFLOW_NAME|DATE_PROCESS_START|DATE_PROCESS_END|CURRENCY|SOURCE_TYPE".split("\\|")

	val CNTRL_FILE_HEADERS = "CNTL_FILENAME|CNTL_DATE|CNTL_REC_CNT|CNTL_HASHTOT1|CNTL_HASHTOT2".split("\\|")

	val AUDIT_FILE_HEADERS = "AUDIT_FILENAME|AUDIT_DATE|AUDIT_REC_CNT|AUDIT_HASHTOT1|AUDIT_HASHTOT2".split("\\|")

	/**
	 * 
	 */
	def parse(key: FileType, payload: String) : Seq[Seq[(String, String)]] = {
		val headers = if (key == AUDIT_FILE) AUDIT_FILE_HEADERS
					else if (key == CONTROL_FILE) CNTRL_FILE_HEADERS
					else INFO_FILE_HEADERS
		val lines = payload.split("\n") 
		val linesAndFields = for {
			l <- lines
			parts = headers zip (l.split("\\|") map(_.replace("\n","")))  
		} yield parts.toSeq

		linesAndFields
	}
}