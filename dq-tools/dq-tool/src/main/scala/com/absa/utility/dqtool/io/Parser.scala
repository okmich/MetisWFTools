package com.absa.utility.dqtool.io

import com.absa.utility.dqtool.FileType

trait Parser {

	def parse(key: FileType.Value, payload: String) : Seq[Seq[(String, String)]]
}