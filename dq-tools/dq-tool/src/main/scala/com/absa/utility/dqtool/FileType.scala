package com.absa.utility.dqtool

/**
 *
 *
 */
object FileType extends Enumeration {

	type FileType  = Value

	/**
	 * AUDIT_FILE
	 */
	val AUDIT_FILE = Value("audit")
	/**
	 * CONTROL_FILE
	 */
	val CONTROL_FILE = Value("control")
	/**
	 * INFO_FILE
	 */
	val INFO_FILE = Value("info")
}