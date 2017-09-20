/**
 * 
 */
package com.absa.utility.dqtoolweb.controller;

import java.io.Serializable;

/**
 * @author cloudera
 *
 */
public class FormModel implements Serializable {

	private String date;
	private String fileName;

	public FormModel() {
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
