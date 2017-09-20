/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.absa.utility.dqtoolweb.db;

import java.util.List;
import java.util.Map;

/**
 *
 * @author ABME340
 */
public interface DBInterface {

    /**
     *
     * @return
     */
    Map<String, List<String>> getProductFiles();

    /**
     *
     * @param fileName
     * @return
     */
    List<String> getFileOccurences(String fileName);

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    List<Map<String, String>> getAuditFile(String fileName, String date);

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    List<Map<String, String>> getControlFile(String fileName, String date);

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    List<Map<String, String>> getRawInfoFile(String fileName, String date);

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    List<Map<String, String>> getPublishInfoFile(String fileName, String date);
    
    /**
     * 
     * @param fileName
     * @param date
     * @return 
     */
    List<String> getBadRecords(String fileName, String date);
    

}
