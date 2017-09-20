/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.absa.utility.dqtoolweb.controller.rest;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.absa.utility.dqtoolweb.db.DBInterface;

/**
 *
 * @author ABME340
 */
@RestController
@RequestMapping(value = "/api", produces = MediaType.APPLICATION_JSON_VALUE)
public class DQToolRestController {

    @Autowired
    private DBInterface dbInterface;

    public DQToolRestController() {
    }

    /**
     *
     * @return
     */
    @RequestMapping(value = "/product-files", method = RequestMethod.GET)
    public Map<String, List<String>> getProductFiles() {
        return dbInterface.getProductFiles();
    }

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    @RequestMapping(value = "/audit/{fileName}/{date}", method = RequestMethod.GET)
    public List<Map<String, String>> getAuditFile(
            @PathVariable String fileName, @PathVariable String date) {
        return dbInterface.getAuditFile(fileName, date);
    }

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    @RequestMapping(value = "/cntrl/{fileName}/{date}", method = RequestMethod.GET)
    public List<Map<String, String>> getControlFile(
            @PathVariable String fileName, @PathVariable String date) {
        return dbInterface.getControlFile(fileName, date);
    }

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    @RequestMapping(value = "/info/{fileName}/raw/{date}", method = RequestMethod.GET)
    public List<Map<String, String>> getRawInfoFile(
            @PathVariable String fileName, @PathVariable String date) {
        return dbInterface.getRawInfoFile(fileName, date);
    }

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    @RequestMapping(value = "/info/{fileName}/pub/{date}", method = RequestMethod.GET)
    public List<Map<String, String>> getPublishInfoFile(
            @PathVariable String fileName, @PathVariable String date) {
        return dbInterface.getPublishInfoFile(fileName, date);
    }

    /**
     *
     * @param fileName
     * @param date
     * @return
     */
    @RequestMapping(value = "/badrecs/{fileName}/pub/{date}", method = RequestMethod.GET)
    public List<String> getBadRecords(
            @PathVariable String fileName, @PathVariable String date) {
        return dbInterface.getBadRecords(fileName, date);
    }
}
