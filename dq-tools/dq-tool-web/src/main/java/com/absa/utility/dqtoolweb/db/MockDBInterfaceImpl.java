/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.absa.utility.dqtoolweb.db;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.stereotype.Service;

/**
 *
 * @author ABME340
 */
//@Service
public class MockDBInterfaceImpl implements DBInterface {

    private static final String LANDING = "C:\\Users\\abme340\\Downloads\\artifacts\\landing";
    private static final String PUBLISH = "C:\\Users\\abme340\\Downloads\\artifacts\\publish";

    public MockDBInterfaceImpl() {
    }

    @Override
    public List<String> getFileOccurences(String fileName) {
        return Arrays.asList("20170503");
    }

    @Override
    public List<Map<String, String>> getAuditFile(String fileName, String date) {
        String[] headers = "AUDIT_FILENAME|AUDIT_DATE|AUDIT_REC_CNT|AUDIT_HASHTOT1|AUDIT_HASHTOT2".split("\\|");
        Path path = Paths.get(LANDING, "_CQDF60.aud");

        return readFile(path, headers);
    }

    @Override
    public List<Map<String, String>> getControlFile(String fileName, String date) {
        String[] headers = "CNTL_FILENAME|CNTL_DATE|CNTL_REC_CNT|CNTL_HASHTOT1|CNTL_HASHTOT2".split("\\|");

        return readFile(Paths.get(LANDING, "_CQDF60.cntrl"), headers);
    }

    @Override
    public List<Map<String, String>> getRawInfoFile(String fileName, String date) {
        String[] headers = "AUDIT_FILENAME|AUDIT_DATE|AUDIT_REC_CNT|CNTL_REC_CNT|AUDIT_HASHTOT1|CNTL_HASHTOT1|AUDIT_HASHTOT2|CNTL_HASHTOT2|COUNTRY|DATA_OWNER|SERVICE_NOW_APPLICATION|HISTORY_TYPE|VERSION|WORKFLOW_NAME|DATE_PROCESS_START|DATE_PROCESS_END|CURRENCY|SOURCE_TYPE".split("\\|");

        return readFile(Paths.get(LANDING, "_CQDF60.info"), headers);
    }

    @Override
    public List<Map<String, String>> getPublishInfoFile(String fileName, String date) {
        String[] headers = "PUB_FILENAME|PUB_AUDIT_DATE|PUB_ATTR_CNT|PUB_REC_CNT|PUB_HASHTOT1|PUB_HASHTOT2|COUNTRY|DATA_OWNER|SERVICE_NOW_APPLICATION|HISTORY_TYPE|VERSION|WORKFLOW_NAME|DATE_PROCESS_START|DATE_PROCESS_END|CURRENCY|SOURCE_TYPE".split("\\|");

        return readFile(Paths.get(PUBLISH, "_CQDF60.info"), headers);
    }

    private List<Map<String, String>> readFile(Path path, String[] headers) {
        List<Map<String, String>> list = new ArrayList<>();
        List<String> lines;
        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
            Logger.getLogger(MockDBInterfaceImpl.class.getName()).log(Level.INFO, lines.toString());
        } catch (IOException ex) {
            Logger.getLogger(MockDBInterfaceImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }

        Map<String, String> map;
        for (String line : lines) {
            System.out.println(">> " + line);
            map = new HashMap<>();
            String[] parts = line.split("\\|");

            for (int i = 0; i < headers.length; i++) {
                map.put(headers[i], parts[i]);
            }
            list.add(map);
        }
        return list;
    }

    @Override
    public Map<String, List<String>> getProductFiles() {
        Map<String, List<String>> map = new LinkedHashMap<>();
        map.put("cheq", Arrays.asList("CHEQBAL"));
        return map;
    }

    @Override
    public List<String> getBadRecords(String fileName, String date) {
        return Arrays.asList("closed_date, open_date, corporate_branch_code, debit_interest_rate, set_off_ind, site_code");
    }
}
