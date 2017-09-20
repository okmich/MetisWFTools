/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.absa.utility.dqtoolweb.db;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ABME340
 */
@Service
public class HBaseDBInterfaceImpl implements DBInterface {

    @Value("${hbase.site.xml.file}")
    private String hbaseSiteXmlFile;
    @Value("${hdfs.site.xml.file}")
    private String hdfsSiteXmlFile;
    @Value("${core.site.xml.file}")
    private String coreSiteXmlFile;

    @Value("${security.batch.user}")
    private String batchUser;
    @Value("${security.batch.keytab}")
    private String batchKeyTab;

    private Table auditFileTable;
    private Table controlFileTable;
    private Table infoFileTable;
    private Table productFileTable;
    private Table badRecordTable;

    private final byte[] COLUMN_FAMILY_AUDIT = toBytes("aud");
    private final byte[] COLUMN_FAMILY_CONTROL = toBytes("cntrl");
    private final byte[] COLUMN_FAMILY_INFO = toBytes("raw");
    private final byte[] COLUMN_FAMILY_PUB = toBytes("pub");
    private final byte[] COLUMN_FAMILY_MAIN = toBytes("main");
    private final byte[] COLUMN_FAMILY_BAD = toBytes("bad");

    public HBaseDBInterfaceImpl() {
    }

    @PostConstruct
    protected void init() {
        try {
            Configuration conf = HBaseConfiguration.create();

            conf.addResource(new Path(hbaseSiteXmlFile));
            conf.addResource(new Path(hdfsSiteXmlFile));
            conf.addResource(new Path(coreSiteXmlFile));

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation userGroupInformation = UserGroupInformation.
                    loginUserFromKeytabAndReturnUGI(batchUser, batchKeyTab);
            UserGroupInformation.setLoginUser(userGroupInformation);

            Connection connection;
            connection = ConnectionFactory.createConnection(conf);

            // create the tables
            this.auditFileTable = connection.getTable(TableName
                    .valueOf("dl_data_quality:audit"));
            this.controlFileTable = connection.getTable(TableName
                    .valueOf("dl_data_quality:cntl"));
            this.infoFileTable = connection.getTable(TableName
                    .valueOf("dl_data_quality:info"));
            this.productFileTable = connection.getTable(TableName
                    .valueOf("dl_data_quality:prodfile"));
            this.badRecordTable = connection.getTable(TableName
                    .valueOf("dl_data_quality:badrec"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> getProductFiles() {
        Map<String, List<String>> retMap = new LinkedHashMap<>();
        try {
            Scan scan = new Scan();
            scan.addFamily(COLUMN_FAMILY_MAIN);
            try (ResultScanner resultScanner = productFileTable.getScanner(scan);) {
                Result result = resultScanner.next();
                String[] productFile;
                while (result != null) {
                    String row = Bytes.toString(result.getRow());
                    if (row != null && !row.isEmpty()) {
                        productFile = row.split("\\|");
                        if (retMap.containsKey(productFile[0])) {
                            retMap.get(productFile[0]).add(productFile[1]);
                        } else {
                            //create a new list
                            List<String> fileList = new ArrayList<>();
                            fileList.add(productFile[1]);
                            //add list to map
                            retMap.put(productFile[0], fileList);
                        }
                    }
                    result = resultScanner.next();
                }
            }
            return retMap;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<String> getFileOccurences(String fileName) {
        try {
            Scan scan = new Scan();
            scan.addFamily(COLUMN_FAMILY_INFO);

            scan.setFilter(new PrefixFilter(toBytes(fileName + "|")));
            List<String> dates = new ArrayList<>();
            try (ResultScanner resultScanner = infoFileTable.getScanner(scan);) {
                Result result = resultScanner.next();
                while (result != null) {
                    String row = Bytes.toString(result.getRow());
                    if (row != null && !row.isEmpty()) {
                        dates.add(row.split("\\|")[1]);
                    }
                    result = resultScanner.next();
                }
            }

            return dates;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<Map<String, String>> getAuditFile(String fileName, String date) {
        try {
            String key = fileName.toUpperCase() + "|" + date.trim();
            Result result = auditFileTable.get(new Get(toBytes(key)));

            Map<String, String> map = new HashMap<>();
            map.put("AUDIT_FILENAME", asString(result.getValue(COLUMN_FAMILY_AUDIT, toBytes("AUDIT_FILENAME"))));
            map.put("AUDIT_DATE", asString(result.getValue(COLUMN_FAMILY_AUDIT, toBytes("AUDIT_DATE"))));
            map.put("AUDIT_REC_CNT", asString(result.getValue(COLUMN_FAMILY_AUDIT, toBytes("AUDIT_REC_CNT"))));
            map.put("AUDIT_HASHTOT1", asString(result.getValue(COLUMN_FAMILY_AUDIT, toBytes("AUDIT_HASHTOT1"))));
            map.put("AUDIT_HASHTOT2", asString(result.getValue(COLUMN_FAMILY_AUDIT, toBytes("AUDIT_HASHTOT2"))));

            return Arrays.asList(map);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<Map<String, String>> getControlFile(String fileName, String date) {
        try {
            String key = fileName.toUpperCase() + "|" + date.trim();

            Scan scan = new Scan();
            scan.addFamily(COLUMN_FAMILY_CONTROL);
            Filter filter = new PrefixFilter(toBytes(key));
            scan.setFilter(filter);

            List<Map<String, String>> listMap = new ArrayList<>();

            Map<String, String> map;
            try (ResultScanner resultScanner = controlFileTable.getScanner(scan);) {
                Result result = resultScanner.next();
                while (result != null) {
                    map = new HashMap<>();
                    map.put("CNTL_FILENAME", asString(result.getValue(COLUMN_FAMILY_CONTROL, toBytes("CNTL_FILENAME"))));
                    map.put("CNTL_DATE", asString(result.getValue(COLUMN_FAMILY_CONTROL, toBytes("CNTL_DATE"))));
                    map.put("CNTL_REC_CNT", asString(result.getValue(COLUMN_FAMILY_CONTROL, toBytes("CNTL_REC_CNT"))));
                    map.put("CNTL_HASHTOT1", asString(result.getValue(COLUMN_FAMILY_CONTROL, toBytes("CNTL_HASHTOT1"))));
                    map.put("CNTL_HASHTOT2", asString(result.getValue(COLUMN_FAMILY_CONTROL, toBytes("CNTL_HASHTOT2"))));

                    result = resultScanner.next();
                    listMap.add(map);
                }
            }
            return listMap;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<Map<String, String>> getRawInfoFile(String fileName, String date) {
        try {
            String key = fileName.toUpperCase() + "|" + date.trim();
            Result result = infoFileTable.get(new Get(toBytes(key)));

            Map<String, String> map = new HashMap<>();
            map.put("AUDIT_FILENAME", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("AUDIT_FILENAME"))));
            map.put("AUDIT_DATE", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("AUDIT_DATE"))));
            map.put("AUDIT_REC_CNT", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("AUDIT_REC_CNT"))));
            map.put("AUDIT_HASHTOT1", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("AUDIT_HASHTOT1"))));
            map.put("AUDIT_HASHTOT2", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("AUDIT_HASHTOT2"))));
            map.put("CNTL_REC_CNT", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("CNTL_REC_CNT"))));
            map.put("CNTL_HASHTOT1", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("CNTL_HASHTOT1"))));
            map.put("CNTL_HASHTOT2", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("CNTL_HASHTOT2"))));
            map.put("COUNTRY", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("COUNTRY"))));
            map.put("CURRENCY", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("CURRENCY"))));
            map.put("DATA_OWNER", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("DATA_OWNER"))));
            map.put("DATE_PROCESS_START", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("DATE_PROCESS_START"))));
            map.put("DATE_PROCESS_END", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("DATE_PROCESS_END"))));
            map.put("HISTORY_TYPE", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("HISTORY_TYPE"))));
            map.put("SERVICE_NOW_APPLICATION", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("SERVICE_NOW_APPLICATION"))));
            map.put("SOURCE_TYPE", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("SOURCE_TYPE"))));
            map.put("VERSION", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("VERSION"))));
            map.put("WORKFLOW_NAME", asString(result.getValue(COLUMN_FAMILY_INFO, toBytes("WORKFLOW_NAME"))));

            return Arrays.asList(map);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<Map<String, String>> getPublishInfoFile(String fileName,
            String date) {
        try {
            String key = fileName.toUpperCase() + "|" + date.trim();
            Result result = infoFileTable.get(new Get(toBytes(key)));

            Map<String, String> map = new HashMap<>();
            map.put("PUB_ATTR_CNT", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("PUB_ATTR_CNT"))));
            map.put("PUB_AUDIT_DATE", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("PUB_AUDIT_DATE"))));
            map.put("PUB_FILENAME", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("PUB_FILENAME"))));
            map.put("PUB_REC_CNT", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("PUB_REC_CNT"))));
            map.put("PUB_HASHTOT1", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("PUB_HASHTOT1"))));
            map.put("PUB_HASHTOT2", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("PUB_HASHTOT2"))));
            map.put("COUNTRY", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("COUNTRY"))));
            map.put("CURRENCY", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("CURRENCY"))));
            map.put("DATA_OWNER", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("DATA_OWNER"))));
            map.put("DATE_PROCESS_START", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("DATE_PROCESS_START"))));
            map.put("DATE_PROCESS_END", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("DATE_PROCESS_END"))));
            map.put("HISTORY_TYPE", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("HISTORY_TYPE"))));
            map.put("SERVICE_NOW_APPLICATION", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("SERVICE_NOW_APPLICATION"))));
            map.put("SOURCE_TYPE", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("SOURCE_TYPE"))));
            map.put("VERSION", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("VERSION"))));
            map.put("WORKFLOW_NAME", asString(result.getValue(COLUMN_FAMILY_PUB, toBytes("WORKFLOW_NAME"))));

            return Arrays.asList(map);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<String> getBadRecords(String fileName, String date) {
        String key = fileName.toUpperCase() + "|" + date.trim();
        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY_BAD);
        Filter filter = new PrefixFilter(toBytes(key));
        scan.setFilter(filter);

        List<String> recList = new ArrayList<>();
        try {
            try (ResultScanner resultScanner = badRecordTable.getScanner(scan);) {
                Result result = resultScanner.next();
                while (result != null) {
                    recList.add(asString(result.getValue(COLUMN_FAMILY_BAD, toBytes("data"))));
                    result = resultScanner.next();
                }
            }
            return recList;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     * @param bytes
     * @return
     */
    private String asString(byte[] bytes) {
        if (bytes == null) {
            return "";
        }
        return Bytes.toString(bytes);
    }
}
