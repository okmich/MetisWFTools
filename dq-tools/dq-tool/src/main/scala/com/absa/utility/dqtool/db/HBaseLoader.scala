package com.absa.utility.dqtool.db


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration 
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes.toBytes

class HBaseLoader(key: String, hbaseZkQuorum: String, hbaseZkClientPort: String) {

	private val COLUMN_FAMILY_AUDIT = toBytes("aud")
	private val COLUMN_FAMILY_CONTROL = toBytes("cntrl")
	private val COLUMN_FAMILY_INFO = toBytes("raw")
	private val COLUMN_FAMILY_PUB = toBytes("pub")
	private val COLUMN_FAMILY_MAIN = toBytes("main")
	private val COLUMN_FAMILY_BAD = toBytes("bad")

	private val connection : Connection = {
		val config = HBaseConfiguration.create()
		config.set(HConstants.ZOOKEEPER_QUORUM, hbaseZkQuorum);
		config.set(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseZkClientPort);
		config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
		config.set(HConstants.ZK_SESSION_TIMEOUT, "60000");
		config.set("hbase.master.kerberos.principal", "hbase/_HOST@INTRANET.BARCAPINT.COM");
		config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@INTRANET.BARCAPINT.COM");
		config.set("hbase.rest.kerberos.principal", "hbase/_HOST@INTRANET.BARCAPINT.COM");
		config.set("hbase.thrift.kerberos.principal", "hbase/_HOST@INTRANET.BARCAPINT.COM");
		config.set("hbase.security.authentication", "kerberos");
		ConnectionFactory.createConnection(config)
	}

	private val audTable: Table = connection.getTable(TableName.valueOf("dl_data_quality:audit"))
	private val contrlTable: Table = connection.getTable(TableName.valueOf("dl_data_quality:cntl"))
	private val infoTable: Table = connection.getTable(TableName.valueOf("dl_data_quality:info"))
	private val productTable: Table = connection.getTable(TableName.valueOf("dl_data_quality:prodfile"))
	private val badRecTable: Table = connection.getTable(TableName.valueOf("dl_data_quality:badrec"))


	def loadBadRecords(data: Array[String]) : Unit = {
		var i = 0
		val len = data.length

		while (i < len){
			val put = new Put(toBytes(s"$key|$i"));
			put.addColumn(COLUMN_FAMILY_BAD, toBytes("data"), toBytes(data(i)))
       		badRecTable.put(put)
       		
       		i = i + 1;
		}
		
	}

	def loadProductAndFile(product:String, fileName: String) : Unit = {
        val put = new Put(toBytes(product + "|" + fileName));
        put.addColumn(COLUMN_FAMILY_MAIN, toBytes("value"), toBytes(1))
        productTable.put(put)
	}

	def loadAuditFile(data: Seq[Seq[(String, String)]]) : Unit = {
		data foreach (loadFile(audTable, COLUMN_FAMILY_AUDIT, _))
	}

	def loadControlFile(data: Seq[Seq[(String, String)]]) : Unit = {
		data foreach (loadFile(contrlTable,COLUMN_FAMILY_CONTROL, _))
	}

	def loadInfoFile(data: Seq[Seq[(String, String)]]) : Unit = {
		data foreach (loadFile(infoTable, COLUMN_FAMILY_INFO, _))
	}

	def loadPublishFile(data: Seq[(String, String)]) : Unit = {
		val putCmd = new Put(toBytes(key))
		for (item <- data) 
			putCmd.addColumn(COLUMN_FAMILY_PUB, toBytes(item._1), 
				toBytes(item._2))

		infoTable.put(putCmd)
	}

	def close = connection.close

	private def loadFile(tbl: Table, cf: Array[Byte], data: Seq[(String, String)]) : Unit = {
        val put = new Put(toBytes(key));
        for (item <- data) put.addColumn(cf, toBytes(item._1), toBytes(item._2))
        tbl.put(put)
	}

}