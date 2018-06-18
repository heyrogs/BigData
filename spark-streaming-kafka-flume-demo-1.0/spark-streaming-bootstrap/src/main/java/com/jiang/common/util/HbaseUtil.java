package com.jiang.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ajiang
 * @create by 18-6-18 上午12:41
 */
public class HbaseUtil {

    public static final Logger log = LoggerFactory.getLogger(HbaseUtil.class);
    private static Configuration conf;

    static {
        String hbaseServer = "hbase.zookeeper.quorum";
        String serverPort = "hbase.zookeeper.property.clientPort";
        //开始配置文件
        Map<String, Object> hbaseParams = PropUtil.loadMap();
        conf = HBaseConfiguration.create();
        conf.set(hbaseServer, hbaseParams.get(hbaseServer).toString().trim());
        conf.set(serverPort, hbaseParams.get(serverPort).toString().trim());
    }


    /**
     * get table instance
     *
     * @param tableName
     * @return
     */
    public static HTable getHTable(String tableName) {
        HTable hTable = null;
        try {
            hTable = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hTable;
    }


    /**
     * query data
     *
     * @param tableName
     * @param day
     * @param family
     * @param field
     * @return
     * @throws IOException
     */
    public static Map<String, String> query(final String tableName, final String day, final String family, final String field)
            throws IOException {

        HTable table = getHTable(tableName);
        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(day));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        Map<String, String> resultMap = new HashMap<>();
        for (Result result : rs) {
            String row = Bytes.toString(result.getRow());
            //Long count = Bytes.toLong(result.getValue(Bytes.toBytes(family), Bytes.toBytes(field)), 8);
           // Integer count = Bytes.toInt(result.getValue(Bytes.toBytes(family), Bytes.toBytes(field)));
            String countStr = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(field)));
            resultMap.put(row, countStr);
        }
        return resultMap;
    }


    /**
     * 创建表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return
     */
    public static String createTable(String tableName, String columnFamily) {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName tn = TableName.valueOf(tableName);
            //判断表是否存在
            /*if (admin.tableExists(tn)) {
                log.info("table exists !");
                return "EXISTS";
            }*/
            HTableDescriptor htd = new HTableDescriptor(tn);
            htd.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(htd);
            admin.close();
            log.info("create table({}) success !", tableName);
        } catch (IOException e) {
            log.info("create table({}) fail --> {}", tableName, e.getCause());
        } finally {
            close(connection);
        }
        return "OK";
    }

    /**
     * delete
     *
     * @param tableName
     */
    public static void deleteTable(String tableName) {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
            log.info("delete table {} ok.", tableName);
        } catch (IOException e) {
            log.info("delete table {} fail.-->{}", tableName, e.getCause());
        } finally {
            close(connection);
        }
    }


    /**
     * 插入一行记录
     *
     * @param tableName 表名
     * @param rowKey    rowkey
     * @param family    列族
     * @param qualifier 列
     * @param value     列对应的值
     */
    public static void add(String tableName, String rowKey, String family, String qualifier, String value) {

        try {
            Table table = getHTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
            log.info("insert recored {} to table {} ok.", rowKey, tableName);
        } catch (IOException e) {
            log.info("insert data fail --> {}", e.getCause());
        }
    }


    /**
     * 关闭连接
     */
    public static void close(final Connection connection) {
        try {
            if (connection != null)
                connection.close();
            log.debug("hbase connection close success !");
        } catch (IOException e) {
            log.debug("hbase 关闭连接出错 -->:{}", e.getLocalizedMessage());
        }
    }


    public static void main(String[] args) throws Exception{

        String tableName = "test_data_table";
        String family = "fm";
        String qualifier = "count";

        /*deleteTable(tableName);
        createTable(tableName, family);*/
       /*add("test_data_table","20180618-"+1,family,qualifier,"10");
       add("test_data_table","20180618-"+2,family,qualifier,"20");
       add("test_data_table","20180618-"+3,family,qualifier,"30");
       add("test_data_table","20180618-"+4,family,qualifier,"40");
       add("test_data_table","20180618-"+5,family,qualifier,"50");*/
        System.out.println("args = [" + query(tableName,"20180618",family,qualifier) + "]");

    }



}
