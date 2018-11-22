package com.hbase.observer.v7;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BulkRequestBuilder;

/**
 *  目前只考虑一个表配置一个索引。
 *  联调报错 批量提交
 *  at java.util.HashMap$HashIterator.nextNode(HashMap.java:1429)
 *  java.util.ConcurrentModificationException
 */
public class HBaseDataSyncEsObserverv7 extends BaseRegionObserver {
    private static final Log log = LogFactory.getLog(HBaseDataSyncEsObserverv7.class);

    private static final String INDEXNAME = "hbaseindex";

    private static String hbaseIndexName;
    private static String hbaseTypeName;
    // private static Map<String,String> columnESName = new
    // HashMap<String,String>();
    // hbase列名，索引名和类型，索引列名。索引的mapping在保存索引配置时就创建了。
    private static Map<String, Map<String, String>> columnESName = new HashMap<String, Map<String, String>>();

    // 索引名和类型,索引的列名和列值
//	private static Map<String, Map<String, Object>> columnESValue = new HashMap<String, Map<String, Object>>();
    private static BulkRequestBuilder bulkRequestBuilder = null;

    private static void readConfiguration(CoprocessorEnvironment env) {
        String tableName = "";
        if (env instanceof RegionCoprocessorEnvironment) {
            RegionCoprocessorEnvironment envregion = (RegionCoprocessorEnvironment) env;
            tableName = envregion.getRegionInfo().getTable().getNameAsString();
        }
        Configuration conf = env.getConfiguration();
        ESClientv7.clusterName = conf.get("es_cluster");
        ESClientv7.nodeHost = conf.get("es_host");
        ESClientv7.nodePort = conf.getInt("es_port", 9300);
        log.error("ESClientv7.clusterName = :"+ESClientv7.clusterName);
        log.error("ESClientv7.nodeHost:"+ESClientv7.nodeHost);
        log.error("ESClientv7.nodePort:"+ESClientv7.nodePort);
        try {
            Table table = env.getTable(TableName.valueOf(INDEXNAME));
            Scan scan = new Scan();
            ResultScanner rs = null;
            rs = table.getScanner(scan);
            for (Result r : rs) {
                //HBase表名，索引名，索引类型
                String rowkey = Bytes.toString(r.getRow());
                String[] tabatt = rowkey.split(",");
                if (tableName.equals(tabatt[0])) {
                    hbaseIndexName = tabatt[1];
                    hbaseTypeName = tabatt[2];
                    for (Cell cell : r.rawCells()) {
                        String columnNameTemp = Bytes.toString(CellUtil.cloneQualifier(cell));
                        if (columnESName.get(columnNameTemp) == null) {
                            columnESName.put(columnNameTemp, new HashMap<String, String>());
                        }
                        String columnValueTemp = Bytes.toString(CellUtil.cloneValue(cell));
                        columnESName.get(columnNameTemp).put(tabatt[1].concat(",").concat(tabatt[2]),
                                (columnValueTemp.split(",")[0]));
                    }
                }
            }
            rs.close();
            table.close();
        } catch (IOException e) {
            String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
            log.error(fullStackTrace);
        }
    }

    /**
     * 这个方法会在regionserver打开region时候执行
     */
    public void start(CoprocessorEnvironment e) throws IOException {
        log.info("HBaseDataSyncEsObserverv7  start 111");
        readConfiguration(e);
        log.info("HBaseDataSyncEsObserverv7  start 222");
        ESClientv7.initEsClient();
        log.info("HBaseDataSyncEsObserverv7  start 333");
        bulkRequestBuilder = ESClientv7.client.prepareBulk();
        log.info("HBaseDataSyncEsObserverv7  start 444");
    }

    public void stop(CoprocessorEnvironment e) throws IOException {
        log.info("HBaseDataSyncEsObserverv7:stop 111");
        ESClientv7.closeEsClient();
        log.info("HBaseDataSyncEsObserverv7:stop 222");
        // ElasticSearchBulkOperatorv7.shutdownScheduEx();
    }

    /**
     * 不考虑一行hbase数据对应多个索引的行，简单处理
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        String indexId = new String(put.getRow());
        try {
            Map<String, String> values = new HashMap<String,String>();

            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    Map<String, String> t1 = columnESName.get(family.concat(":").concat(key));
                    if (t1 != null) {
                        // key1是索引名和类型
                        for (String key1 : t1.keySet()) {
                            values.put(t1.get(key1), value);
                        }
                    }
                }
            }
            ElasticSearchBulkOperatorv7.addUpdateBuilderToBulk
                    (ESClientv7.client.prepareUpdate(hbaseIndexName,hbaseTypeName, indexId).setDocAsUpsert(true).setDoc(values));
            //清空数值
//			for (String key3 : columnESValue.keySet()) {
//				columnESValue.put(key3, new HashMap<String, Object>());
//			}

        } catch (Exception ex) {
            String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(ex);
            log.error(fullStackTrace);
        }
    }

    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
                           Durability durability) throws IOException {
        String indexId = new String(delete.getRow());
        try {
            ElasticSearchBulkOperatorv7.addDeleteBuilderToBulk(ESClientv7.client.prepareDelete(hbaseIndexName,hbaseTypeName, indexId));
        } catch (Exception ex) {
            log.error(ex);
        }
    }
}