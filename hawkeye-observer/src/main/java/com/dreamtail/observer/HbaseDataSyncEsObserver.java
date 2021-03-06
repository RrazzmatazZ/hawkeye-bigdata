package com.dreamtail.observer;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @author xdq
 * @version 1.0
 * @className HbaseDataSyncEsObserver
 * @description TODO
 * @date 2021/5/11 15:30
 */
public class HbaseDataSyncEsObserver implements RegionObserver, RegionCoprocessor {

    private static final Logger LOG = Logger.getLogger(HbaseDataSyncEsObserver.class);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // init ES client
        ESClient.initEsClient();
        KafkaSyncProducer.initProducer();
        LOG.info("****init start*****");
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        ESClient.closeEsClient();
        KafkaSyncProducer.destroyProducer();
        // shutdown time task
        ElasticSearchBulkOperator.shutdownScheduEx();
        LOG.info("****end*****");
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(put.getRow());
        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            Map<String, Object> infoJson = new HashMap<>();
            Map<String, Object> json = new HashMap<>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    json.put(key, value);
                }
            }
            // set hbase family to es
            infoJson.put("info", json);
            LOG.info(json.toString());
            ElasticSearchBulkOperator.addUpdateBuilderToBulk(ESClient.client.prepareUpdate("user", "dmp_ods", indexId).setDocAsUpsert(true).setDoc(json));
            LOG.info("**** postPut success*****");

            //sync request to kafka
            LOG.info("**** save Kafka sync*****");
            ProducerRecord<String, String> record = new ProducerRecord<>("sync_user_age", indexId);
            KafkaSyncProducer.producer.send(record);
            LOG.info("**** save Kafka sync success*****");

        } catch (Exception ex) {
            LOG.error("observer put  a doc, index [ " + "user" + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(delete.getRow());
        try {
            ElasticSearchBulkOperator.addDeleteBuilderToBulk(ESClient.client.prepareDelete("user", "dmp_ods", indexId));
            LOG.info("**** postDelete success*****");
        } catch (Exception ex) {
            LOG.error(ex);
            LOG.error("observer delete  a doc, index [ " + "user" + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());

        }
    }
}