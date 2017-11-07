/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import static co.oddeye.core.OddeeyMetricMetaList.LOGGER;
import java.util.ArrayList;
import net.opentsdb.core.TSDB;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

/**
 *
 * @author vahan
 */
    public class AddMeta extends Thread {

        private final ArrayList<KeyValue> row;
        private final TSDB tsdb;
        private final byte[] table;
        private final HBaseClient client;
        private final OddeeyMetricMetaList list;

        public AddMeta(ArrayList<KeyValue> o_row, TSDB o_tsdb, HBaseClient o_client, byte[] o_table,OddeeyMetricMetaList o_list) {
            row = o_row;
            tsdb = o_tsdb;
            client = o_client;
            table = o_table;
            list = o_list;
            
                    
        }

        @Override
        public void run() {
            try {
                OddeeyMetricMeta metric = new OddeeyMetricMeta(row, tsdb, false);
                OddeeyMetricMeta add = list.add(metric);
            } catch (InvalidKeyException e) {
                try {
                    LOGGER.warn("InvalidKeyException " + row + " Is deleted");
                    final DeleteRequest deleterequest = new DeleteRequest(table, row.get(0).key());
                    client.delete(deleterequest).joinUninterruptibly();
                } catch (Exception ex) {
                    LOGGER.error(globalFunctions.stackTrace(ex));
                }
            } catch (Exception e) {
                LOGGER.warn(globalFunctions.stackTrace(e));
                LOGGER.warn("Can not add row to metrics " + row);
            }

        }

    }
