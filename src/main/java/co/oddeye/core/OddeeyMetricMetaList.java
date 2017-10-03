/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryUtil;
import net.opentsdb.uid.UniqueId;
import org.hbase.async.DeleteRequest;
import org.hbase.async.FilterList;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.KeyValue;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class OddeeyMetricMetaList extends ConcurrentHashMap<Integer, OddeeyMetricMeta> {

    protected final ArrayList<String> Tagkeys = new ArrayList();
    protected final ArrayList<String> Tagkeyv = new ArrayList();

    static final Logger LOGGER = LoggerFactory.getLogger(OddeeyMetricMetaList.class);

    public OddeeyMetricMetaList() {
        super();
    }

    public OddeeyMetricMetaList(TSDB tsdb, byte[] table, boolean OnlySpecial) {
        super();

        try {
            final HBaseClient client = tsdb.getClient();

            Scanner scanner = client.newScanner(table);
            scanner.setServerBlockCache(false);
            scanner.setMaxNumRows(1000);
            scanner.setFamily("d".getBytes());
//            scanner.setQualifier("n".getBytes());
            final byte[][] Qualifiers = new byte[][]{"n".getBytes(), "timestamp".getBytes(), "type".getBytes(), "Regression".getBytes()};
            scanner.setQualifiers(Qualifiers);

            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = scanner.nextRows(1000).joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    try {
                        OddeeyMetricMeta metric = new OddeeyMetricMeta(row, tsdb, false);
                        if (metric.isSpecial() == OnlySpecial) {
                            OddeeyMetricMeta add = add(metric);
                        }
                    } catch (InvalidKeyException e) {
                        LOGGER.warn("InvalidKeyException " + row + " Is deleted");
                        final DeleteRequest deleterequest = new DeleteRequest(table, row.get(0).key());
                        client.delete(deleterequest).joinUninterruptibly();
                    } catch (Exception e) {
                        LOGGER.warn(globalFunctions.stackTrace(e));
                        LOGGER.warn("Can not add row to metrics " + row);
                    }

                }
            }
        } catch (Exception ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));
        }
    }

    public OddeeyMetricMetaList(TSDB tsdb, byte[] table, String metricname) {
        super();

        try {
            final HBaseClient client = tsdb.getClient();

            Scanner scanner = client.newScanner(table);
            scanner.setServerBlockCache(false);
            scanner.setMaxNumRows(1000);
            scanner.setFamily("d".getBytes());
//            scanner.setQualifier("n".getBytes());
            final byte[][] Qualifiers = new byte[][]{"n".getBytes(), "timestamp".getBytes()};
            scanner.setQualifiers(Qualifiers);

            byte[] NameUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, metricname);
            StringBuilder buffer = new StringBuilder();
            buffer.append("(?s)(");
            buffer.append("\\Q");
            QueryUtil.addId(buffer, NameUID, true);
            buffer.append(")(.*)$");
            final ArrayList<ScanFilter> filters = new ArrayList<>();
            filters.add(new KeyRegexpFilter(buffer.toString()));
            scanner.setFilter(new FilterList(filters));

            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = scanner.nextRows(1000).joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    try {
                        OddeeyMetricMeta metric = new OddeeyMetricMeta(row, tsdb, false);
                        OddeeyMetricMeta add = add(metric);
                    } catch (InvalidKeyException e) {
                        LOGGER.warn("InvalidKeyException " + row + " Is deleted");
                        final DeleteRequest deleterequest = new DeleteRequest(table, row.get(0).key());
                        client.delete(deleterequest).joinUninterruptibly();
                    } catch (Exception e) {
                        LOGGER.warn(globalFunctions.stackTrace(e));
                        LOGGER.warn("Can not add row to metrics " + row);
                    }

                }
            }
        } catch (Exception ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));
        }
    }

    public OddeeyMetricMetaList(TSDB tsdb, byte[] table) {
        super();

        try {
            final HBaseClient client = tsdb.getClient();

            Scanner scanner = client.newScanner(table);
            scanner.setServerBlockCache(false);
            scanner.setMaxNumRows(10000);
            scanner.setFamily("d".getBytes());
            scanner.setQualifier("n".getBytes());
            final byte[][] Qualifiers = new byte[][]{"n".getBytes(), "timestamp".getBytes(), "type".getBytes(), "Regression".getBytes()};
            scanner.setQualifiers(Qualifiers);
            ArrayList<ArrayList<KeyValue>> rows;
            class AddMeta implements Runnable {

                private final ArrayList<KeyValue> row;

                public AddMeta(ArrayList<KeyValue> o_row) {
                    row = o_row;
                }

                @Override
                public void run() {
                    try {
                        OddeeyMetricMeta metric = new OddeeyMetricMeta(row, tsdb, false);
                        OddeeyMetricMeta add = add(metric);
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

            ExecutorService executor = Executors.newFixedThreadPool(24);
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    executor.submit(new AddMeta(row));
                }
            }
        } catch (Exception ex) {
            LOGGER.warn(globalFunctions.stackTrace(ex));
        }

    }

    public OddeeyMetricMeta set(OddeeyMetricMeta e) {
        int code = e.hashCode();
        if (code == 0) {
            LOGGER.warn("Get hash Error for " + e.getName());
            return null;
        }
        if (this.containsKey(code)) {
//          TODO Mi ban en chi            
//          return  this.get(code);
            return this.replace(code, e);
        } else {
            return this.put(code, e);
        }

    }

    /**
     * @param e
     * @return the OddeeyMetricMeta
     */
    private OddeeyMetricMeta add(OddeeyMetricMeta e) {
        if (this.containsKey(e.hashCode())) {
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode " + e.hashCode() + " Is exist ");
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode e infa" + e.getName() + " tags " + e.getTags());
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode contains infa" + this.get(e.hashCode()).getName() + " tags " + this.get(e.hashCode()).getTags());
        }

        for (String tagkey : e.getTags().keySet()) {
            try {
                if (!Tagkeys.contains(tagkey)) {
                    Tagkeys.add(tagkey);
                }
            } catch (Exception ex) {
                OddeeyMetricMeta.LOGGER.warn(globalFunctions.stackTrace(ex));
                OddeeyMetricMeta.LOGGER.warn("Tagkeys " + tagkey + " ERROR e infa" + e.getName() + " tags " + e.getTags());
            }

        }

        for (Entry<String, OddeyeTag> tagvalue : e.getTags().entrySet()) {
            try {

                if (!Tagkeyv.contains(tagvalue.getValue().getValue())) {
                    Tagkeyv.add(tagvalue.getValue().getValue());
                }
            } catch (Exception ex) {
                OddeeyMetricMeta.LOGGER.warn(globalFunctions.stackTrace(ex));
                OddeeyMetricMeta.LOGGER.warn("Tagkeyv "+tagvalue.getValue().getValue()+" ERROR e infa" + e.getName() + " tags " + e.getTags());
            }
        }
        return this.put(e.hashCode(), e);
    }

    public String[] getTagK() {
        String[] stockArr = new String[Tagkeys.size()];
        return Tagkeys.toArray(stockArr);
    }

    /**
     * @return the getTagkeyv
     */
    public String[] getTagkeyV() {
        String[] stockArr = new String[Tagkeyv.size()];
        return Tagkeyv.toArray(stockArr);

    }

}
