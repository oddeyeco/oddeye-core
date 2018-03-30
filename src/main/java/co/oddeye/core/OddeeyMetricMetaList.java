/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    private static final long serialVersionUID = 465895478L;

    protected final ConcurrentHashMap<String, Integer> Tagkeys = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, Integer> Tagkeyv = new ConcurrentHashMap<>();

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
            ExecutorService executor = Executors.newCachedThreadPool();
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    executor.submit(new AddMeta(row, tsdb, client, table, this));
                }
            }
        } catch (Exception ex) {
            LOGGER.warn(globalFunctions.stackTrace(ex));
        }

    }

    public OddeeyMetricMetaList(TSDB tsdb, byte[] table, int maxThread) {
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
            ExecutorService executor = Executors.newFixedThreadPool(maxThread);
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    executor.submit(new AddMeta(row, tsdb, client, table, this));
                }
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
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
    protected OddeeyMetricMeta add(OddeeyMetricMeta e) {
        if (this.containsKey(e.hashCode())) {
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode " + e.hashCode() + " Is exist ");
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode e infa " + e.getName() + " tags " + e.getTags());
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode c infa " + this.get(e.hashCode()).getName() + " tags " + this.get(e.hashCode()).getTags());
            return null;
        }

        e.getTags().keySet().forEach((tagkey) -> {
            try {
                if (!Tagkeys.containsKey(tagkey)) {
                    Tagkeys.put(tagkey, 1);
                } else {
                    Integer count = Tagkeys.get(tagkey);
                    Tagkeys.put(tagkey, count++);
                }
            } catch (Exception ex) {
                OddeeyMetricMeta.LOGGER.warn(globalFunctions.stackTrace(ex));
                OddeeyMetricMeta.LOGGER.warn("Tagkeys " + tagkey + " ERROR e infa" + e.getName() + " tags " + e.getTags());
            }
        });

        e.getTags().entrySet().forEach((tagvalue) -> {
            try {
                if (!Tagkeyv.containsKey(tagvalue.getValue().getValue())) {
                    Tagkeyv.put(tagvalue.getValue().getValue(), 1);
                } else {
                    Integer count = Tagkeyv.get(tagvalue.getValue().getValue());
                    Tagkeyv.put(tagvalue.getValue().getValue(), count++);
                }
            } catch (Exception ex) {
                OddeeyMetricMeta.LOGGER.warn(globalFunctions.stackTrace(ex));
                OddeeyMetricMeta.LOGGER.warn("Tagkeyv " + tagvalue.getValue().getValue() + " ERROR e infa " + e.getName() + " tags " + e.getTags());
            }
        });
        return this.put(e.hashCode(), e);
    }

    public String[] getTagK() {
        String[] stockArr = new String[Tagkeys.size()];
        return Tagkeys.keySet().toArray(stockArr);
    }

    /**
     * @return the getTagkeyv
     */
    public String[] getTagkeyV() {
        String[] stockArr = new String[Tagkeyv.size()];
        return Tagkeyv.keySet().toArray(stockArr);

    }

}
