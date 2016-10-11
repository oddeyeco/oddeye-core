/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.TSDB;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

/**
 *
 * @author vahan
 */
public class OddeeyMetricMetaList extends HashMap<Integer, OddeeyMetricMeta> {

    private final ArrayList<String> Tagkeys = new ArrayList();
    private final ArrayList<String> Tagkeyv = new ArrayList();

    public OddeeyMetricMetaList() {
        super();
    }

    public OddeeyMetricMetaList(TSDB tsdb, byte[] table) throws Exception {
        super();
        try {
            final HBaseClient client = tsdb.getClient();
            Scanner scanner = client.newScanner(table);
            scanner.setServerBlockCache(false);
            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = scanner.nextRows(1000).joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    this.add(new OddeeyMetricMeta(row, tsdb, false));
                }
            }
//        GetRequest request = new GetRequest("oddeyerules", key);

//        client.get(request);
        } catch (Exception ex) {
            throw ex;

        }
    }

//    public OddeeyMetricMetaList getbyTagK_or(String[] tagkeys) {
//        final OddeeyMetricMetaList result = new OddeeyMetricMetaList();
//        this.stream().forEach((OddeeyMetricMeta MetricMeta) -> {
//            for (String tagkey : tagkeys) {
//                if (MetricMeta.getTags().containsKey(tagkey)) {
//                    if (!result.contains(MetricMeta)) {
//                        result.add(MetricMeta);
//                    }
//                }
//            }
//        });
//        return result;
//    }
//    public OddeeyMetricMetaList getbyTagV_or(String[] tagVs) {
//        final OddeeyMetricMetaList result = new OddeeyMetricMetaList();
//        this.stream().forEach((OddeeyMetricMeta MetricMeta) -> {
//            for (String tagv : tagVs) {
//                MetricMeta.getTags().entrySet().stream().filter((tag) -> (tag.getValue().getValue().equals(tagv))).filter((_item) -> (!result.contains(MetricMeta))).forEach((_item) -> {
//                    result.add(MetricMeta);
//                });
//
//            }
//        });
//        return result;
//    }
//    public OddeeyMetricMetaList getbyTag(OddeyeTag tag) {
//        final OddeeyMetricMetaList result = new OddeeyMetricMetaList();
//        this.stream().forEach((OddeeyMetricMeta MetricMeta) -> {
//            if (MetricMeta.getTags().containsValue(tag)) {
//                if (!result.contains(MetricMeta)) {
//                    result.add(MetricMeta);
//                }
//            }
//        });
//        return result;
//    }
    public OddeeyMetricMeta set(OddeeyMetricMeta e) {
        return this.put(e.hashCode(), e);
    }

    public OddeeyMetricMeta add(OddeeyMetricMeta e) {
        if (this.containsKey(e.hashCode())) {
            OddeeyMetricMeta.LOGGER.warn("OddeeyMetricMeta vs hashcode " +e.hashCode() +" Is exist ");            
        }

        e.getTags().keySet().stream().filter((tagkey) -> (!Tagkeys.contains(tagkey))).forEach((tagkey) -> {
            Tagkeys.add(tagkey);
        });

        e.getTags().entrySet().stream().filter((tag) -> (!Tagkeyv.contains(tag.getValue().getValue()))).forEach((Map.Entry<String, OddeyeTag> tag) -> {
            Tagkeyv.add(tag.getValue().getValue());
        });

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
