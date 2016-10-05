/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import co.oddeye.core.MetriccheckRule;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.hbase.async.KeyValue;

/**
 *
 * @author vahan
 */
public class OddeeyMetricMeta {

    private String name;
    private byte[] nameTSDBUID;
    private final Map<String, OddeyeTag> tags = new HashMap<>();
    private final Cache<String, MetriccheckRule> RulesCache = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(1, TimeUnit.HOURS).build();
    private static final Gson gson = new Gson();
    private final static String Aggregator = "none";
    private final static String[] RulesDownsamples = {"1h-dev", "1h-avg", "1h-max", "1h-min"};
//    private Map<String, Object> Metricmap = new HashMap<>();
    private Map<String, String> Tagmap;

    public OddeeyMetricMeta(JsonElement json, TSDB tsdb) {
        Map<String, Object> map = gson.fromJson(json, tags.getClass());
        map.entrySet().stream().forEach((Map.Entry<String, Object> entry) -> {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.equals("tags")) {
                Map<String, Object> t_value = (Map) value;
                tags.clear();
                t_value.entrySet().stream().forEach((Map.Entry<String, Object> tag) -> {
                    if (!tag.getKey().toLowerCase().equals("alert_level")) {
                        tags.put(tag.getKey(), new OddeyeTag(tag, tsdb));
                    }
                });
            }
            if (key.equals("metric")) {
                name = String.valueOf(value);
                try {
                    nameTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, name);
                } catch (NoSuchUniqueName e) {
                    nameTSDBUID = tsdb.assignUid(key, name);
                }

            }
        });
    }

    public OddeeyMetricMeta(ArrayList<KeyValue> row, TSDB tsdb) throws Exception {
        final byte[] key = row.get(0).key();
        nameTSDBUID = Arrays.copyOfRange(key, 0, 3);
        int i = 3;
        while (i < key.length) {
            byte[] tgkey = Arrays.copyOfRange(key, i, i + 3);
            byte[] tgval = Arrays.copyOfRange(key, i + 3, i + 6);
            OddeyeTag tag = new OddeyeTag(tgkey, tgval, tsdb);
            tags.put(tag.getKey(), tag);
            i = i + 6;
        }
        name = tsdb.getUidName(UniqueId.UniqueIdType.METRIC, nameTSDBUID).join();        
        for (final KeyValue kv : row) {

            if (kv.qualifier().length != 6)
            {
                continue;
            }
            MetriccheckRule RuleItem = RulesCache.getIfPresent(Hex.encodeHexString(kv.qualifier()));
            if (RuleItem == null) {
                RuleItem = new MetriccheckRule(kv.qualifier());
            }
            // Herdakanucjun@ karevora
            byte[] b_value = Arrays.copyOfRange(kv.value(), 0, 8);
            RuleItem.update("avg", ByteBuffer.wrap(b_value).getDouble());
            b_value = Arrays.copyOfRange(kv.value(), 8, 16);
            RuleItem.update("dev", ByteBuffer.wrap(b_value).getDouble());
            b_value = Arrays.copyOfRange(kv.value(), 16, 24);
            RuleItem.update("min", ByteBuffer.wrap(b_value).getDouble());
            b_value = Arrays.copyOfRange(kv.value(), 24, 32);
            RuleItem.update("max", ByteBuffer.wrap(b_value).getDouble());

            
//                    System.out.println("Metric: " + name + "host: " + querytags.get("host") + ":" + family + ":" + R_value + " time:" + CalendarObj.getTime());
            RulesCache.put(Hex.encodeHexString(kv.qualifier()), RuleItem);

        }
//        row.
    }

    public OddeeyMetricMeta(byte[] key, TSDB tsdb) throws Exception {
        nameTSDBUID = Arrays.copyOfRange(key, 0, 3);
        int i = 3;
        while (i < key.length) {
            byte[] tgkey = Arrays.copyOfRange(key, i, i + 3);
            byte[] tgval = Arrays.copyOfRange(key, i + 3, i + 6);
            OddeyeTag tag = new OddeyeTag(tgkey, tgval, tsdb);
            tags.put(tag.getKey(), tag);
            i = i + 6;
        }
        name = tsdb.getUidName(UniqueId.UniqueIdType.METRIC, nameTSDBUID).join();
    }

    public void CalculateRules(long startdate, long enddate, TSDB tsdb) throws Exception {
        final TSQuery tsquery = new TSQuery();

        tsquery.setStart(Long.toString(startdate));
        tsquery.setEnd(Long.toString(enddate));
        final List<TagVFilter> filters = new ArrayList<>();
        final ArrayList<TSSubQuery> sub_queries = new ArrayList<>();
        final Map<String, String> querytags = new HashMap<>();
        final Map<String, SeekableView> datalist = new TreeMap<>();
        final Calendar CalendarObj = Calendar.getInstance();

        double R_value;
        String family;
        byte[] time_key;
        MetriccheckRule RuleItem;
        tags.entrySet().stream().forEach((tag) -> {
            querytags.put(tag.getKey(), tag.getValue().getValue());
        });

//        querytags.put("UUID", Metric.getAsJsonObject().get("tags").getAsJsonObject().get("UUID").getAsString());
        TagVFilter.mapToFilters(querytags, filters, true);
        for (String dsrule : RulesDownsamples) {
            final TSSubQuery sub_query = new TSSubQuery();
            sub_query.setMetric(name);
            sub_query.setAggregator(Aggregator);
            sub_query.setFilters(filters);
            sub_query.setDownsample(dsrule);
//            sub_query.setIndex(0);
            sub_queries.add(sub_query);
        }

        tsquery.setQueries(sub_queries);
        tsquery.validateAndSetQuery();
        Query[] tsdbqueries = tsquery.buildQueries(tsdb);

        final int nqueries = tsdbqueries.length;
        for (int nq = 0; nq < nqueries; nq++) {
            final DataPoints[] series = tsdbqueries[nq].run();
            for (final DataPoints datapoints : series) {
                try {
                    Tagmap = datapoints.getTags();
                    if (!Tagmap.equals(querytags)) {
                        throw new Exception("Invalid tags");
                    }
                } catch (Exception e) {
                    throw new Exception(e);
                }
                final SeekableView Datalist = datapoints.iterator();
                while (Datalist.hasNext()) {
                    final DataPoint Point = Datalist.next();
                    CalendarObj.setTimeInMillis(Point.timestamp());
                    family = sub_queries.get(datapoints.getQueryIndex()).downsamplingSpecification().getFunction().toString();
                    time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
                    R_value = Point.doubleValue();
                    RuleItem = RulesCache.getIfPresent(Hex.encodeHexString(time_key));
                    if (RuleItem == null) {
                        RuleItem = new MetriccheckRule(time_key);
                    }
                    RuleItem.update(family, R_value);
//                    System.out.println("Metric: " + name + "host: " + querytags.get("host") + ":" + family + ":" + R_value + " time:" + CalendarObj.getTime());

                    RulesCache.put(Hex.encodeHexString(time_key), RuleItem);

                }
            }
        }
    }

    public ConcurrentMap<String, MetriccheckRule> getRulesMap() {
        return RulesCache.asMap();
    }

    public void getRulePutValues(byte[][] qualifiers, byte[][] values) {
        ConcurrentMap<String, MetriccheckRule> rulesmap = RulesCache.asMap();
        qualifiers = new byte[rulesmap.size()][];
        values = new byte[rulesmap.size()][];
        int i = 0;
        for (Map.Entry<String, MetriccheckRule> rule : rulesmap.entrySet()) {
            qualifiers[i] = rule.getValue().getKey();
            values[i] = rule.getValue().getValues();
        }
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the nameTSDBUID
     */
    public byte[] getNameTSDBUID() {
        return nameTSDBUID;
    }

    /**
     * @return the key
     */
    public byte[] getKey() {
        byte[] key;
        key = nameTSDBUID;

        for (OddeyeTag tag : tags.values()) {
            key = ArrayUtils.addAll(key, tag.getKeyTSDBUID());
            key = ArrayUtils.addAll(key, tag.getValueTSDBUID());
        }
        return key;
    }

    /**
     * @return the tags
     */
    public Map<String, OddeyeTag> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof OddeeyMetricMeta) {
            final OddeeyMetricMeta o = (OddeeyMetricMeta) anObject;
            if (Arrays.equals(o.getNameTSDBUID(), nameTSDBUID) && (tags.equals(o.getTags()))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Arrays.hashCode(this.nameTSDBUID);
        hash = 53 * hash + Objects.hashCode(this.tags);
        return hash;
    }

}
