/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.io.Serializable;
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
import org.hbase.async.BinaryComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class OddeeyMetricMeta implements Serializable, Comparable<OddeeyMetricMeta> {

    static final Logger LOGGER = LoggerFactory.getLogger(OddeeyMetricMeta.class);
    private String name;
    private byte[] nameTSDBUID;
    private final Map<String, OddeyeTag> tags = new TreeMap<>();
    private String tagsFullFilter = "";
    private final Cache<String, MetriccheckRule> RulesCache = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(2, TimeUnit.HOURS).build();
    private static final Gson gson = new Gson();
    private final static String[] Aggregator = {"max", "avg", "max", "min"};
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
                        tagsFullFilter = tagsFullFilter + tag.getKey() + "=" + tag.getValue() + ";";
                    }
                });
            }
            if (key.equals("metric")) {
                name = String.valueOf(value);
                if (name == null) {
                    throw new NullPointerException("Has not metriq name:" + json.toString());
                }
                try {
                    nameTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, name);
                } catch (NoSuchUniqueName e) {
                    nameTSDBUID = tsdb.assignUid(key, name);
                }

            }
        });
    }

    public OddeeyMetricMeta(ArrayList<KeyValue> row, TSDB tsdb, boolean loadAllRules) throws Exception {
        
        final byte[] key = row.get(0).key();
        if ((key.length-3)%6 != 0 )
        {
            throw new Exception("Invalid key size:"+key.length);
        }
        nameTSDBUID = Arrays.copyOfRange(key, 0, 3);
        int i = 3;
        while (i < key.length) {
            byte[] tgkey = Arrays.copyOfRange(key, i, i + 3);
            byte[] tgval = Arrays.copyOfRange(key, i + 3, i + 6);
            OddeyeTag tag;
            try {
                tag = new OddeyeTag(tgkey, tgval, tsdb);
                tags.put(tag.getKey(), tag);
                tagsFullFilter = tagsFullFilter + tag.getKey() + "=" + tag.getValue() + ";";
            } catch (Exception e) {
                LOGGER.warn(e.toString());
            }

            i = i + 6;
        }
        name = tsdb.getUidName(UniqueId.UniqueIdType.METRIC, nameTSDBUID).join();
        if (name == null) {
            throw new NullPointerException("Has not metriq name:" + Hex.encodeHexString(nameTSDBUID));
        }

        if (loadAllRules) {
            for (final KeyValue kv : row) {
                if (kv.qualifier().length != 6) {
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
                RulesCache.put(Hex.encodeHexString(kv.qualifier()), RuleItem);

            }
        }
    }

    public OddeeyMetricMeta(byte[] key, TSDB tsdb) throws Exception {
        tagsFullFilter = "";
        nameTSDBUID = Arrays.copyOfRange(key, 0, 3);
        int i = 3;
        while (i < key.length) {
            byte[] tgkey = Arrays.copyOfRange(key, i, i + 3);
            byte[] tgval = Arrays.copyOfRange(key, i + 3, i + 6);
            OddeyeTag tag = new OddeyeTag(tgkey, tgval, tsdb);
            tags.put(tag.getKey(), tag);
            tagsFullFilter = tagsFullFilter + tag.getKey() + "=" + tag.getValue() + ";";
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
        int index;
        index = 0;
        for (String dsrule : RulesDownsamples) {
            final TSSubQuery sub_query = new TSSubQuery();
            sub_query.setMetric(name);
            sub_query.setAggregator(Aggregator[index]);
            sub_query.setFilters(filters);
            sub_query.setDownsample(dsrule);
//            sub_query.setIndex(0);
            sub_queries.add(sub_query);
            index++;
        }

        tsquery.setQueries(sub_queries);
        tsquery.validateAndSetQuery();
        Query[] tsdbqueries = tsquery.buildQueries(tsdb);

        final int nqueries = tsdbqueries.length;
        for (int nq = 0; nq < nqueries; nq++) {
            final DataPoints[] series = tsdbqueries[nq].run();
            for (final DataPoints datapoints : series) {
//                try {
//                    Tagmap = datapoints.getTags();
//                    Tagmap.remove("alert_level");
//                    if (!Tagmap.equals(querytags)) {
//                        throw new Exception("Invalid tags");
//                    }
//                } catch (Exception e) {
//                    throw new Exception(e);
//                }
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

    public void CalculateRulesAsync(long startdate, long enddate, TSDB tsdb) throws Exception {
        final TSQuery tsquery = new TSQuery();

        tsquery.setStart(Long.toString(startdate));
        tsquery.setEnd(Long.toString(enddate));
        final List<TagVFilter> filters = new ArrayList<>();
        final ArrayList<TSSubQuery> sub_queries = new ArrayList<>();
        final Map<String, String> querytags = new HashMap<>();
        final Map<String, SeekableView> datalist = new TreeMap<>();
        final Calendar CalendarObj = Calendar.getInstance();

        tags.entrySet().stream().forEach((tag) -> {
            querytags.put(tag.getKey(), tag.getValue().getValue());
        });

//        querytags.put("UUID", Metric.getAsJsonObject().get("tags").getAsJsonObject().get("UUID").getAsString());
        TagVFilter.mapToFilters(querytags, filters, true);
        int index;
        index = 0;
        for (String dsrule : RulesDownsamples) {
            final TSSubQuery sub_query = new TSSubQuery();
            sub_query.setMetric(name);
            sub_query.setAggregator(Aggregator[index]);
            sub_query.setFilters(filters);
            sub_query.setDownsample(dsrule);
//            sub_query.setIndex(0);
            sub_queries.add(sub_query);
            index++;
        }

        tsquery.setQueries(sub_queries);
        tsquery.validateAndSetQuery();
        Query[] tsdbqueries = tsquery.buildQueries(tsdb);

        final int nqueries = tsdbqueries.length;
        final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<>(nqueries);
        for (int nq = 0; nq < nqueries; nq++) {
            deferreds.add(tsdbqueries[nq].runAsync());
        }

        class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {

            @Override
            public Object call(final ArrayList<DataPoints[]> query_results)
                    throws Exception {
                double R_value;
                String family;
                byte[] time_key;
                MetriccheckRule RuleItem;

                for (DataPoints[] series : query_results) {
                    for (final DataPoints datapoints : series) {
//                        try {
//                            Tagmap = datapoints.getTags();
//                            Tagmap.remove("alert_level");
//                            if (!Tagmap.equals(querytags)) {
//                                throw new Exception("Invalid tags");
//                            }
//                        } catch (Exception e) {
//                            throw new Exception(e);
//                        }
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
                            RulesCache.put(Hex.encodeHexString(time_key), RuleItem);

                        }
                    }
                }

                return null;
            }
        }
        try {
            Deferred.groupInOrder(deferreds).addCallback(new QueriesCB())
                    .joinUninterruptibly();
        } catch (Exception e) {
            throw new RuntimeException("Shouldn't be here", e);
        }
    }

    public ConcurrentMap<String, MetriccheckRule> getRulesMap() {
        return RulesCache.asMap();
    }

    public MetriccheckRule getRule(final Calendar CalendarObj, final byte[] table, final HBaseClient client) throws Exception {
        final byte[] time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
        MetriccheckRule Rule = RulesCache.getIfPresent(Hex.encodeHexString(time_key));
        if (Rule == null) {
            Rule = new MetriccheckRule(time_key);
            GetRequest get = new GetRequest(table, getKey());
            ScanFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(time_key));
            get.setFilter(filter);
            final ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
            if (ruledata.isEmpty()) {
                LOGGER.warn("Rule by " + CalendarObj.getTime() + " for " + name + " " + tags.get("host").getValue() + " not exist in Database");
            }
            for (final KeyValue kv : ruledata) {
                if (kv.qualifier().length != 6) {
                    continue;
                }

                // Herdakanucjun@ karevora
                byte[] b_value = Arrays.copyOfRange(kv.value(), 0, 8);
                Rule.update("avg", ByteBuffer.wrap(b_value).getDouble());
                b_value = Arrays.copyOfRange(kv.value(), 8, 16);
                Rule.update("dev", ByteBuffer.wrap(b_value).getDouble());
                b_value = Arrays.copyOfRange(kv.value(), 16, 24);
                Rule.update("min", ByteBuffer.wrap(b_value).getDouble());
                b_value = Arrays.copyOfRange(kv.value(), 24, 32);
                Rule.update("max", ByteBuffer.wrap(b_value).getDouble());
                LOGGER.info("get Rule from Database: " + name + "by " + CalendarObj.getTime());
            }

            RulesCache.put(Hex.encodeHexString(time_key), Rule);
        }
        return Rule;
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

    public String getFullFilter() {
        return tagsFullFilter;
    }

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }

        if (anObject instanceof OddeeyMetricMeta) {
            final OddeeyMetricMeta o = (OddeeyMetricMeta) anObject;
            if (!Arrays.equals(o.getNameTSDBUID(), nameTSDBUID)) {
                return false;
            }
            if (!tags.get("UUID").equals(o.getTags().get("UUID"))) {
                return false;
            }
            if (!tags.get("host").equals(o.getTags().get("host"))) {
                return false;
            }
            if (!tags.get("cluster").equals(o.getTags().get("cluster"))) {
                return false;
            }

//            if (Arrays.equals(o.getNameTSDBUID(), nameTSDBUID) && (tags.equals(o.getTags()))) {
//                return true;
//            }
//            if (((tags.get("cluster").equals(o.getTags().get("cluster")))
//                    && Arrays.equals(o.getNameTSDBUID(), nameTSDBUID)
//                    && (tags.get("host").equals(o.getTags().get("host")))
//                    && (tags.get("UUID").equals(o.getTags().get("UUID")))) != (Arrays.equals(o.getNameTSDBUID(), nameTSDBUID) && (tags.equals(o.getTags())))) {
//                System.out.println(name + "-" + tags);
//            }
//            if ((tags.get("cluster").equals(o.getTags().get("cluster")))
//                    && Arrays.equals(o.getNameTSDBUID(), nameTSDBUID)
//                    && (tags.get("host").equals(o.getTags().get("host")))
//                    && (tags.get("UUID").equals(o.getTags().get("UUID")))) {
//                return true;
//            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.name);
        hash = 53 * hash + Arrays.hashCode(this.tags.get("UUID").getValueTSDBUID());
        hash = 53 * hash + Arrays.hashCode(this.tags.get("host").getValueTSDBUID());
        hash = 53 * hash + Arrays.hashCode(this.tags.get("cluster").getValueTSDBUID());
        return hash;
    }

    @Override
    public int compareTo(OddeeyMetricMeta o) {
        int result = name.compareTo(o.getName());
        return result;

    }

}