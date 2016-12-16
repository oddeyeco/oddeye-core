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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.hbase.async.BinaryComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.FilterList;
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
    private long lasttime;

    private byte[] nameTSDBUID;
    private final Map<String, OddeyeTag> tags = new TreeMap<>();
    private String tagsFullFilter = "";
    private final Cache<String, MetriccheckRule> RulesCache = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(2, TimeUnit.HOURS).build();
    private final Cache<String, MetriccheckRule> RulesCalced = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(1, TimeUnit.HOURS).build();
    private static final Gson GSON = new Gson();
    private final static String[] AGGREGATOR = {"max", "avg", "max", "min"};
    private final static String[] RULESDOWNSAMPLES = {"1h-dev", "1h-avg", "1h-max", "1h-min"};
//    private Map<String, Object> Metricmap = new HashMap<>();
    private Map<String, String> Tagmap;
    private SimpleRegression regression = new SimpleRegression();
    private ArrayList<Integer> LevelList = new ArrayList();
    private ErrorState ErrorState = new ErrorState();

    public OddeeyMetricMeta(JsonElement json, TSDB tsdb) {
        Map<String, Object> map = GSON.fromJson(json, tags.getClass());
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
        if ((key.length - 3) % 6 != 0) {
            throw new InvalidKeyException("Invalid key size:" + key.length);
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
        row.stream().filter((cell) -> (Arrays.equals(cell.qualifier(), "timestamp".getBytes()))).forEachOrdered((cell) -> {
            lasttime = ByteBuffer.wrap(cell.value()).getLong();
        });
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

                RuleItem.update(kv.value());
                // Herdakanucjun@ karevora
//                byte[] b_value = Arrays.copyOfRange(kv.value(), 0, 8);
//                RuleItem.update("avg", ByteBuffer.wrap(b_value).getDouble());
//                b_value = Arrays.copyOfRange(kv.value(), 8, 16);
//                RuleItem.update("dev", ByteBuffer.wrap(b_value).getDouble());
//                b_value = Arrays.copyOfRange(kv.value(), 16, 24);
//                RuleItem.update("min", ByteBuffer.wrap(b_value).getDouble());
//                b_value = Arrays.copyOfRange(kv.value(), 24, 32);
//                RuleItem.update("max", ByteBuffer.wrap(b_value).getDouble());
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

    public OddeeyMetricMeta(OddeeyMetric metric, TSDB tsdb) throws Exception {
        name = metric.getName();
        try {
            nameTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, name);
        } catch (NoSuchUniqueName e) {
            nameTSDBUID = tsdb.assignUid("metric", name);
        }
        final Map<String, String> tM2 = Collections.unmodifiableMap(metric.getTSDBTags());

        for (Map.Entry<String, String> tag : tM2.entrySet()) {
            tags.put(tag.getKey(), new OddeyeTag(tag.getKey(), tag.getValue(), tsdb));
            tagsFullFilter = tagsFullFilter + tag.getKey() + "=" + tag.getValue() + ";";
        }
    }

    @Deprecated
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
        for (String dsrule : RULESDOWNSAMPLES) {
            final TSSubQuery sub_query = new TSSubQuery();
            sub_query.setMetric(name);
            sub_query.setAggregator(AGGREGATOR[index]);
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

    @Deprecated
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
        for (String dsrule : RULESDOWNSAMPLES) {
            final TSSubQuery sub_query = new TSSubQuery();
            sub_query.setMetric(name);
            sub_query.setAggregator(AGGREGATOR[index]);
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

    public ArrayList<Deferred<DataPoints[]>> CalculateRulesApachMath(long startdate, long enddate, TSDB tsdb) throws Exception {
        final TSQuery tsquery = new TSQuery();

        final Calendar tmpCalendarObj = Calendar.getInstance();
        tmpCalendarObj.setTimeInMillis(startdate);
        byte[] time_key;
        while (tmpCalendarObj.getTimeInMillis() < enddate) {
            time_key = ByteBuffer.allocate(6).putShort((short) tmpCalendarObj.get(Calendar.YEAR)).putShort((short) tmpCalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) tmpCalendarObj.get(Calendar.HOUR_OF_DAY)).array();
            final MetriccheckRule RuleItem = new MetriccheckRule(getKey(), time_key);
            RuleItem.setHasNotData(true);
            RulesCache.put(Hex.encodeHexString(time_key), RuleItem);
            tmpCalendarObj.add(Calendar.HOUR, 1);
        }

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
        final TSSubQuery sub_query = new TSSubQuery();
        sub_query.setMetric(name);
        sub_query.setAggregator("none");
        sub_query.setFilters(filters);
//            sub_query.setDownsample(dsrule);
//            sub_query.setIndex(0);
        sub_queries.add(sub_query);

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
                byte[] time_key;
                MetriccheckRule RuleItem;
//                DescriptiveStatistics stats = new DescriptiveStatistics();
                Map<String, DescriptiveStatistics> statslist = new HashMap<>();
                for (DataPoints[] series : query_results) {
                    for (final DataPoints datapoints : series) {
                        final SeekableView Datalist = datapoints.iterator();
                        while (Datalist.hasNext()) {
                            final DataPoint Point = Datalist.next();
                            CalendarObj.setTimeInMillis(Point.timestamp());
                            time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
                            DescriptiveStatistics stats = statslist.get(Hex.encodeHexString(time_key));
                            if (stats == null) {
                                stats = new DescriptiveStatistics();
                                statslist.put(Hex.encodeHexString(time_key), stats);
                            }
                            R_value = Point.doubleValue();
                            stats.addValue(R_value);
                            statslist.replace(Hex.encodeHexString(time_key), stats);
                        }
                    }
                }

                for (final Map.Entry<String, DescriptiveStatistics> stats : statslist.entrySet()) {
                    final String s_time_key = stats.getKey();
                    RuleItem = RulesCache.getIfPresent(s_time_key);
                    time_key = Hex.decodeHex(s_time_key.toCharArray());
                    if (RuleItem == null) {

                        RuleItem = new MetriccheckRule(time_key);
                    }

//                    double GeometricMean = stats.getValue().getGeometricMean();
//                    OddeeyMetricMeta.LOGGER.warn("Count:" + stats.getValue().getN());
                    RuleItem.update("avg", stats.getValue().getMean());
                    RuleItem.update("dev", stats.getValue().getStandardDeviation());
                    RuleItem.update("min", stats.getValue().getMin());
                    RuleItem.update("max", stats.getValue().getMax());
                    RuleItem.setHasNotData(false);
                    RulesCalced.put(s_time_key, RuleItem);
                    RulesCache.put(s_time_key, RuleItem);
//                    double aa = stats.getValue().getStandardDeviation();
                }

                return null;
            }
        }
        try {
            Deferred.groupInOrder(deferreds).addCallback(new QueriesCB());
            return deferreds;
        } catch (Exception e) {
            throw new RuntimeException("Shouldn't be here", e);
        }
    }

    public ConcurrentMap<String, MetriccheckRule> getRulesMap() {
        return RulesCache.asMap();
    }

    public ConcurrentMap<String, MetriccheckRule> getCalcedRulesMap() {
        return RulesCalced.asMap();
    }

    public void clearCalcedRulesMap() {
        RulesCalced.cleanUp();
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
                Rule.update(kv.value());
                LOGGER.info("get Rule from Database: " + name + "by " + CalendarObj.getTime());
            }

            RulesCache.put(Hex.encodeHexString(time_key), Rule);
        }
        return Rule;
    }

    public Map<String, MetriccheckRule> getRules(final Calendar CalendarObj, int days, final byte[] table, final HBaseClient client) throws Exception {
        Map<String, MetriccheckRule> rules = new TreeMap<>();
        MetriccheckRule Rule;

        List<ScanFilter> list = new LinkedList<>();
//        list.add(new ValueFilter(CompareFilter.CompareOp.GREATER,new BinaryComparator(data)));
//        list.add(new ValueFilter(CompareFilter.CompareOp.LESS,new BinaryComparator(Negdata)));
//        FilterList filterlist = new FilterList(list,FilterList.Operator.MUST_PASS_ALL);
//        scanner.setFilter(filterlist);        

        for (int i = 0; i < days; i++) {
            final byte[] time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
            Rule = RulesCache.getIfPresent(Hex.encodeHexString(time_key));
            if (Rule == null) {
                Rule = new MetriccheckRule(getKey(), time_key);
                list.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(time_key)));
            }
            rules.put(Hex.encodeHexString(time_key), Rule);

            CalendarObj.add(Calendar.DATE, -1);
        }

        if (list.size() > 0) {
            FilterList filterlist = new FilterList(list, FilterList.Operator.MUST_PASS_ONE);
            GetRequest get = new GetRequest(table, getKey());
            get.setFilter(filterlist);
            final ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
            if (ruledata.isEmpty()) {
                LOGGER.info("Rule not exist in Database by " + " for " + name + " " + tags.get("host").getValue() + " filter " + list);
            } else {
                for (final KeyValue kv : ruledata) {
                    if (kv.qualifier().length != 6) {
                        continue;
                    }
                    Rule = new MetriccheckRule(getKey(), kv.qualifier());
                    Rule.update(kv.value());
                    rules.put(Hex.encodeHexString(kv.qualifier()), Rule);
                    LOGGER.info("get Rule from Database: " + name + "by " + CalendarObj.getTime());
                }
            }

            try {
                RulesCache.putAll(rules);
            } catch (Exception e) {
                LOGGER.info("has Emty Rules: ");
            }

        }
        return rules;

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

    public byte[] getUUIDKey() {
        byte[] key;
        key = tags.get("UUID").getKeyTSDBUID() ;
        key = ArrayUtils.addAll(key, nameTSDBUID);
        for (OddeyeTag tag : tags.values()) {
            if (!tag.getKey().equals("UUID")) {
                key = ArrayUtils.addAll(key, tag.getKeyTSDBUID());
                key = ArrayUtils.addAll(key, tag.getValueTSDBUID());
            }
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
//        int hash = 5;
//        hash = 53 * hash + Objects.hashCode(this.name);
//        hash = 53 * hash + Arrays.hashCode(this.tags.get("UUID").getValueTSDBUID());
//        hash = 53 * hash + Arrays.hashCode(this.tags.get("host").getValueTSDBUID());
//        hash = 53 * hash + Arrays.hashCode(this.getKey());
        int hash = Objects.hashCode(Hex.encodeHexString(this.getKey()));
        return hash;
    }

    @Override
    public int compareTo(OddeeyMetricMeta o) {
        int result = name.compareTo(o.getName());
        return result;

    }

    /**
     * @return the lasttime
     */
    public long getLasttime() {
        return lasttime;
    }

    /**
     * @return the regression
     */
    public SimpleRegression getRegression() {
        return regression;
    }

    public void setRegression(SimpleRegression reg) {
        regression = reg;
    }

    /**
     * @return the regression
     * @throws java.io.IOException
     */
    public byte[] getSerializedRegression() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        byte[] Bytes;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(regression);
            out.flush();
            Bytes = bos.toByteArray();
            return Bytes;
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    /**
     * @param Bytes
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public void setSerializedRegression(byte[] Bytes) throws IOException, ClassNotFoundException {
        if (Bytes.length > 0) {
            ByteArrayInputStream bis = new ByteArrayInputStream(Bytes);
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                regression = (SimpleRegression) in.readObject();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException ex) {
                    // ignore close exception
                }
            }
        }
    }

    /**
     * @param Bytes
     * @return regression
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public SimpleRegression appendSerializedRegression(byte[] Bytes) throws IOException, ClassNotFoundException {
        if (Bytes.length > 0) {
            final SimpleRegression tmpregretion;
            final ByteArrayInputStream bis = new ByteArrayInputStream(Bytes);
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                tmpregretion = (SimpleRegression) in.readObject();
                regression.append(tmpregretion);
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException ex) {
                    // ignore close exception
                }
            }
        }
        return regression;
    }

    /**
     * @return the LevelList
     */
    public ArrayList<Integer> getLevelList() {
        return LevelList;
    }

    /**
     * @return the ErrorState
     */
    public ErrorState getErrorState() {
        return ErrorState;
    }

    /**
     * @param ErrorState the ErrorState to set
     */
    public void setErrorState(ErrorState ErrorState) {
        this.ErrorState = ErrorState;
    }
}
