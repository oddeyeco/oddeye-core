/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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
public class OddeeyMetricMetaCalculeted extends OddeeyMetricMeta implements Serializable, Comparable<OddeeyMetricMeta>, Cloneable {

    private static final long serialVersionUID = 127895478L;

    static final Logger LOGGER = LoggerFactory.getLogger(OddeeyMetricMetaCalculeted.class);

    private final Cache<String, MetriccheckRule> RulesCalced = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(30, TimeUnit.MINUTES).build();
   
    private boolean inProcess = false;
    public OddeeyMetricMetaCalculeted(ArrayList<KeyValue> row, TSDB tsdb, boolean loadAllRules) throws Exception {
        super(row, tsdb, loadAllRules);
    }

    public OddeeyMetricMetaCalculeted(byte[] key, TSDB tsdb) throws Exception {
        super(key, tsdb);
    }

    public OddeeyMetricMetaCalculeted(OddeeyMetric metric, TSDB tsdb) throws Exception {
        super(metric, tsdb);
    }

    public void CalculateRulesApachMathSinq(long startdate, long enddate, TSDB tsdb) throws Exception {        
        final TSQuery tsquery = new TSQuery();
        final Calendar tmpCalendarObj = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        tmpCalendarObj.setTimeInMillis(startdate);
        byte[] time_key;
        while (tmpCalendarObj.getTimeInMillis() < enddate) {
            time_key = ByteBuffer.allocate(6).putShort((short) tmpCalendarObj.get(Calendar.YEAR)).putShort((short) tmpCalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) tmpCalendarObj.get(Calendar.HOUR_OF_DAY)).array();
            final MetriccheckRule RuleItem = new MetriccheckRule(getKey(), time_key);
            RuleItem.setHasNotData(true);
            getRulesCache().put(Hex.encodeHexString(time_key), RuleItem);
            tmpCalendarObj.add(Calendar.HOUR, 1);
        }

        tsquery.setStart(Long.toString(startdate));
        tsquery.setEnd(Long.toString(enddate));
        final List<TagVFilter> filters = new ArrayList<>();
        final ArrayList<TSSubQuery> sub_queries = new ArrayList<>();
        final Map<String, String> querytags = new HashMap<>();
        final Calendar CalendarObj = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        getTags().entrySet().stream().forEach((tag) -> {
            querytags.put(tag.getKey(), tag.getValue().getValue());
        });
        TagVFilter.mapToFilters(querytags, filters, true);
        final TSSubQuery sub_query = new TSSubQuery();
        sub_query.setMetric(getName());
        sub_query.setAggregator("none");
        sub_query.setFilters(filters);
        sub_queries.add(sub_query);

        tsquery.setQueries(sub_queries);
        tsquery.validateAndSetQuery();
        Query[] tsdbqueries = tsquery.buildQueries(tsdb);

        final int nqueries = tsdbqueries.length;        
        for (int nq = 0; nq < nqueries; nq++) {
            long sttime = System.currentTimeMillis();
            DataPoints[] query_results = tsdbqueries[nq].run();
            if (LOGGER.isInfoEnabled()) {
                long time = System.currentTimeMillis() - sttime;
                LOGGER.info("TSDB get " + query_results.length + " points from " + time + " ms");

                for (final DataPoints datapoints : query_results) {
                    LOGGER.info("TSDB get " + datapoints.size() + " points from " + time + " ms");
                }
            }

            double R_value;
//            byte[] time_key;
            MetriccheckRule RuleItem;
//                DescriptiveStatistics stats = new DescriptiveStatistics();
            Map<String, DescriptiveStatistics> statslist = new HashMap<>();

            for (final DataPoints datapoints : query_results) {
                final SeekableView Datalist = datapoints.iterator();
                while (Datalist.hasNext()) {
                    final DataPoint Point = Datalist.next();
                    if ((Point.timestamp() > enddate) || (Point.timestamp() < startdate)) {
                        continue;
                    }
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

            for (final Map.Entry<String, DescriptiveStatistics> stats : statslist.entrySet()) {
                final String s_time_key = stats.getKey();
                RuleItem = getRulesCache().getIfPresent(s_time_key);
                time_key = Hex.decodeHex(s_time_key.toCharArray());
                if (RuleItem == null) {

                    RuleItem = new MetriccheckRule(time_key);
                }

                RuleItem.update("avg", stats.getValue().getMean());
                RuleItem.update("dev", stats.getValue().getStandardDeviation());
                RuleItem.update("min", stats.getValue().getMin());
                RuleItem.update("max", stats.getValue().getMax());
                RuleItem.setHasNotData(false);
                getRulesCalced().put(s_time_key, RuleItem);
                getRulesCache().put(s_time_key, RuleItem);
            }

        }
    }   
    public ArrayList<Deferred<DataPoints[]>> CalculateRulesApachMath(long startdate, long enddate, TSDB tsdb) throws Exception {
        final TSQuery tsquery = new TSQuery();

        final Calendar tmpCalendarObj = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        tmpCalendarObj.setTimeInMillis(startdate);
        byte[] time_key;
        while (tmpCalendarObj.getTimeInMillis() < enddate) {
            time_key = ByteBuffer.allocate(6).putShort((short) tmpCalendarObj.get(Calendar.YEAR)).putShort((short) tmpCalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) tmpCalendarObj.get(Calendar.HOUR_OF_DAY)).array();
            final MetriccheckRule RuleItem = new MetriccheckRule(getKey(), time_key);
            RuleItem.setHasNotData(true);
            getRulesCache().put(Hex.encodeHexString(time_key), RuleItem);
            tmpCalendarObj.add(Calendar.HOUR, 1);
        }

        tsquery.setStart(Long.toString(startdate));
        tsquery.setEnd(Long.toString(enddate));
        final List<TagVFilter> filters = new ArrayList<>();
        final ArrayList<TSSubQuery> sub_queries = new ArrayList<>();
        final Map<String, String> querytags = new HashMap<>();
        final Calendar CalendarObj = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        getTags().entrySet().stream().forEach((tag) -> {
            querytags.put(tag.getKey(), tag.getValue().getValue());
        });
        TagVFilter.mapToFilters(querytags, filters, true);
        final TSSubQuery sub_query = new TSSubQuery();
        sub_query.setMetric(getName());
        sub_query.setAggregator("none");
        sub_query.setFilters(filters);
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

            private final long enddate;
            private final long startdate;

            public QueriesCB(long e_date, long s_date) {
                enddate = e_date;
                startdate = s_date;
            }

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
                            if ((Point.timestamp() > enddate) || (Point.timestamp() < startdate)) {
                                continue;
                            }
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
                    RuleItem = getRulesCache().getIfPresent(s_time_key);
                    time_key = Hex.decodeHex(s_time_key.toCharArray());
                    if (RuleItem == null) {

                        RuleItem = new MetriccheckRule(time_key);
                    }

                    RuleItem.update("avg", stats.getValue().getMean());
                    RuleItem.update("dev", stats.getValue().getStandardDeviation());
                    RuleItem.update("min", stats.getValue().getMin());
                    RuleItem.update("max", stats.getValue().getMax());
                    RuleItem.setHasNotData(false);
                    getRulesCalced().put(s_time_key, RuleItem);
                    getRulesCache().put(s_time_key, RuleItem);
                }
                return null;
            }
        }
        try {
            Deferred.groupInOrder(deferreds).addCallback(new QueriesCB(enddate, startdate));
            return deferreds;
        } catch (Exception e) {
            throw new RuntimeException("Shouldn't be here", e);
        }
    }
    public ConcurrentMap<String, MetriccheckRule> getRulesMap() {
        return getRulesCache().asMap();
    }
    public ConcurrentMap<String, MetriccheckRule> getCalcedRulesMap() {
        return getRulesCalced().asMap();
    }
    public void clearCalcedRulesMap() {
        getRulesCalced().invalidateAll();
    }
    public MetriccheckRule getRule(final Calendar CalendarObj, final byte[] table, final HBaseClient client) throws Exception {
        final byte[] time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
        MetriccheckRule Rule = getRulesCache().getIfPresent(Hex.encodeHexString(time_key));
        if (Rule == null) {
            Rule = new MetriccheckRule(time_key);
            GetRequest get = new GetRequest(table, getKey());
            ScanFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(time_key));
            get.setFilter(filter);
            final ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
            if (ruledata.isEmpty()) {
                LOGGER.warn("Rule by " + CalendarObj.getTime() + " for " + getName() + " " + getTags().get("host").getValue() + " not exist in Database");
            }
            for (final KeyValue kv : ruledata) {
                if (kv.qualifier().length != 6) {
                    continue;
                }
                Rule.update(kv.value());
                LOGGER.info("get Rule from Database: " + getName() + "by " + CalendarObj.getTime());
            }

            getRulesCache().put(Hex.encodeHexString(time_key), Rule);
        }
        return Rule;
    }
    public Map<String, MetriccheckRule> prepareRules(final Calendar CalendarObj, int days, final byte[] table, final HBaseClient client) throws Exception {
        Map<String, MetriccheckRule> rules = new TreeMap<>();
        MetriccheckRule Rule;
        List<ScanFilter> list = new LinkedList<>();
        int validcount = 0;
        while ((rules.size() < days)) {
            final byte[] time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
            Rule = getRulesCache().getIfPresent(Hex.encodeHexString(time_key));
            if (Rule == null) {
                Rule = new MetriccheckRule(getKey(), time_key);
                list.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(time_key)));
            } else {
                if (Rule.isIsValidRule()) {
                    validcount++;
                }
            }
            rules.put(Hex.encodeHexString(time_key), Rule);

            if (validcount >= days) {
                break;
            }

            CalendarObj.add(Calendar.DATE, -1);
        }
        if (list.size() > 0) {
            FilterList filterlist = new FilterList(list, FilterList.Operator.MUST_PASS_ONE);
            GetRequest get = new GetRequest(table, getKey());
            get.setFilter(filterlist);
            ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
            if (ruledata.isEmpty()) {
                LOGGER.info("Rule not exist in Database by " + " for " + getName() + " " + getTags().get("host").getValue() + " filter " + list);
                getRulesCache().putAll(rules);
            } else {
                Collections.reverse(ruledata);
                validcount = 0;
                for (final KeyValue kv : ruledata) {
                    if (kv.qualifier().length != 6) {
                        continue;
                    }
                    Rule = new MetriccheckRule(getKey(), kv.qualifier());
                    Rule.update(kv.value());

                    LOGGER.info("get Rule from Database: " + getName() + "by " + Rule.getTime().getTime());
                    if (Rule.isIsValidRule()) {
                        validcount++;
                    }
                    if (validcount > days) {
                        rules.remove(Hex.encodeHexString(kv.qualifier()));
                    } else {
                        rules.put(Hex.encodeHexString(kv.qualifier()), Rule);
                    }
                }
            }
            try {
                getRulesCache().putAll(rules);
            } catch (Exception e) {
                LOGGER.info("has Emty Rules: ");
            }

        }
        return rules;

    }

    /**
     * @return the RulesCalced
     */
    public Cache<String, MetriccheckRule> getRulesCalced() {
        return RulesCalced;
    }

    /**
     * @return the inProcess
     */
    public boolean isInProcess() {
        return inProcess;
    }

    /**
     * @param inProcess the inProcess to set
     */
    public void setInProcess(boolean inProcess) {
        this.inProcess = inProcess;
    }

}
