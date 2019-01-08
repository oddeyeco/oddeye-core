/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonArray;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.ArrayUtils;
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
public class OddeeyMetricMeta implements Serializable, Comparable<OddeeyMetricMeta>, Cloneable {

    private static final long serialVersionUID = 127895478L;

    static final Logger LOGGER = LoggerFactory.getLogger(OddeeyMetricMeta.class);
    private String name;
    private long lasttime;
    private long inittime = System.currentTimeMillis();

    private byte[] nameTSDBUID;
    private Map<String, OddeyeTag> tags = new TreeMap<>();
    private String tagsFullFilter = "";
//    private final boolean Special;
    private final Cache<String, MetriccheckRule> RulesCache = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(60, TimeUnit.MINUTES).build();
//    private final Cache<String, MetriccheckRule> RulesCalced = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(30, TimeUnit.MINUTES).build();
    private SimpleRegression regression = new SimpleRegression();

    private ArrayList<Map<String, Object>> LevelValuesList = new ArrayList<>();
    private ArrayList<Integer> LevelList = new ArrayList<>();
    private ErrorState ErrorState = new ErrorState();
    private OddeeyMetricTypesEnum type;
    private int lastreaction = 0;

    public OddeeyMetricMeta(String metricName, Map<String, String> _tags, TSDB tsdb) {
        tagsFullFilter = "";
        nameTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, metricName);
        for (Map.Entry<String, String> tag : _tags.entrySet()) {
            try {
                OddeyeTag ODtag = new OddeyeTag(tag.getKey(), tag.getValue(), tsdb);
                tags.put(tag.getKey(), ODtag);
                tagsFullFilter = tagsFullFilter + ODtag.getKey() + "=" + ODtag.getValue() + ";";
            } catch (Exception ex) {
                LOGGER.error(ex.toString());
            }
        }
        name = metricName;
        type = OddeeyMetricTypesEnum.NONE;
    }

    @Override
    public OddeeyMetricMeta clone() throws CloneNotSupportedException {
        try {
            OddeeyMetricMeta obg = (OddeeyMetricMeta) super.clone();
            obg.setErrorState(this.ErrorState.clone());
            return obg;
        } catch (CloneNotSupportedException ex) {
            throw new InternalError();
        }
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

        name = tsdb.getUidName(UniqueId.UniqueIdType.METRIC, nameTSDBUID).join();
        if (name == null) {
            throw new NullPointerException("Has not metriq name:" + Hex.encodeHexString(nameTSDBUID));
        }
        type = OddeeyMetricTypesEnum.NONE;
        for (KeyValue cell : row) {
            if (Arrays.equals(cell.qualifier(), "n".getBytes())) {
                inittime = cell.timestamp();
            }
            if (Arrays.equals(cell.qualifier(), "timestamp".getBytes())) {
                lasttime = ByteBuffer.wrap(cell.value()).getLong();
            }
            if (Arrays.equals(cell.qualifier(), "type".getBytes())) {
                type = OddeeyMetricTypesEnum.values()[ByteBuffer.wrap(cell.value()).getShort()];
            }
            if (Arrays.equals(cell.qualifier(), "Regression".getBytes())) {
                this.setSerializedRegression(cell.value());
            }
            if (Arrays.equals(cell.qualifier(), "lastreaction".getBytes())) {
                lastreaction = ByteBuffer.wrap(cell.value()).getInt();
            }

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
        type = OddeeyMetricTypesEnum.NONE;
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
        type = metric.getType();
        lasttime = metric.getTimestamp();
        lastreaction = metric.getReaction();
    }

    public OddeeyMetricMeta(OddeeyMetricMeta source) {
        Field[] fieldsSource = source.getClass().getDeclaredFields();
        Field[] fieldsTarget = this.getClass().getSuperclass().getDeclaredFields();

        for (Field fieldTarget : fieldsTarget) {
            for (Field fieldSource : fieldsSource) {
                if (fieldTarget.getName().equals(fieldSource.getName())) {
                    try {
                        if ((!java.lang.reflect.Modifier.isStatic(fieldTarget.getModifiers()))
                                && (!java.lang.reflect.Modifier.isFinal(fieldTarget.getModifiers()))) {
                            fieldTarget.set(this, fieldSource.get(source));
                        }
                    } catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
                        LOGGER.error(e.toString());
                    }
                    break;
                }
            }
        }
    }

    public Map<String, MetriccheckRule> getRules(final Calendar CalendarObj, int days, final byte[] table, final HBaseClient client) throws Exception {
        Map<String, MetriccheckRule> rules = new TreeMap<>();
        Set<String> remrules = new HashSet<>();
        MetriccheckRule Rule;

        List<ScanFilter> list = new LinkedList<>();
        int validcount = 0;
        while ((rules.size() < days * 2)) {
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
        remrules.addAll(rules.keySet());
        if (list.size() > 0) {
            FilterList filterlist = new FilterList(list, FilterList.Operator.MUST_PASS_ONE);
            GetRequest get = new GetRequest(table, getKey());
            get.setFilter(filterlist);
            ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
            if (ruledata.isEmpty()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Rule not exist in Database by " + " for " + name + " " + tags + " for " + CalendarObj.getTime() + " " + days + " days");
                }
                getRulesCache().putAll(rules);
                rules.clear();
            } else {
                Collections.reverse(ruledata);
                validcount = 0;
                for (final KeyValue kv : ruledata) {
                    if (kv.qualifier().length != 6) {
                        continue;
                    }
                    Rule = new MetriccheckRule(getKey(), kv.qualifier());
                    Rule.update(kv.value());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("get Rule from Database: " + name + " by " + MetriccheckRule.QualifierToCalendar(kv.qualifier()).getTime());
                    }

                    if (Rule.isIsValidRule()) {
                        validcount++;
                    }
                    if (validcount > days) {
                        rules.remove(Hex.encodeHexString(kv.qualifier()));
                    } else {
                        rules.put(Hex.encodeHexString(kv.qualifier()), Rule);
                        remrules.remove(Hex.encodeHexString(kv.qualifier()));
                    }
                }
            }
            rules.keySet().removeAll(remrules);
            try {
                getRulesCache().putAll(rules);
            } catch (Exception e) {
                LOGGER.info("has Emty Rules: ");
            }

        }
        return rules;

    }

    public Map<String, MetriccheckRule> getGlobalRules(final Calendar CalendarObj, int days, final byte[] table, final HBaseClient client) throws Exception {
        Map<String, MetriccheckRule> rules = new TreeMap<>();
        Set<String> remrules = new HashSet<>();
        MetriccheckRule Rule;

        List<ScanFilter> list = new LinkedList<>();
        int validcount = 0;
        while ((rules.size() < days)) {
            final byte[] time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
//            Rule = getRulesCache().getIfPresent(Hex.encodeHexString(time_key));
//            if (Rule == null) {
            Rule = new MetriccheckRule(getKey(), time_key);
            list.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(time_key)));
//            } else {
//                if (Rule.isIsValidRule()) {
//                    validcount++;
//                }
//            }
            rules.put(Hex.encodeHexString(time_key), Rule);

            if (validcount >= days) {
                break;
            }

            CalendarObj.add(Calendar.DATE, -1);
        }
//        remrules.addAll(rules.keySet());
        if (list.size() > 0) {
            FilterList filterlist = new FilterList(list, FilterList.Operator.MUST_PASS_ONE);
            GetRequest get = new GetRequest(table, ("@" + getName()).getBytes());
            get.setFilter(filterlist);
            ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
            if (ruledata.isEmpty()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Rule not exist in Database by " + " for " + name + " " + tags + " for " + CalendarObj.getTime() + " " + days + " days");
                }
                rules.clear();
            } else {
                Collections.reverse(ruledata);
                validcount = 0;
                for (final KeyValue kv : ruledata) {
                    if (kv.qualifier().length != 6) {
                        continue;
                    }
                    Rule = new MetriccheckRule(getKey(), kv.qualifier());
                    Rule.update(kv.value());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("get Rule from Database: " + name + " by " + MetriccheckRule.QualifierToCalendar(kv.qualifier()).getTime());
                    }

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
        }
        return rules;

    }

    public void getRulePutValues(byte[][] qualifiers, byte[][] values) {
        ConcurrentMap<String, MetriccheckRule> rulesmap = getRulesCache().asMap();
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
     * @return the name
     */
    public String getDisplayName() {
        return name.replaceAll("_", " ");
    }

    public String getDisplayTags(String separator) {
        String result = "";
        for (Map.Entry<String, OddeyeTag> tag : tags.entrySet()) {
            if (!tag.getKey().equals("UUID")) {
                result = result + " " + tag.getKey() + ":" + tag.getValue().getValue() + separator;
            }
        }
        return result;
    }      

    public String getDisplayTags(String separator, ArrayList<String> targetOption) {
        String result = "";
        for (Map.Entry<String, OddeyeTag> tag : tags.entrySet()) {
            if (!tag.getKey().equals("UUID")) {
                //info_tags_host_value
                if (targetOption.contains("info_tags_"+tag.getKey()+"_value" ))
                {
                    result = result + " " + tag.getKey() + ":" + tag.getValue().getValue() + separator;
                }
                
            }
        }
        return result;
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
        if (nameTSDBUID == null) {
            LOGGER.warn("nameTSDBUID is null");
            return null;
        }
        if (tags == null) {
            LOGGER.warn("tags is null");
            return null;
        }
        key = nameTSDBUID;
        for (OddeyeTag tag : tags.values()) {
            if (tag == null) {
                LOGGER.warn("tag is null");
                return null;
            }
            if (tag.getKeyTSDBUID() == null) {
                LOGGER.warn("tag.getKeyTSDBUID() is null");
                return null;
            }
            if (tag.getValueTSDBUID() == null) {
                LOGGER.warn("tag.getValueTSDBUID() is null");
                return null;
            }
            key = ArrayUtils.addAll(key, tag.getKeyTSDBUID());
            key = ArrayUtils.addAll(key, tag.getValueTSDBUID());
        }
        return key;
    }

    public byte[] getUUIDKey() {
        byte[] key;
        key = tags.get("UUID").getValueTSDBUID();
        key = ArrayUtils.addAll(key, nameTSDBUID);
        for (OddeyeTag tag : tags.values()) {
            if (!tag.getKey().equals("UUID")) {
                key = ArrayUtils.addAll(key, tag.getKeyTSDBUID());
                key = ArrayUtils.addAll(key, tag.getValueTSDBUID());
            }
        }
        return key;
    }

    public static byte[] UUIDKey2Key(byte[] uuidkey, TSDB tsdb) {
        byte[] key = Arrays.copyOfRange(uuidkey, 3, uuidkey.length);
        byte[] TSDBUUID = Arrays.copyOfRange(uuidkey, 0, 3);
        key = ArrayUtils.addAll(key, tsdb.getUID(UniqueId.UniqueIdType.TAGK, "UUID"));
        key = ArrayUtils.addAll(key, TSDBUUID);
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
            return tags.get("cluster").equals(o.getTags().get("cluster"));
        }
        return false;
    }

    @Override
    public int hashCode() {
        byte[] key = this.getKey();
        if (key == null) {
            LOGGER.warn("this.getKey() is null");
            return 0;
        }
        int hash = Objects.hashCode(Hex.encodeHexString(key));
        return hash;
    }

    
    public String sha256Code() {
        byte[] key = this.getKey();
        if (key == null) {
            LOGGER.warn("this.getKey() is null");
            return null;
        }        
        String sha256hex = DigestUtils.sha256Hex(key);
        return sha256hex;
    }    
    
    @Override
    public int compareTo(OddeeyMetricMeta o) {
        int result;

        if (isSpecial() == o.isSpecial()) {
            if (name.equals(o.getName())) {
                result = tags.toString().compareTo(o.getTags().toString());
                return result;
            }
            result = name.compareTo(o.getName());
            return result;
        }
        return type.ordinal() > o.type.ordinal() ? 1 : -1;
    }

    /**
     * @return the lasttime
     */
    public long getLasttime() {
        return lasttime;
    }

    public void setLasttime(long time) {
        lasttime = time;
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
    public final void setSerializedRegression(byte[] Bytes) throws IOException, ClassNotFoundException {
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

    /**
     * @return the Special
     */
    public boolean isSpecial() {
        return type == OddeeyMetricTypesEnum.SPECIAL;
    }

    /**
     * @return the type
     */
    public OddeeyMetricTypesEnum getType() {
        return type;
    }

    public String getTypeName() {
        return type.toString();
    }

    /**
     * @param type the type to set
     */
    public void setType(OddeeyMetricTypesEnum type) {
        this.type = type;
    }

    public void update(byte[] table, HBaseClient client) {
        try {
            GetRequest get = new GetRequest(table, getKey());
            ScanFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("timestamp".getBytes()));
            get.setFilter(filter);
            final ArrayList<KeyValue> data = client.get(get).joinUninterruptibly();
            data.stream().filter((cell) -> (Arrays.equals(cell.qualifier(), "timestamp".getBytes()))).forEachOrdered((cell) -> {
                lasttime = ByteBuffer.wrap(cell.value()).getLong();
            });
        } catch (Exception ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));
        }
    }

    /**
     * @return the LevelOddeeyMetricList
     */
    public ArrayList<Map<String, Object>> LevelValuesList() {
        return LevelValuesList;
    }

    public OddeeyMetricMeta dublicate() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream ous = new ObjectOutputStream(baos)) {
            ous.writeObject(this);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);

        return (OddeeyMetricMeta) ois.readObject();
    }

    public void update(OddeeyMetricMeta mtrscMeta) {
        if (this.lasttime < mtrscMeta.getLasttime()) {
            this.lasttime = mtrscMeta.getLasttime();
        }

    }

    /**
     * @return the RulesCache
     */
    public Cache<String, MetriccheckRule> getRulesCache() {
        return RulesCache;
    }

    /**
     * @return the inittime
     */
    public long getInittime() {
        return inittime;
    }

    public void setInittime(Long time) {
        inittime = time;
    }

    /**
     * @return the livedays
     */
    public int getLivedays() {
        return Math.toIntExact((lasttime - inittime) / (1000 * 60 * 60 * 24));
    }

    /**
     * @return the livedays
     */
    public int getSilencedays() {
        return Math.toIntExact((System.currentTimeMillis() - lasttime) / (1000 * 60 * 60 * 24));
    }

    /**
     * @return the lastreaction
     */
    public int getLastreaction() {
        return lastreaction;
    }

    /**
     * @param lastreaction the lastreaction to set
     */
    public void setLastreaction(int lastreaction) {
        this.lastreaction = lastreaction;
    }
}
