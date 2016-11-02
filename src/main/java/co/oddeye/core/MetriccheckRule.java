/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.hbase.async.KeyValue;

/**
 *
 * @author vahan
 */
public class MetriccheckRule {

    private Double min;
    private Double max;
    private Double avg;
    private Double dev;
    private final byte[] id;
    private final byte[] key;
    private final byte[] qualifier;
    private boolean isValidRule;
    private boolean hasNotData = false;

    public MetriccheckRule(byte[] p_key, byte[] p_qualifier) {
        this.isValidRule = false;

        key = p_key; //ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(houre).array();
        qualifier = p_qualifier; //qualifier = ArrayUtils.addAll(datapoints.metricUID(), tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("UUID"))); //qualifier = ArrayUtils.addAll(qualifier, tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("host")));   

        id = ArrayUtils.addAll(key, qualifier);
    }

    public MetriccheckRule(byte[] p_key) {
        this.isValidRule = false;        
        key = p_key; //ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(houre).array();        
        qualifier = null;
        id = key;
    }

    public MetriccheckRule(KeyValue data) {
        this.isValidRule = false;

        if (Arrays.equals("min".getBytes(), data.family())) {
            min = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("max".getBytes(), data.family())) {
            max = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("avg".getBytes(), data.family())) {
            avg = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("dev".getBytes(), data.family())) {
            dev = ByteBuffer.wrap(data.value()).getDouble();
        }

        key = data.key(); //ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(houre).array();
        qualifier = data.qualifier(); //qualifier = ArrayUtils.addAll(datapoints.metricUID(), tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("UUID"))); //qualifier = ArrayUtils.addAll(qualifier, tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("host")));   

        id = ArrayUtils.addAll(key, qualifier);
    }

    public MetriccheckRule update(byte[] family, byte[] value) throws Exception {

        if (Arrays.equals("min".getBytes(), family)) {
            min = ByteBuffer.wrap(value).getDouble();
        }
        if (Arrays.equals("max".getBytes(), family)) {
            max = ByteBuffer.wrap(value).getDouble();
        }
        if (Arrays.equals("avg".getBytes(), family)) {
            avg = ByteBuffer.wrap(value).getDouble();
        }
        if (Arrays.equals("dev".getBytes(), family)) {
            dev = ByteBuffer.wrap(value).getDouble();
        }

        return this;
    }

    public MetriccheckRule update(String family, Double value) throws Exception {

        if ("min".equals(family)) {
            min = value;
        }
        if ("max".equals(family)) {
            max = value;
        }
        if ("avg".equals(family)) {
            avg = value;
        }
        if ("dev".equals(family)) {
            dev = value;
        }

        isValidRule = true;

        return this;
    }

    public MetriccheckRule update(KeyValue data) throws Exception {

        if (!Arrays.equals(key, data.key())) {
            throw new Exception("Not equals key.");
        }

        if (!Arrays.equals(qualifier, data.qualifier())) {
            throw new Exception("Not equals qualifier.");
        }

        if (Arrays.equals("min".getBytes(), data.family())) {
            min = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("max".getBytes(), data.family())) {
            max = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("avg".getBytes(), data.family())) {
            avg = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("dev".getBytes(), data.family())) {
            dev = ByteBuffer.wrap(data.value()).getDouble();
        }

        return this;
    }

    @Override
    public String toString() {
        return "avg=" + String.format("%1$,.2f", avg) + "dev=" + String.format("%1$,.2f", dev) + "min=" + String.format("%1$,.2f", min) + "max=" + String.format("%1$,.2f", max);
    }

    /**
     * @return the min
     */
    public Double getMin() {
        return min;
    }

    /**
     * @return the max
     */
    public Double getMax() {
        return max;
    }

    /**
     * @return the avg
     */
    public Double getAvg() {
        return avg;
    }

    /**
     * @return the dev
     */
    public Double getDev() {
        return dev;
    }

    /**
     * @return the id
     */
    public byte[] getId() {
        return id;
    }

    public String getIdString() {
        return Hex.encodeHexString(id);
    }

    /**
     * @return the key
     */
    public byte[] getKey() {
        return key;
    }

    public MetriccheckRule update(byte[] value) throws Exception {
        // Herdakanucjun@ karevora
        hasNotData = true;
        byte[] b_value = Arrays.copyOfRange(value, 0, 8);
        Double val = ByteBuffer.wrap(b_value).getDouble();
        if (val > Double.MIN_VALUE) {
            this.update("avg", val);
        }
        b_value = Arrays.copyOfRange(value, 8, 16);
        val = ByteBuffer.wrap(b_value).getDouble();
        if (val > Double.MIN_VALUE) {
            this.update("dev", val);
        }
        b_value = Arrays.copyOfRange(value, 16, 24);
        val = ByteBuffer.wrap(b_value).getDouble();
        if (val > Double.MIN_VALUE) {
            this.update("min", val);
        }
        b_value = Arrays.copyOfRange(value, 24, 32);
        val = ByteBuffer.wrap(b_value).getDouble();
        if (val > Double.MIN_VALUE) {
            this.update("max", val);
        }
        if (isValidRule) {
            hasNotData = false;
        }

        return this;
    }

    public byte[] getValues() {
        ByteBuffer result;
        // Herdakanucjun@ karevora

        result = ByteBuffer.allocate(32);

        if (avg != null) {
            result.putDouble(avg);
        } else {
            result.putDouble(Double.MIN_VALUE);
        }
        if (dev != null) {
            result.putDouble(dev);
        } else {
            result.putDouble(Double.MIN_VALUE);
        }
        if (min != null) {
            result.putDouble(min);
        } else {
            result.putDouble(Double.MIN_VALUE);
        }
        if (max != null) {
            result.putDouble(max);
        } else {
            result.putDouble(Double.MIN_VALUE);
        }
        return result.array();
    }

    /**
     * @return the qualifier
     */
    public byte[] getQualifier() {
        return qualifier;
    }

    /**
     * @return the isValidRule
     */
    public boolean isIsValidRule() {
        return isValidRule;
    }

    /**
     * @return the hasNotData
     */
    public boolean isHasNotData() {
        return hasNotData;
    }

    /**
     * @param hasNotData the hasNotData to set
     */
    public void setHasNotData(boolean hasNotData) {
        this.hasNotData = hasNotData;
    }

}
