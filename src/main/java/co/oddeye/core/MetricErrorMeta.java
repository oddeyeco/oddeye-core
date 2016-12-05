/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import co.oddeye.core.OddeeyMetricMeta;
import net.opentsdb.core.TSDB;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author vahan
 */
public class MetricErrorMeta extends OddeeyMetricMeta {


    private long timestamp;
    private Double value;
    private Double persent_weight;
    private Double persent_predict;
    private short weight;

    private int RecurrenceTmp = 0;    
    
    private int Recurrence1m = 0;
    private int RecurrenceLast1m = 0;
    private int Recurrence20m = 0;
    private int RecurrenceLast20m = 0;
    private int Recurrence10m = 0;
    private int RecurrenceLast10m = 0;
    private int Recurrence30m = 0;
    private int RecurrenceLast30m = 0;
    
    
    public static final short MAX_VALUE = 16;

    public MetricErrorMeta(byte[] key, TSDB tsdb) throws Exception {
        super(key, tsdb);
    }

    /**
     * @return the date
     */
    public java.util.Date getDate() {
        java.util.Date dateValue = new java.util.Date(timestamp*1000);
        return dateValue;
    }    
    
    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return the value
     */
    public Double getValue() {
        return value;
    }


    /**
     * @param value the value to set
     */
    public void setValue(Double value) {
        this.value = value;
    }

    /**
     * @return the persent_weight
     */
    public Double getPersent_weight() {
        return persent_weight;
    }

    /**
     * @param persent_weight the persent_weight to set
     */
    public void setPersent_weight(Double persent_weight) {
        this.persent_weight = persent_weight;
    }

    /**
     * @return the weight
     */
    public short getWeight() {
        return weight;
    }

    /**
     * @param weight the weight to set
     */
    public void setWeight(short weight) {
        this.weight = weight;
    }


    public String getKeyString() {
        return Hex.encodeHexString(getKey());
    }    

    /**
     * @return the Recurrence1m
     */
    public int getRecurrence1m() {
        return Recurrence1m;
    }

    /**
     * @param Recurrence1m the Recurrence1m to set
     */
    public void setRecurrence1m(int Recurrence1m) {
        this.Recurrence1m = Recurrence1m;
    }

    /**
     * @return the RecurrenceLast1m
     */
    public int getRecurrenceLast1m() {
        return RecurrenceLast1m;
    }

    /**
     * @param RecurrenceLast1m the RecurrenceLast1m to set
     */
    public void setRecurrenceLast1m(int RecurrenceLast1m) {
        this.RecurrenceLast1m = RecurrenceLast1m;
    }

    /**
     * @return the Recurrence20m
     */
    public int getRecurrence20m() {
        return Recurrence20m;
    }

    /**
     * @param Recurrence20m the Recurrence20m to set
     */
    public void setRecurrence20m(int Recurrence20m) {
        this.Recurrence20m = Recurrence20m;
    }

    /**
     * @return the RecurrenceLast20m
     */
    public int getRecurrenceLast20m() {
        return RecurrenceLast20m;
    }

    /**
     * @param RecurrenceLast20m the RecurrenceLast20m to set
     */
    public void setRecurrenceLast20m(int RecurrenceLast20m) {
        this.RecurrenceLast20m = RecurrenceLast20m;
    }

    /**
     * @return the Recurrence10m
     */
    public int getRecurrence10m() {
        return Recurrence10m;
    }

    /**
     * @param Recurrence10m the Recurrence10m to set
     */
    public void setRecurrence10m(int Recurrence10m) {
        this.Recurrence10m = Recurrence10m;
    }

    /**
     * @return the RecurrenceLast10m
     */
    public int getRecurrenceLast10m() {
        return RecurrenceLast10m;
    }

    /**
     * @param RecurrenceLast10m the RecurrenceLast10m to set
     */
    public void setRecurrenceLast10m(int RecurrenceLast10m) {
        this.RecurrenceLast10m = RecurrenceLast10m;
    }

    /**
     * @return the Recurrence30m
     */
    public int getRecurrence30m() {
        return Recurrence30m;
    }

    /**
     * @param Recurrence30m the Recurrence30m to set
     */
    public void setRecurrence30m(int Recurrence30m) {
        this.Recurrence30m = Recurrence30m;
    }

    /**
     * @return the RecurrenceLast30m
     */
    public int getRecurrenceLast30m() {
        return RecurrenceLast30m;
    }

    /**
     * @param RecurrenceLast30m the RecurrenceLast30m to set
     */
    public void setRecurrenceLast30m(int RecurrenceLast30m) {
        this.RecurrenceLast30m = RecurrenceLast30m;
    }

    /**
     * @return the RecurrenceTmp
     */
    public int getRecurrenceTmp() {
        return RecurrenceTmp;
    }

    /**
     * @param RecurrenceTmp the RecurrenceTmp to set
     */
    public void setRecurrenceTmp(int RecurrenceTmp) {
        this.RecurrenceTmp = RecurrenceTmp;
    }
    /**
     * @return the persent_predict
     */
    public Double getPersent_predict() {
        return persent_predict;
    }

    /**
     * @param persent_predict the persent_predict to set
     */
    public void setPersent_predict(Double persent_predict) {
        this.persent_predict = persent_predict;
    }
}
