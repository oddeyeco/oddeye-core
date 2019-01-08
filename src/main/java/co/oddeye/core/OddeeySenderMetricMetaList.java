/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.gson.JsonArray;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author vahan
 */
public class OddeeySenderMetricMetaList extends OddeeyMetricMetaList implements Cloneable {

    private static final long serialVersionUID = 465895478L;
    private String targetType;
    private String targetValue;
    private final Map<String, Integer> LastSendList = new HashMap<>();
    private ArrayList<String> targetOption;

    @Deprecated
    public OddeeySenderMetricMetaList() {
    }

    public OddeeySenderMetricMetaList(String tT, String tV,ArrayList<String> options) {
        targetType = tT;
        targetValue = tV;
        targetOption = options;
    }

    @Override
    public OddeeySenderMetricMetaList clone() throws CloneNotSupportedException {
        try {
            return (OddeeySenderMetricMetaList) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new InternalError();
        }
    }

    /**
     * @return the targetType
     */
    public String getTargetType() {
        return targetType;
    }

    /**
     * @param targetType the targetType to set
     */
    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    /**
     * @return the targetValue
     */
    public String getTargetValue() {
        return targetValue;
    }

    /**
     * @param targetValue
     */
    public void setTargetValue(String targetValue) {
        this.targetValue = targetValue;
    }

    /**
     * @return the LastSendList
     */
    public Map<String, Integer> getLastSendList() {
        return LastSendList;
    }

    /**
     * @return the targetOption
     */
    public ArrayList<String> getTargetOption() {
        return targetOption;
    }

    /**
     * @param targetOption the targetOption to set
     */
    public void setTargetOption(ArrayList<String> targetOption) {
        this.targetOption = targetOption;
    }

}
