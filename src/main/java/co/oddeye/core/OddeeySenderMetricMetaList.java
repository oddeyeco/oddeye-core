/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

/**
 *
 * @author vahan
 */
public class OddeeySenderMetricMetaList extends OddeeyMetricMetaList {
    
    private String targetType;
    private String targetValue;

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
     * @param targetValuew the targetValue to set
     */
    public void setTargetValue(String targetValue) {
        this.targetValue = targetValue;
    }
    
}
