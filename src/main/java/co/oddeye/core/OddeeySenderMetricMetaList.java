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
public class OddeeySenderMetricMetaList extends OddeeyMetricMetaList implements Cloneable {

    private static final long serialVersionUID = 465895478L;
    private String targetType;
    private String targetValue;

    @Deprecated
    public OddeeySenderMetricMetaList() {
    }

    public OddeeySenderMetricMetaList(String tT, String tV) {
        targetType = tT;
        targetValue = tV;
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

}
