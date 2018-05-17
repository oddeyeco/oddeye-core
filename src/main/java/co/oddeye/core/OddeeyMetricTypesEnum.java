/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.WordUtils;

/**
 *
 * @author vahan
 */
public enum OddeeyMetricTypesEnum {
    SPECIAL,
    NONE,
    COUNTER,
    @Deprecated
    RATE,
    PERCENT,
    OPS,
    BPS,
    IEC,
    METRIC,
    TIME,
    TEMPERATURE;

    private String value;

    public String getValue() {
        return value;
    }

    public short getShort() {
        return (short) this.ordinal();
    }

    @Override
    public String toString() {
        switch (this) {
            case OPS: {
                return "Rate Operations";
            }
            case BPS: {
                return "Rate byte data";
            }
            case METRIC: {
                return "Data Metric";
            }
            case IEC: {
                return "Data IEC";
            }
            default: {
                return WordUtils.capitalize(super.toString().toLowerCase());
            }
        }

    }
}
