/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.HashMap;
import java.util.Map;

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
    TIME,
    TEMPERATURE;

    private String value;
    
    public String getValue() {
        return value;        
    }        
    
    public short getShort() {
        return (short) this.ordinal();
    }            
}


