/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.Arrays;
/**
 *
 * @author vahan
 */
public final class OddeeyMetricTypes {
    public static final short MERIC_TYPE_SPECIAL = 0;
    public static final short MERIC_TYPE_NONE    = 1;
    public static final short MERIC_TYPE_COUNTER = 2;
    public static final short MERIC_TYPE_RATE    = 3;
    public static final short MERIC_TYPE_PERSENT = 4;
    
    public static final String ST_MERIC_TYPE_SPECIAL = "Special";
    public static final String ST_MERIC_TYPE_NONE    = "None";
    public static final String ST_MERIC_TYPE_COUNTER = "Counter";
    public static final String ST_MERIC_TYPE_RATE    = "Rate";
    public static final String ST_MERIC_TYPE_PERSENT = "Persent";    
    
    public final static short[] MERIC_TYPES = new short[]{MERIC_TYPE_SPECIAL, MERIC_TYPE_NONE, MERIC_TYPE_COUNTER, MERIC_TYPE_RATE, MERIC_TYPE_PERSENT};

    public final static String[] ST_MERIC_TYPES = new String[]{ST_MERIC_TYPE_SPECIAL, ST_MERIC_TYPE_NONE, ST_MERIC_TYPE_COUNTER, ST_MERIC_TYPE_RATE, ST_MERIC_TYPE_PERSENT};       
    
    public static short getIndexByName (String name)
    {
        boolean contains = Arrays.asList(ST_MERIC_TYPES).contains(name);
        if (contains) {
            return (short) Arrays.asList(ST_MERIC_TYPES).indexOf(name);
        }
        return MERIC_TYPE_NONE;        
    }
    
    public static String getName(short idx) {
        boolean contains = Arrays.asList(MERIC_TYPES).contains(idx);
        if (contains) {
            return ST_MERIC_TYPES[idx];
        }
        return ST_MERIC_TYPE_NONE;
    }    
}