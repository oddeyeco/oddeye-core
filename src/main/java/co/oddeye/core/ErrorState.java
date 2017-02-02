/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

/**
 *
 * @author vahan
 */
public class ErrorState implements Serializable {

    private int level = -255;
    private int state;
    private long time;
    private long timestart;
    private long timeend;
    private String message;
    private final Map<Integer, Long> starttimes;
    private final Map<Integer, Long> endtimes;
    
    public final static String ST_ALERT_STATE_START = "Start";
    public final static String ST_ALERT_STATE_CONT = "Continue"; 
    public final static String ST_ALERT_STATE_DW = "Down";
    public final static String ST_ALERT_STATE_UP = "Up";    
    public final static String ST_ALERT_STATE_END = "End";    
    
    public final static Integer ALERT_STATE_START = 0;
    public final static Integer ALERT_STATE_CONT = 1;
    public final static Integer ALERT_STATE_DW = 2;
    public final static Integer ALERT_STATE_UP = 3;
    public final static Integer ALERT_STATE_END = -1;
    
    public final static Integer[] ALERT_STATE_INDEX = new Integer[]{ALERT_STATE_START, ALERT_STATE_CONT, ALERT_STATE_DW, ALERT_STATE_UP};
    public final static String[] ALERT_STATE = new String[]{ST_ALERT_STATE_START, ST_ALERT_STATE_CONT, ST_ALERT_STATE_DW, ST_ALERT_STATE_UP};


    public ErrorState() {
        super();
        starttimes = new HashMap<>();
        endtimes = new HashMap<>();
    }

    public ErrorState(JsonObject ErrorData) {
        super();
        level = ErrorData.get("level").getAsInt();
        state = ErrorData.get("action").getAsInt();
        time = ErrorData.get("time").getAsLong();
        java.lang.reflect.Type type = new TypeToken<HashMap<Integer, Long>>() {
        }.getType();
        starttimes = globalFunctions.getGson().fromJson(ErrorData.get("starttimes"), type);
        endtimes = globalFunctions.getGson().fromJson(ErrorData.get("endtimes"), type);
        if (ErrorData.get("message")!=null) {
            message = ErrorData.get("message").getAsString();
        }
    }

    /**
     * @return the level
     */
    public int getLevel() {
        return level;
    }
    
    public String getLevelName() {
        return AlertLevel.getName(level);
    }    

    /**
     * @return the state
     */
    public int getState() {
        return state;
    }
    public String getStateName() {
        boolean contains = Arrays.asList(ALERT_STATE_INDEX).contains(state);
        if (contains) {
            return ALERT_STATE[state];
        }
        return "NaN";                
    }    

    /**
     * @return the time
     */
    public long getTime() {
        return time;
    }

    /**
     * @return the timestart
     */
    public long getTimestart() {
        return timestart;
    }

    /**
     * @return the timeend
     */
    public long getTimeend() {
        return timeend;
    }

    @Override
    public String toString() {
        if (level > -1) {
            return AlertLevel.getName(level) + state;
        }
        return "NaN" + state;
    }

    /**
     * @param level the level to set
     * @param timestamp
     */
    public void setLevel(int level, long timestamp) {
        if (this.level == level) {
            state = ALERT_STATE_CONT;
        } else if (this.level == -1) {
            state = ALERT_STATE_START;
            starttimes.put(level, timestamp);
        } else if (this.level > level) {
            state = ALERT_STATE_DW;//Dowun
            endtimes.put(this.level, timestamp);
        } else if (this.level < level) {
            state = ALERT_STATE_UP;//Up
            starttimes.put(level, timestamp);
        }  
        
        if ((level == -1)&&(this.level!=-1)) {
            state = ALERT_STATE_END;//End error
            endtimes.put(this.level, timestamp);
        }
        this.level = level;
        time = timestamp;
    }

    /**
     * @return the starttimes
     */
    public Map<Integer, Long> getStarttimes() {
        return starttimes;
    }

    /**
     * @return the endtimes
     */
    public Map<Integer, Long> getEndtimes() {
        return endtimes;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

}
