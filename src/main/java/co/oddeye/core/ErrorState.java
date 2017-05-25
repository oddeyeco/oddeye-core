/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class ErrorState implements Serializable, Comparable {
    private int level = -255;
    private int prevlevel = -1;
    private int state;
    private long time;
    private long timestart;
    private long timeend;
    private String message;
    private final Integer reaction;
    private final Double startvalue;
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

    static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ErrorState.class);
    public static ErrorState createSerializedErrorState(byte[] Bytes) {
        ErrorState result = null;
        if (Bytes.length > 0) {
            ByteArrayInputStream bis = new ByteArrayInputStream(Bytes);
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                result = (ErrorState) in.readObject();
            } catch (IOException | ClassNotFoundException ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException ex) {
                    LOGGER.error(globalFunctions.stackTrace(ex));
                }
            }
        }
        return result;
    }          
    
    public byte[] getSerialized() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        byte[] Bytes;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
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
        
    
    public ErrorState() {
        super();
        starttimes = new HashMap<>();
        endtimes = new HashMap<>();
        reaction = null;
        startvalue  = null;
    }

    public ErrorState(JsonObject ErrorData) {
        super();
        level = ErrorData.get("level").getAsInt();
        state = ErrorData.get("action").getAsInt();
        time = ErrorData.get("time").getAsLong();
        
        reaction = ErrorData.get("reaction").getAsInt();
        startvalue  = ErrorData.get("startvalue").getAsDouble();
        java.lang.reflect.Type type = new TypeToken<HashMap<Integer, Long>>() {
        }.getType();
        starttimes = globalFunctions.getGson().fromJson(ErrorData.get("starttimes"), type);
        endtimes = globalFunctions.getGson().fromJson(ErrorData.get("endtimes"), type);
        if (ErrorData.get("message") != null) {
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
        return starttimes.get(level);
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
        this.prevlevel = this.level;
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

        if ((level == -1) && (this.level != -1)) {
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

    /**
     * @return the prevlevel
     */
    public int getPrevlevel() {
        return prevlevel;
    }

    /**
     * @return the reaction
     */
    public Integer getReaction() {
        return reaction;
    }

    /**
     * @return the startvalue
     */
    public Double getStartvalue() {
        return startvalue;
    }

    @Override
    public int compareTo(Object o) {
        final ErrorState os = (ErrorState) o;
        return getTimestart() > os.getTimestart() ? 1 : -1;
    }

}
