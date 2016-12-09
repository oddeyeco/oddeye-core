/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author vahan
 */
public class ErrorState implements Serializable {

    private int level = -1;
    private int state;
    private long time;
    private long timestart;
    private long timeend;
    private final Map<Integer, Long> starttimes = new HashMap<>();
    private final Map<Integer, Long> endtimes = new HashMap<>();

    /**
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * @return the state
     */
    public int getState() {
        return state;
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
            state = 1;//Cont
        } else if (this.level == -1) {
            state = 0;//Start
            starttimes.put(level, timestamp);
        } else if (this.level > level) {
            state = 2;//Dowun
            endtimes.put(this.level, timestamp);
        } else if (this.level < level) {
            state = 3;//Up
            starttimes.put(level, timestamp);
        } else if (level == -1) {
            state = -1;//End error
            endtimes.put(this.level, timestamp);
        }
        this.level = level;
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

}
