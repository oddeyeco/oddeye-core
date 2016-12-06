/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.io.Serializable;

/**
 *
 * @author vahan
 */
public class ErrorState implements Serializable{

    private int level = -1;
    private int state;
    private long time;
    private long timestart;
    private long timeend;

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

    /**
     * @param timeend the timeend to set
     */
    public void setTimeend(long timeend) {
        this.timeend = timeend;
    }

    @Override
    public String toString() {
        if (level > -1) {
            return AlertLevel.getName(level) + state;
        }
        return "NaN"+ state;
    }

    /**
     * @param level the level to set
     */
    public void setLevel(int level) {
        if (this.level == -1) {
            state = 0;//Start
        } else if (this.level == level) {
            state = 1;//Cont
        } else if (this.level > level) {
            state = 2;//Dowun
        } else if (this.level < level) {
            state = 3;//Up
        }
        if (level == -1)
        {
            state = -1;//End error
        }
        this.level = level;
    }

}
