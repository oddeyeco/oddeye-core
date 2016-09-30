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
public class Metric {
    private String name;
    private byte[] nameTSDBUID;    
    private final Map<String,Tag> tags = new HashMap<>();

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the nameTSDBUID
     */
    public byte[] getNameTSDBUID() {
        return nameTSDBUID;
    }

    /**
     * @return the tags
     */
    public Map<String,Tag> getTags() {
        return tags;
    }
    
}
