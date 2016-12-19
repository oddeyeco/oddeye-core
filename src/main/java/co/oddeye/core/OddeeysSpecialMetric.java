/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.gson.JsonElement;

/**
 *
 * @author vahan
 */
public class OddeeysSpecialMetric extends OddeeyMetric{
    
    private final String message;    
    private final String type;
    
    public OddeeysSpecialMetric(JsonElement json) {
        super(json);
        message = json.getAsJsonObject().get("message").getAsString();
        type = json.getAsJsonObject().get("type").getAsString();
        
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return the st
     */
    public boolean isSpecialTag() {
        return true;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }
    
}
