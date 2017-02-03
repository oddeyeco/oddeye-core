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
    private final String status;
    
    public OddeeysSpecialMetric(JsonElement json) {
        super(json);
        message = json.getAsJsonObject().get("message").getAsString();
        status  = json.getAsJsonObject().get("status").getAsString();
        
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return the state
     */
    public String getStatus() {
        return status;
    }
    
}
