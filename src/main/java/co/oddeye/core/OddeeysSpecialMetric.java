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
public class OddeeysSpecialMetric extends OddeeyMetric {
    private static final long serialVersionUID = 465895478L;
    private final String message;
    private final String status;

    public OddeeysSpecialMetric(JsonElement json) {
        super(json);
        if (json.getAsJsonObject().has("message")) {
            message = json.getAsJsonObject().get("message").getAsString();
        } else {
            message = "";
        }
        if (json.getAsJsonObject().has("status")) {
            status = json.getAsJsonObject().get("status").getAsString();
        }
        else
        {
            status ="";
        }

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
