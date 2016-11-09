/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class OddeeyMetric implements Serializable, Comparable<OddeeyMetric> {

    static final Logger LOGGER = LoggerFactory.getLogger(OddeeyMetric.class);
    private String name;
    private final Map<String, String> tags = new TreeMap<>();
    private static final Gson GSON = new Gson();
    private Double value;
    private Long timestamp;

    public OddeeyMetric(JsonElement json) {
        Map<String, Object> map = GSON.fromJson(json, tags.getClass());
        map.entrySet().stream().forEach((Map.Entry<String, Object> entry) -> {
            String key = entry.getKey();
            Object ObValue = entry.getValue();
            if (key.equals("tags")) {
                Map<String, Object> t_value = (Map) ObValue;
                tags.clear();
                t_value.entrySet().stream().forEach((Map.Entry<String, Object> tag) -> {
                    tags.put(tag.getKey(), String.valueOf(tag.getValue()));
                });
            }
            if (key.equals("metric")) {
                name = String.valueOf(ObValue);
                if (name == null) {
                    throw new NullPointerException("Has not metriq name:" + json.toString());
                }
            }
            
            if (key.equals("value")) {
                value = Double.valueOf(String.valueOf(ObValue));
                if (value == null) {
                    throw new NullPointerException("Has not metriq value:" + json.toString());
                }                
            }            
            if (key.equals("timestamp")) {
                timestamp =(long) (Double.valueOf(String.valueOf(ObValue)) * 1000);
//                Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000
                if (timestamp == null) {
                    throw new NullPointerException("Has not metriq value:" + json.toString());
                }
                
            }            
        });
    }

    @Override
    public int compareTo(OddeeyMetric o) {
        int result = getName().compareTo(o.getName());
        return result;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the tags
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * @return the value
     */
    public Double getValue() {
        return value;
    }

    /**
     * @return the timestamp
     */
    public Long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getTSDBTags() {
        final Map<String, String> litetags = new HashMap<>(tags) ;
        litetags.remove("alert_level");
        return litetags;
    }


}
