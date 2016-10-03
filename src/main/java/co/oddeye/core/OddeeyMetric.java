/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;


import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

/**
 *
 * @author vahan
 */
public class OddeeyMetric {
    private String name;
    private byte[] nameTSDBUID;    
    private final Map<String,OddeyeTag> tags = new HashMap<>();
    private static final Gson gson = new Gson();
    private Map<String, Object> Metricmap = new HashMap<>();
    
    
    public OddeeyMetric(JsonElement json,TSDB tsdb)
    {
        Map<String, Object> map  = gson.fromJson(json, tags.getClass());   
        map.entrySet().stream().forEach((Map.Entry<String, Object> entry) -> {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.equals("tags"))
            {
                Map<String, Object> t_value = (Map) value;
                tags.clear();
                t_value.entrySet().stream().forEach((tag) -> {
                    tags.put(tag.getKey(), new OddeyeTag(tag, tsdb));
                });
                Metricmap.put(key, tags);
            }
            if (key.equals("metric")) {
                name = String.valueOf(value);
                nameTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, name);
            }
        });
        
    }
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
    public Map<String,OddeyeTag> getTags() {
        return tags;
    }
    
}
