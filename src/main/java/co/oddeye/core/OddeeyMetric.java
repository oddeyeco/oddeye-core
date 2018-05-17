/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonElement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import net.opentsdb.core.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class OddeeyMetric implements Serializable, Comparable<OddeeyMetric>, Cloneable {

    private static final long serialVersionUID = 465895478L;

    static final Logger LOGGER = LoggerFactory.getLogger(OddeeyMetric.class);
    private String name;
    private final Map<String, String> tags = new TreeMap<>();
    private Double value;
    private Long timestamp;
    protected OddeeyMetricTypesEnum metricType;
    private int reaction;

    @Override
    public OddeeyMetric clone() throws CloneNotSupportedException {
        try {
            return (OddeeyMetric) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new InternalError();
        }
    }

    public OddeeyMetric(JsonElement json) {
        metricType = OddeeyMetricTypesEnum.NONE;
        reaction = 0;
        Type listType = new TypeToken<Map<String, Object>>() {
            private static final long serialVersionUID = 123854678L;
        }.getType();
        Map<String, Object> map = globalFunctions.getGson().fromJson(json, listType);
        map.entrySet().stream().forEach(new Consumer<Map.Entry<String, Object>>() {
            @Override
            public void accept(Map.Entry<String, Object> entry) {
                String key = entry.getKey();
                Object ObValue = entry.getValue();
                if (key.equals("tags")) {
                    if (ObValue instanceof Map) {
                        Map<?, ?> t_value = (Map<?, ?>) ObValue;
                        tags.clear();
                        for (Map.Entry<?, ?> tag : t_value.entrySet()) {
                            Tags.validateString("tagk", (String) tag.getKey());
                            Tags.validateString("tagv", String.valueOf(tag.getValue()));
                            tags.put((String) tag.getKey(), String.valueOf(tag.getValue()));
                        }
//                        t_value.entrySet().stream().forEach((Map.Entry<String, Object> tag) -> {
//                            tags.put(tag.getKey(), String.valueOf(tag.getValue()));
//                        });
                    }
                }
                if (key.equals("metric")) {
                    name = String.valueOf(ObValue);
                    Tags.validateString("metric", name);
                    if (name == null) {
                        throw new NullPointerException("Has not metriq name:" + json.toString());
                    }
                }
                if (key.equals("type")) {
//                    type = OddeeyMetricTypes.getIndexByName(String.valueOf(ObValue));
                    try {
                        metricType = OddeeyMetricTypesEnum.valueOf(String.valueOf(ObValue).toUpperCase());
                    } catch (Exception e) {
                        LOGGER.error(globalFunctions.stackTrace(e));
                        metricType = OddeeyMetricTypesEnum.NONE;
                    }
                    
                }
                if (key.equals("value")) {
                    value = Double.valueOf(String.valueOf(ObValue));
                    if (value == null) {
                        throw new NullPointerException("Has not metriq value:" + json.toString());
                    }
                }
                if (key.equals("timestamp")) {
                    timestamp = (long) (Double.valueOf(String.valueOf(ObValue)) * 1000);
//                Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000
                    if (timestamp == null) {
                        throw new NullPointerException("Has not metriq value:" + json.toString());
                    }
                }
                if (key.equals("reaction")) {
                    reaction = Double.valueOf(String.valueOf(ObValue)).intValue();
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
        final Map<String, String> litetags = new HashMap<>(tags);
        litetags.remove("alert_level");
        return litetags;
    }

    /**
     * @return the type
     */
    public OddeeyMetricTypesEnum getType() {
        return metricType;
    }    
    
    public String getTypeName() {
        return metricType.toString();
    }

    public boolean isSpecial() {
        return metricType == OddeeyMetricTypesEnum.SPECIAL;
    }

    /**
     * @return the reaction
     */
    public int getReaction() {
        return reaction;
    }

    public OddeeyMetric dublicate() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream ous = new ObjectOutputStream(baos)) {
            ous.writeObject(this);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);

        return (OddeeyMetric) ois.readObject();
    }

    @Override
    public int hashCode() {
        int hash = Objects.hashCode(getName() + getTags().toString());
        return hash;
    }
}
