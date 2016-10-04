/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import org.apache.commons.lang.ArrayUtils;

/**
 *
 * @author vahan
 */
public class OddeeyMetricMeta {

    private String name;
    private byte[] nameTSDBUID;
    private final Map<String, OddeyeTag> tags = new HashMap<>();
    private static final Gson gson = new Gson();
//    private Map<String, Object> Metricmap = new HashMap<>();

    public OddeeyMetricMeta(JsonElement json, TSDB tsdb) {
        Map<String, Object> map = gson.fromJson(json, tags.getClass());
        map.entrySet().stream().forEach((Map.Entry<String, Object> entry) -> {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.equals("tags")) {
                Map<String, Object> t_value = (Map) value;
                tags.clear();
                t_value.entrySet().stream().forEach((Map.Entry<String, Object> tag) -> {
                    if (!tag.getKey().toLowerCase().equals("alert_level")) {
                        tags.put(tag.getKey(), new OddeyeTag(tag, tsdb));
                    }
                });
            }
            if (key.equals("metric")) {
                name = String.valueOf(value);
                try {
                    nameTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.METRIC, name);
                } catch (NoSuchUniqueName e) {
                    nameTSDBUID = tsdb.assignUid(key, name);
                }

            }
        });
    }

    public OddeeyMetricMeta(byte[] key, TSDB tsdb) throws Exception {
       nameTSDBUID = Arrays.copyOfRange(key, 0, 3);
       int i = 3;
        while (i < key.length) {
           byte[] tgkey = Arrays.copyOfRange(key, i, i+3);     
           byte[] tgval = Arrays.copyOfRange(key, i+3, i+6);     
           OddeyeTag tag = new OddeyeTag(tgkey, tgval, tsdb);
            tags.put(tag.getKey(), tag);
            i=i+6;
        }
        name = tsdb.getUidName(UniqueId.UniqueIdType.METRIC, nameTSDBUID).join();
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
     * @return the key
     */
    public byte[] getKey() {
        byte[] key;
        key = nameTSDBUID;

        for (OddeyeTag tag : tags.values()) {
            key = ArrayUtils.addAll(key, tag.getKeyTSDBUID());
            key = ArrayUtils.addAll(key, tag.getValueTSDBUID());
        }
        return key;
    }

    /**
     * @return the tags
     */
    public Map<String, OddeyeTag> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof OddeeyMetricMeta) {
            final OddeeyMetricMeta o = (OddeeyMetricMeta) anObject;
            if (Arrays.equals(o.getNameTSDBUID(), nameTSDBUID) && (tags.equals(o.getTags()))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Arrays.hashCode(this.nameTSDBUID);
        hash = 53 * hash + Objects.hashCode(this.tags);
        return hash;
    }

}
