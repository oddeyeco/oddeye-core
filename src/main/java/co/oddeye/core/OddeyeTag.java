/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.Map;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;

/**
 *
 * @author vahan
 */
public class OddeyeTag {

    private final String key;
    private final String value;
    private byte[] keyTSDBUID;
    private final byte[] valueTSDBUID;

    public OddeyeTag(Map.Entry<String, Object> entry, TSDB tsdb) {
        key = entry.getKey();
        value = String.valueOf(entry.getValue());

        try {
            keyTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGK, key);
        } catch (NoSuchUniqueName e) {
            keyTSDBUID = tsdb.assignUid(key, value);
//            keyTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGK, key);
        }

        valueTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGV, value);
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @return the keyTSDBUID
     */
    public byte[] getKeyTSDBUID() {
        return keyTSDBUID;
    }

    /**
     * @return the valueTSDBUID
     */
    public byte[] getValueTSDBUID() {
        return valueTSDBUID;
    }
}
