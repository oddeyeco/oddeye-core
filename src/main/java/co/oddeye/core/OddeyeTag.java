/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;

/**
 *
 * @author vahan
 */
public class OddeyeTag implements Serializable{

    private String key;
    private String value;
    private byte[] keyTSDBUID;
    private byte[] valueTSDBUID;

    public OddeyeTag(Map.Entry<String, Object> entry, TSDB tsdb) {
        key = entry.getKey();
        value = String.valueOf(entry.getValue());

        if (key.toLowerCase().equals("alert_level")) {
            value = Integer.toString(Math.round(Float.valueOf(value)));
        }
        try {
            keyTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGK, key);
        } catch (NoSuchUniqueName e) {
            keyTSDBUID = tsdb.assignUid("tagk", key);
        }

        try {
            valueTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGV, value);
        } catch (NoSuchUniqueName e) {
            valueTSDBUID = tsdb.assignUid("tagv", value);
        }

    }

    public OddeyeTag(String p_key, String p_value, TSDB tsdb) throws Exception {
        key = p_key;
        value = p_value;
        
        
        if (key == null) {
            throw new Exception("Key is Null");
        }
        if (value == null) {
            throw new Exception("value is Null");
        }

        if (key.toLowerCase().equals("alert_level")) {
            value = Integer.toString(Math.round(Float.valueOf(value)));
        }
        try {
            keyTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGK, key);
        } catch (NoSuchUniqueName e) {
            keyTSDBUID = tsdb.assignUid("tagk", key);
        }

        try {
            valueTSDBUID = tsdb.getUID(UniqueId.UniqueIdType.TAGV, value);
        } catch (NoSuchUniqueName e) {
            valueTSDBUID = tsdb.assignUid("tagv", value);
        }

    }

    OddeyeTag(byte[] tgkey, byte[] tgval, TSDB tsdb) throws Exception {
        keyTSDBUID = tgkey;
        valueTSDBUID = tgval;        
        key = tsdb.getUidName(UniqueId.UniqueIdType.TAGK, keyTSDBUID).joinUninterruptibly();
        value = tsdb.getUidName(UniqueId.UniqueIdType.TAGV, valueTSDBUID).joinUninterruptibly();
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

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof OddeyeTag) {
            final OddeyeTag o = (OddeyeTag) anObject;
            if (Arrays.equals(o.getKeyTSDBUID(), keyTSDBUID) && (Arrays.equals(o.getValueTSDBUID(), valueTSDBUID))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Arrays.hashCode(this.keyTSDBUID);
        hash = 53 * hash + Arrays.hashCode(this.valueTSDBUID);
        return hash;
    }
    
    @Override
    public String toString() {
        return value;
    }    
}
