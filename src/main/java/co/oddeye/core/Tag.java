/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

/**
 *
 * @author vahan
 */
public class Tag {
    private String key;
    private String value;
    private byte[] keyTSDBUID;
    private byte[] valueTSDBUID;    

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
