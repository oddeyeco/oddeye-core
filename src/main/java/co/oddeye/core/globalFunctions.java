/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Calendar;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class globalFunctions {

    static final Logger LOGGER = LoggerFactory.getLogger(globalFunctions.class);
    private static HBaseClient client = null;
    private static TSDB tsdb = null;

    private static HBaseClient secindaryclient = null;
    private static TSDB secindarytsdb = null; 
    private static final Gson gson = new Gson();
    private static final JsonParser PARSER = new JsonParser();
    private static final Calendar calendar = Calendar.getInstance();
    
    static public String stackTrace(Exception cause) {
        if (cause == null) {
            return "-/-";
        }
        StringWriter sw = new StringWriter(1024);
        final PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    static public TSDB getTSDB(Config TSDBconfig, org.hbase.async.Config ClientConfig) {

        while (tsdb == null) {
            try {
                client = getClient(ClientConfig);
                tsdb = new TSDB(
                        client,
                        TSDBconfig);
            } catch (Exception e) {
                LOGGER.warn("OpenTSDB Connection fail in prepare");
                LOGGER.error("Exception: " + stackTrace(e));
            }

        }
        return tsdb;
    }

    static public HBaseClient getClient(org.hbase.async.Config ClientConfig) {
        while (client == null) {
            try {
                client = new org.hbase.async.HBaseClient(ClientConfig);
            } catch (Exception e) {
                LOGGER.warn("HBaseClient Connection fail in prepare");
                LOGGER.error("Exception: " + stackTrace(e));
            }

        }
        return client;
    }

    /**
     * @param ClientConfig
     * @return the secindaryclient
     */
    public static HBaseClient getSecindaryclient(org.hbase.async.Config ClientConfig) {
        while (secindaryclient == null) {
            try {
                secindaryclient = new org.hbase.async.HBaseClient(ClientConfig);
            } catch (Exception e) {
                LOGGER.warn("HBaseClient Connection fail in prepare");
                LOGGER.error("Exception: " + stackTrace(e));
            }

        }        
        return secindaryclient;
    }

    /**
     * @param TSDBconfig
     * @param ClientConfig
     * @return the secindarytsdb
     */
    public static TSDB getSecindarytsdb(Config TSDBconfig, org.hbase.async.Config ClientConfig) {
        while (secindarytsdb == null) {
            try {
                secindaryclient = getSecindaryclient(ClientConfig);
                secindarytsdb = new TSDB(
                        secindaryclient,
                        TSDBconfig);
            } catch (Exception e) {
                LOGGER.warn("OpenTSDB Connection fail in prepare");
                LOGGER.error("Exception: " + stackTrace(e));
            }

        }               
        return secindarytsdb;
    }

    /**
     * @return the gson
     */
    public static Gson getGson() {
        return gson;
    }

    public static int getDayStamp(Long timestamp) {
        calendar.setTimeInMillis(timestamp);        
        return calendar.get(Calendar.DATE);
    }    
    public static int getNoDayStamp(Long timestamp) {
        return (int)(timestamp - getDayStamp(timestamp));
    }        
    
    public static byte[] getDayKey(Long timestamp) {        
        calendar.setTimeInMillis(timestamp);
        return ByteBuffer.allocate(4).putShort((short)calendar.get(Calendar.YEAR)).putShort((short)calendar.get(Calendar.DAY_OF_YEAR)).array();
    }    
    public static byte[] getNoDayKey(Long timestamp) {
        calendar.setTimeInMillis(timestamp);
        return ByteBuffer.allocate(3).put((byte)calendar.get(Calendar.HOUR_OF_DAY)).put((byte)calendar.get(Calendar.MINUTE)).put((byte)calendar.get(Calendar.SECOND)).array();
    }    

    /**
     * @return the PARSER
     */
    public static JsonParser getJsonParser() {
        return PARSER;
    }
}
