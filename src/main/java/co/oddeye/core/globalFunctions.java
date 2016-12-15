/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.io.PrintWriter;
import java.io.StringWriter;
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

}
