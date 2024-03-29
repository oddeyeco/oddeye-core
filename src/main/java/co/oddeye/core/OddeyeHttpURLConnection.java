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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.HttpsURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OddeyeHttpURLConnection {

    static final Logger LOGGER = LoggerFactory.getLogger(OddeyeHttpURLConnection.class);
    private static final String USER_AGENT = "Mozilla/5.0";
//    private static final int taskcount = 10;
    private static final ExecutorService executor = Executors.newFixedThreadPool(10);

    public static class GetRequest implements Callable<String> {

        private final URL url;

        public GetRequest(URL url) {
            this.url = url;
        }

        @Override
        public String call() throws Exception {
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);
            try {
                int responseCode = con.getResponseCode();
//                System.out.println("Response Code : " + responseCode);
                StringBuffer response;
//                System.out.println("Sending 'GET' request to URL : " + url);
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()))) {
                    String inputLine;
                    response = new StringBuffer();
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }
//                System.out.println("Response : " + response);
                return response.toString();
            } catch (IOException e) {
                LOGGER.error(globalFunctions.stackTrace(e));
            }
            return "";
        }

    }

    // HTTP GET request
    public static void sendGet(String url) throws Exception {
        executor.submit(new GetRequest(new URL(url)));
    }

    public static String getGet(String url) throws Exception {

//        String url = "https://selfsolve.apple.com/wcResults.do";
        URL obj = new URL(url);
        
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        //add reuqest header
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", USER_AGENT);
        con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

        con.setDoOutput(true);
        int responseCode = con.getResponseCode();
//        System.out.println("\nSending 'POST' request to URL : " + url);
//        System.out.println("Post parameters : " + urlParameters);
//        System.out.println("Response Code : " + responseCode);

        StringBuffer response;
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()))) {
            String inputLine;
            response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }

        //print result
        return response.toString();
    }

    public static JsonElement getGetJSON(String url) throws Exception {
        String result = getGet(url);
        return globalFunctions.getJsonParser().parse(result);
    }

    // HTTP POST request
    public static String getPost(String url, String urlParameters) throws Exception {

//        String url = "https://selfsolve.apple.com/wcResults.do";
        URL obj = new URL(url);
        HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

        //add reuqest header
        con.setRequestMethod("POST");
        con.setRequestProperty("User-Agent", USER_AGENT);
        con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

//        String urlParameters = "sn=C02G8416DRJM&cn=&locale=&caller=&num=12345";
        // Send post request
        con.setDoOutput(true);
        try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
            wr.writeBytes(urlParameters);
            wr.flush();
        }

        int responseCode = con.getResponseCode();
//        System.out.println("\nSending 'POST' request to URL : " + url);
//        System.out.println("Post parameters : " + urlParameters);
//        System.out.println("Response Code : " + responseCode);

        StringBuffer response;
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()))) {
            String inputLine;
            response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }

        //print result
        return response.toString();
    }

    public static JsonElement getPostJSON(String url, String urlParameters) throws Exception {
        String result = getPost(url, urlParameters);
        return globalFunctions.getJsonParser().parse(result);
    }
}
