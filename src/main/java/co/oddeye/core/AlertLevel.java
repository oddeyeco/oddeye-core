/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author vahan
 */
public class AlertLevel extends HashMap<Integer, Map<Integer, Double>> {

    public final static Integer ALERT_END_ERROR = -1;
    public final static Integer ALERT_LEVEL_ALL = 0;
    public final static Integer ALERT_LEVEL_LOW = 1;
    public final static Integer ALERT_LEVEL_GUARDED = 2;
    public final static Integer ALERT_LEVEL_ELEVATED = 3;
    public final static Integer ALERT_LEVEL_HIGH = 4;
    public final static Integer ALERT_LEVEL_SEVERE = 5;

    public final static String ST_ALERT_LEVEL_ALL = "All";
    public final static String ST_ALERT_LEVEL_LOW = "Low";
    public final static String ST_ALERT_LEVEL_GUARDED = "Guarded";
    public final static String ST_ALERT_LEVEL_ELEVATED = "Elevated";
    public final static String ST_ALERT_LEVEL_HIGH = "High";
    public final static String ST_ALERT_LEVEL_SEVERE = "Severe";

    public final static Integer[] ALERT_LEVELS_INDEX = new Integer[]{ALERT_LEVEL_ALL, ALERT_LEVEL_LOW, ALERT_LEVEL_GUARDED, ALERT_LEVEL_ELEVATED, ALERT_LEVEL_HIGH, ALERT_LEVEL_SEVERE};

    public final static String[] ALERT_LEVELS = new String[]{ST_ALERT_LEVEL_ALL, ST_ALERT_LEVEL_LOW, ST_ALERT_LEVEL_GUARDED, ST_ALERT_LEVEL_ELEVATED, ST_ALERT_LEVEL_HIGH, ST_ALERT_LEVEL_SEVERE};

    public final static String ST_ALERT_PARAM_VALUE_NAME = "Min Value";
    public final static String ST_ALERT_PARAM_PECENT_NAME = "Min Percent";
    public final static String ST_ALERT_PARAM_WEIGTH_NAME = "Min Weight";
    public final static String ST_ALERT_PARAM_RECCOUNT_NAME = "Min Recurrence Count";
    public final static String ST_ALERT_PARAM_PREDICTPERCENT_NAME = "Min Predict Percent";

    public final static Integer ALERT_PARAM_VALUE = 0;
    public final static Integer ALERT_PARAM_PECENT = 1;
    public final static Integer ALERT_PARAM_WEIGTH = 2;
    public final static Integer ALERT_PARAM_RECCOUNT = 3;
    public final static Integer ALERT_PARAM_PREDICTPERSENT = 4;

    public final static String[] ALERT_LEVEL_PARAM = new String[]{ST_ALERT_PARAM_VALUE_NAME, ST_ALERT_PARAM_PECENT_NAME, ST_ALERT_PARAM_WEIGTH_NAME, ST_ALERT_PARAM_RECCOUNT_NAME, ST_ALERT_PARAM_PREDICTPERCENT_NAME};

    public AlertLevel() {
//        this(true);
    }

    public AlertLevel(boolean usedefault) {
        if (usedefault) {

//            new HashMap<String, String>() {
//                {
//                    put("a", "1");
//                    put("b", "2");
//                }
//            }
            this.put(ALERT_LEVEL_ALL, new HashMap<Integer, Double>() {
                {
                    put(ALERT_PARAM_VALUE, 0.2);
                    put(ALERT_PARAM_PECENT, 10.0);
                    put(ALERT_PARAM_WEIGTH, 2.0);
                    put(ALERT_PARAM_RECCOUNT, 4.0);
                    put(ALERT_PARAM_PREDICTPERSENT, 10.0);
                }
            });
            this.put(ALERT_LEVEL_LOW, new HashMap<Integer, Double>() {
                {
                    put(ALERT_PARAM_VALUE, 1.0);
                    put(ALERT_PARAM_PECENT, 20.0);
                    put(ALERT_PARAM_WEIGTH, 8.0);
                    put(ALERT_PARAM_RECCOUNT, 4.0);
                    put(ALERT_PARAM_PREDICTPERSENT, 20.0);
                }
            });

            this.put(ALERT_LEVEL_GUARDED, new HashMap<Integer, Double>() {
                {
                    put(ALERT_PARAM_VALUE, 1.0);
                    put(ALERT_PARAM_PECENT, 40.0);
                    put(ALERT_PARAM_WEIGTH, 10.0);
                    put(ALERT_PARAM_RECCOUNT, 4.0);
                    put(ALERT_PARAM_PREDICTPERSENT, 50.0);
                }
            });

            this.put(ALERT_LEVEL_ELEVATED, new HashMap<Integer, Double>() {
                {
                    put(ALERT_PARAM_VALUE, 1.0);
                    put(ALERT_PARAM_PECENT, 70.0);
                    put(ALERT_PARAM_WEIGTH, 14.0);
                    put(ALERT_PARAM_RECCOUNT, 5.0);
                    put(ALERT_PARAM_PREDICTPERSENT, 80.0);
                }
            });            
            this.put(ALERT_LEVEL_HIGH, new HashMap<Integer, Double>() {
                {
                    put(ALERT_PARAM_VALUE, 1.0);
                    put(ALERT_PARAM_PECENT, 90.0);
                    put(ALERT_PARAM_WEIGTH, 15.0);
                    put(ALERT_PARAM_RECCOUNT, 6.0);
                    put(ALERT_PARAM_PREDICTPERSENT, 100.0);
                }
            });                        
            this.put(ALERT_LEVEL_SEVERE, new HashMap<Integer, Double>() {
                {
                    put(ALERT_PARAM_VALUE, 1.0);
                    put(ALERT_PARAM_PECENT, 120.0);
                    put(ALERT_PARAM_WEIGTH, 16.0);
                    put(ALERT_PARAM_RECCOUNT, 8.0);
                    put(ALERT_PARAM_PREDICTPERSENT, 150.0);
                }
            }); 
//            this.put(ALERT_LEVEL_SEVERE, ImmutableMap.<Integer, Double>builder()
//                    .put(ALERT_PARAM_VALUE, 1.0)
//                    .put(ALERT_PARAM_PECENT, 80.0)
//                    .put(ALERT_PARAM_WEIGTH, 16.0)
//                    .put(ALERT_PARAM_RECCOUNT, 4.0)
//                    .put(ALERT_PARAM_PREDICTPERSENT, 80.0)
//                    .build());
//          this.get(this).get(this)
        }
    }

    public Integer getErrorLevel(MetricErrorMeta e) {
        Integer[] Levels = ALERT_LEVELS_INDEX;
        Arrays.sort(Levels, Collections.reverseOrder());

        for (Integer level : Levels) {
            if ((Math.abs(e.getValue()) >= this.get(level).get(ALERT_PARAM_VALUE))
                    && (Math.abs(e.getPersent_weight()) >= this.get(level).get(ALERT_PARAM_PECENT))
                    && (Math.abs(e.getWeight()) >= this.get(level).get(ALERT_PARAM_WEIGTH))
                    //                &&(Math.abs(e.getRecurrenceTmp())>= this.get(level).get(ALERT_PARAM_RECCOUNT))
                    && (Math.abs(e.getPersent_predict()) >= this.get(level).get(ALERT_PARAM_PREDICTPERSENT))) {
                return level;
            }
        }
        return ALERT_LEVEL_ALL;
    }

    public static String getName(Integer idx) {

        boolean contains = Arrays.asList(ALERT_LEVELS_INDEX).contains(idx);
        if (contains) {
            return ALERT_LEVELS[idx];
        }
        return "NaN "+idx;
    }

    public static Integer getPyName(String Name) {

        if (Name.equals("OK")) {
            return -1;
        }
        if (Name.equals("ELEVATED")) {
            return ALERT_LEVEL_ELEVATED;
        }        
        if (Name.equals("WARNING")) {
            return ALERT_LEVEL_HIGH;
        }
        if (Name.equals("ERROR")) {
            return ALERT_LEVEL_SEVERE;
        }
        return -1;
    }

    public Integer getErrorLevel(int weight, double weight_per, Double value, double predict_value_per) {
        Integer[] Levels = ALERT_LEVELS_INDEX;
        Arrays.sort(Levels, Collections.reverseOrder());

        for (Integer level : Levels) {
            if ((Math.abs(value) >= this.get(level).get(ALERT_PARAM_VALUE))
                    && (Math.abs(weight_per) >= this.get(level).get(ALERT_PARAM_PECENT))
                    && (Math.abs(weight) >= this.get(level).get(ALERT_PARAM_WEIGTH))
                    //                &&(Math.abs(e.getRecurrenceTmp())>= this.get(level).get(ALERT_PARAM_RECCOUNT))
                    && (Math.abs(predict_value_per) >= this.get(level).get(ALERT_PARAM_PREDICTPERSENT))) {
                return level;
            }
        }
        return ALERT_END_ERROR;
    }

}
