/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package com.yahoo.ycsb.measurements;

import br.ufc.lsbd.benchxtend.configuration.Distribution;
import br.ufc.lsbd.benchxtend.configuration.Sla;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * Collects latency measurements, and reports them when requested.
 *
 * @author cooperb
 *
 */
public class Measurements {

    private static final String MEASUREMENT_TYPE = "measurementtype";
    private static final String MEASUREMENT_TYPE_DEFAULT = "histogram";
    static Measurements singleton = null;
    static Properties measurementproperties = null;
    static long workloadStartTime = -1;
    static Sla sla;
    static Distribution distribution;
    HashMap<String, OneMeasurement> data;

    public static void setProperties(Properties props) {
        measurementproperties = props;
    }
    
    public static void setSla(Sla _sla){
        sla = _sla;
    }
    
    public static void setDistribution(Distribution _distribution){
        distribution = _distribution;
    }
    
    public static void setWorkloadStartTime(long _workloadStartTime){
        workloadStartTime = _workloadStartTime;
    }

    /**
     * Return the singleton Measurements object.
     */
    public synchronized static Measurements getMeasurements() {
        if (singleton == null) {
            singleton = new Measurements(measurementproperties);
        }
        return singleton;
    }
    
    boolean histogram = true;
    boolean individual = false;
    private Properties _props;
    
    /**
     * Create a new object with the specified properties.
     */
    private Measurements(Properties props) {
        data = new HashMap<String, OneMeasurement>();

        _props = props;

        if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT).compareTo("individual") == 0) {
            individual = true;
        } else {
            if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT).compareTo("histogram") == 0) {
                histogram = true;
            } else {
                histogram = false;
            }
        }
    }
    
    private Measurements(Properties props, Sla _sla, Distribution _distribution, long _workloadStartTime){
        this(props);
        workloadStartTime = _workloadStartTime;
        distribution = _distribution;
        sla = _sla;
    }

    OneMeasurement constructOneMeasurement(String name) {
        if (individual) {
            if(sla == null || distribution == null || workloadStartTime == -1){
                System.out.println("ERROR: SLA, Distribution or StartTime was not set to use Individual "
                        + "measurement. Histogram measurement will be used instead.");
                return new OneMeasurementHistogram(name, _props);
            }else{
                return new OneMeasurementIndividual(name, _props, sla, distribution, workloadStartTime);
            }
        } else {
            if (histogram) {
                return new OneMeasurementHistogram(name, _props);
            } else {
                return new OneMeasurementTimeSeries(name, _props);
            }
        }
    }

    /**
     * Report a single value of a single metric. E.g. for read latency,
     * operation="READ" and latency is the measured value.
     */
    public synchronized void measure(String operation, int latency) {
        if (!data.containsKey(operation)) {
            synchronized (this) {
                if (!data.containsKey(operation)) {
                    data.put(operation, constructOneMeasurement(operation));
                }
            }
        }

        try {
            data.get(operation).measure(latency);
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
            e.printStackTrace();
            e.printStackTrace(System.out);
        }
    }
    
    /**
     * Report a single value of a single metric, but reports also the time
     * when the query was started. E.g. for read latency, operation="READ" and 
     * latency is the measured value.
     * 
     * @param operation
     * @param queryStartTime 
     * @param latency 
     */
    public void measure(String operation, long queryStartTime, int latency) {
        if (!data.containsKey(operation)) {
            synchronized (this) {
                if (!data.containsKey(operation)) {
                    data.put(operation, constructOneMeasurement(operation));
                }
            }
        }

        try {
            data.get(operation).measure(queryStartTime, latency);
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
            e.printStackTrace();
            e.printStackTrace(System.out);
        }
    }

    /**
     * Report a return code for a single DB operation.
     */
    public void reportReturnCode(String operation, int code) {
        if (!data.containsKey(operation)) {
            synchronized (this) {
                if (!data.containsKey(operation)) {
                    data.put(operation, constructOneMeasurement(operation));
                }
            }
        }
        data.get(operation).reportReturnCode(code);
    }

    /**
     * Export the current measurements to a suitable format.
     *
     * @param exporter Exporter representing the type of format to write to.
     * @throws IOException Thrown if the export failed.
     */
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        for (OneMeasurement measurement : data.values()) {
            measurement.exportMeasurements(exporter);
        }
    }

    /**
     * Return a one line summary of the measurements.
     */
    public String getSummary() {
        String ret = "";
        for (OneMeasurement m : data.values()) {
            ret += m.getSummary() + " ";
        }

        return ret;
    }

    
}
