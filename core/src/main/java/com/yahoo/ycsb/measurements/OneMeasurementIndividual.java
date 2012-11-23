/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author rodrigofelix
 */
public class OneMeasurementIndividual extends OneMeasurement {

    long totallatency;
    int count=0;
    int min;
    int max;
    ArrayList<String> responseTimes;
    private HashMap<Integer, int[]> returncodes;

    public OneMeasurementIndividual(String name, Properties props) {
        super(name);
        totallatency = 0;
        min = -1;
        max = -1;
        responseTimes = new ArrayList<String>();
        returncodes = new HashMap<Integer, int[]>();
    }

    @Override
    public void reportReturnCode(int code) {
        Integer Icode = code;
        if (!returncodes.containsKey(Icode)) {
            int[] val = new int[1];
            val[0] = 0;
            returncodes.put(Icode, val);
        }
        returncodes.get(Icode)[0]++;
    }

    @Override
    public void measure(int latency) {

        responseTimes.add(latency + ", " + (int) System.currentTimeMillis());

        count++;
        totallatency += latency;

        if (latency > max) {
            max = latency;
        }

        if ((latency < min) || (min < 0)) {
            min = latency;
        }
    }

    @Override
    public String getSummary() {
        return "";
    }

    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        DecimalFormat d = new DecimalFormat("#.##");
        
        exporter.write(getName(), "Total Queries: ", count);
        exporter.write(getName(), "Total Latency: ", totallatency);
        exporter.write(getName(), "Average Latency (us): ", d.format(totallatency / count));
        exporter.write(getName(), "Min Latency (us): ", d.format(min / count));
        exporter.write(getName(), "Max Latency (us): ", d.format(max / count));
        
        for (int i = 0; i < responseTimes.size(); i++) {
            exporter.write(getName(), "Query", responseTimes.get(i));
        }
    }
}
