/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb.measurements;

import br.ufc.lsbd.benchxtend.configuration.Distribution;
import br.ufc.lsbd.benchxtend.configuration.Sla;
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
    long workloadStartTime;
    Distribution distribution;
    Sla sla;
    ArrayList<int[]> responseTimes;
    private HashMap<Integer, int[]> returncodes;

    public OneMeasurementIndividual(String name, Properties props, Sla sla, Distribution distribution, long workloadStartTime) {
        super(name);
        this.workloadStartTime = workloadStartTime;
        this.sla = sla;
        this.distribution = distribution;
        totallatency = 0;
        min = -1;
        max = -1;
        responseTimes = new ArrayList<int[]>();
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

    /**
     * reports the time that a query was started and the spent time to execute
     * it. the moment a query started is important to check the number of 
     * active clients in that specific moment
     * 
     * @param startTime time when a single query was started
     * @param latency time spent up to execute the query
     */
    @Override
    public void measure(long startTime, int latency) {
        int[] values = new int[3];
        values[0] = latency;
        values[1] = (int) ((startTime - this.workloadStartTime) / 1000);
        
        responseTimes.add(values);

        count++;
        totallatency += latency;

        if (latency > max) {
            max = latency;
        }

        if ((latency < min) || (min < 0)) {
            min = latency;
        }
    }
    
    /**
     * considers the start time as the current time
     * @param latency 
     */
    @Override
    public void measure(int latency) {
        measure(System.nanoTime(), latency);
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
        
        ArrayList<Float> underprovSet = new ArrayList<Float>();
        ArrayList<Float> overprovSet = new ArrayList<Float>();
        long expectedTime = sla.getTimeByType(getName());
        
        for (int i = 0; i < responseTimes.size(); i++) {
            exporter.write(getName(), "Query", responseTimes.get(i)[0] + ", " + responseTimes.get(i)[1]);
            
            if(responseTimes.get(i)[0] > expectedTime){
                // calculates the rate violated / expected in an underprovisioning scenario
                underprovSet.add((float) responseTimes.get(i)[0] / expectedTime);
            }else if(responseTimes.get(i)[0] < expectedTime - (distribution.elasticity.overprovisionLambda * expectedTime)){
                // calculates the rate expected / violated in an overprovisioning scenario
                overprovSet.add((float) expectedTime / responseTimes.get(i)[0]);
            }
        }
        
        float underprov = 0;
        float overprov = 0;
        
        if (underprovSet.size() > 0) {
            float total = 0f;
            
            // calculates the sum of rates
            for(Float f : underprovSet){
                total += f;
            }
            
            // calculates actually underprov metric, ie. the arithmetic average of rates
            underprov = (total / (float) underprovSet.size());
            exporter.write(getName(), "Underprov: ", underprov);
            exporter.write(getName(), "Total of underprov queries: ", underprovSet.size());
        } else {
            exporter.write(getName(), "Underprov: ", 0);
            exporter.write(getName(), "Total of underprov queries: ", 0);
        }
        
        if (overprovSet.size() > 0) {
            float total = 0f;
            
            // calculates the sum of rates
            for(Float f : overprovSet){
                total += f;
            }
            
            // calculates actually underprov metric, ie. the arithmetic average of rates
            overprov = total / (float) overprovSet.size();
            exporter.write(getName(), "Overprov metric: ", overprov);
            exporter.write(getName(), "Total of overprov queries: ", overprovSet.size());
        } else {
            exporter.write(getName(), "Overprov metric: ", 0);
            exporter.write(getName(), "Total of overprov queries: ", 0);
        }
        
        // calculates the elasticitydb metric
        float elasticitydb;
        float x = distribution.elasticity.underprovisionWeight;
        float y = distribution.elasticity.overprovisionWeight;
        elasticitydb = (x * underprov + y * overprov) / (x + y);
        
        exporter.write(getName(), "Elasticitydb metric: ", elasticitydb);
    }   
}
