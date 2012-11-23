/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.manager;

//import br.ufc.lsbd.benchxtend.log.ExecutionLog;
import com.yahoo.ycsb.Client;
import java.io.IOException;
import java.util.TimerTask;

/**
 *
 * @author rodrigofelix
 */
public class VariationTask extends TimerTask {

    public ClientManager manager;

    public VariationTask(ClientManager manager) {
        this.manager = manager;
    }

    @Override
    public void run() {
        if (manager.currentTimelineIndex < manager.distribution.timeline.size() - 1) {
            // gets the current number of clients
            int current = manager.distribution.timeline.get(manager.currentTimelineIndex).value;
            
            // gets the aimed number of clients
            int goal = manager.distribution.timeline.get(manager.currentTimelineIndex + 1).value;

            // takes the decision of adding or removing clients
            if (current > goal) {
                manager.remove(current - goal);
            } else if (current < goal) {
                manager.add(goal - current);
            }

            System.out.println("Changing clients from " + current + " to " + goal);
            
            manager.currentTimelineIndex++;
        } else {
            // gets the last number of clients
            int last = manager.distribution.timeline.get(manager.currentTimelineIndex).value;
            
            // removes all the clients
            manager.remove(last);
            
            long endTime = System.currentTimeMillis();
            
            System.out.println("Removing last " + last + " clients");
            
            // stops the timer
            manager.timer.cancel();
            
            // starts the process to persists the execution log
            // TODO: define a way to persist the logs calling the exportMeasurements here
            // See end of Client.java source file 
            
            try {
                Client.exportMeasurements(manager.workload.properties, 0, endTime - manager.workload.startTime);
            } catch (IOException e) {
                System.err.println("Could not export measurements, error: " + e.getMessage());
                e.printStackTrace();
                System.exit(-1);
            }
            
            // ExecutionLog.persist(manager.workload.measurements, ExecutionLog.EXECUTION);
            // ExecutionLog.persist(manager.timelineHistory, ExecutionLog.TIMELINE);
        }
    }
}
