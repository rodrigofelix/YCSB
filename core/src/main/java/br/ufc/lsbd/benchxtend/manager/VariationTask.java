/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.manager;

//import br.ufc.lsbd.benchxtend.log.ExecutionLog;
import br.ufc.lsbd.benchxtend.ExecutionLog;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.ClientThread;
import java.io.IOException;

/**
 *
 * @author rodrigofelix
 */
public class VariationTask implements Runnable {

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

            if (current != goal) {
                System.out.println("Changing clients from " + current + " to " + goal);
            }

            manager.currentTimelineIndex++;
        } else {
            // gets the last number of clients
            int last = manager.distribution.timeline.get(manager.currentTimelineIndex).value;

            System.out.println("Removing last " + last + " clients");

            // removes all the clients
            manager.remove(last);

            for (ClientThread t : manager.clients) {
                try {
                    // wait for all threads to be completed
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.out);
                }
            }

            // when everything is finished, shutdown the executor
            manager.executor.shutdown();

            long endTime = System.nanoTime();

            System.out.println("-------------------------------");
            System.out.println("Printing time, # of machines");

            // ExecutionLog.persist(manager.workload.measurements, ExecutionLog.EXECUTION);
            // TODO: figure out why this must be called before exportMeasurements 
            ExecutionLog.persist(manager.timelineHistory, ExecutionLog.TIMELINE);

            System.out.println("-------------------------------");

            // starts the process of persisting the execution log
            try {
                Client.exportMeasurements(manager.workload.properties, 0, (endTime - manager.workload.startTime) / (1000 * 1000));
            } catch (IOException e) {
                System.err.println("Could not export measurements, error: " + e.getMessage());
                System.exit(-1);
            }
        }
    }
}
