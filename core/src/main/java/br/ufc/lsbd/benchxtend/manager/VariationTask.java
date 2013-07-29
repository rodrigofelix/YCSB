/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.manager;

//import br.ufc.lsbd.benchxtend.log.ExecutionLog;
import br.ufc.lsbd.benchxtend.ExecutionLog;
import br.ufc.lsbd.benchxtend.LogEntry;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.ClientThread;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author rodrigofelix
 */
public class VariationTask implements Runnable {

    public ClientManager manager;
    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

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
            } else {
                // in this case, there is no need to change the number of clients, but
                // there must be logged the number of clients was kept the same
                int total_active = 0;

                // gets the list of active clients
                for (int j = 0; j < manager.clients.size(); j++) {
                    if (!manager.clients.get(j).isStopRequested()) {
                        total_active++;
                    }
                }
                
                // adds a new entry in the history
                manager.timelineHistory.add(new LogEntry(ClientManager.getIntervalFromBeginning(manager.workload.startTime), total_active));
            }

            if (current != goal) {
                output("Changing clients from " + current + " to " + goal);
            } else {
                output("Keeping " + current + " clients");
            }

            manager.currentTimelineIndex++;
        } else {
            // gets the last number of clients
            int last = manager.distribution.timeline.get(manager.currentTimelineIndex).value;

            output("Removing last " + last + " clients");

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
            System.out.println("Printing time, # of clients");

            // ExecutionLog.persist(manager.workload.measurements, ExecutionLog.EXECUTION);
            // TODO: figure out why this must be called before exportMeasurements 
            ExecutionLog.persist(manager.timelineHistory, ExecutionLog.TIMELINE);

            System.out.println("-------------------------------");

            // starts the process of persisting the execution log
            try {
                Client.exportMeasurements(manager.workload.properties, 0, (endTime - manager.workload.startTime) / (1000 * 1000));
            } catch (IOException e) {
                output("Could not export measurements, error: " + e.getMessage());
                System.exit(-1);
            }
        }
    }
    
    public void output(String value) {
        Date date = new Date();
        System.out.println(new StringBuilder("[").append(dateFormat.format(date)).append("]").append(" ").append(value));
    }
}
