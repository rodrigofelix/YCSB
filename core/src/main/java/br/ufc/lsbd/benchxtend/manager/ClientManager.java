/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.manager;

import br.ufc.lsbd.benchxtend.LogEntry;
import br.ufc.lsbd.benchxtend.configuration.*;
import com.yahoo.ycsb.ClientThread;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBFactory;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.measurements.Measurements;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rodrigofelix
 */
public class ClientManager {

    public ArrayList<ClientThread> clients;
    public Workload workload;
    public Distribution distribution;
    public ArrayList<LogEntry> timelineHistory;
    public String dbName;
    public int currentTimelineIndex = 0;
    public Timer timer;
    // step size (in seconds) to be considered when interpolate
    // between two Entries defined by the user in the config file
    public static float INTERPOLATION_STEP = 1f;

    public ClientManager(Distribution distribution, Sla sla, Workload workload) {
        this.clients = new ArrayList<ClientThread>();
        this.distribution = distribution;
        this.workload = workload;
        this.timelineHistory = new ArrayList<LogEntry>();
        this.dbName = workload.properties.getProperty("db", "com.yahoo.ycsb.BasicDB");
    }

    public void init() {
        if (distribution.timeline.size() > 0) {
            int initialValue = distribution.timeline.get(0).value;

            // duplicates the config timeline and depending on the distribution type
            // inserts intermediate values between each two defined entries
            generateTimeline();

            if (initialValue > 0) {
                // set the start time of the workload
                workload.startTime = System.nanoTime();
                
                Measurements.setStartTime(workload.startTime);

                // create first group of clients
                add(distribution.timeline.get(0).value);

                // creates the timer to varies the number of clients according
                // to the timeline previously generated
                timer = new Timer();
                VariationTask task = new VariationTask(this);
                timer.schedule(task, (long) (INTERPOLATION_STEP * 1000), (long) (INTERPOLATION_STEP * 1000));
            } else {
                Logger.getLogger(ClientManager.class.getName()).log(Level.SEVERE, null, "Timeline entry must be greater than zero");
            }
        } else {
            Logger.getLogger(ClientManager.class.getName()).log(Level.SEVERE, null, "At least one entry must be defined in the timeline");
        }
    }

    public void add() {
        add(1);
    }

    public void add(int number) {
        if (number > 0) {
            ClientThread client = null;
            for (int i = 0; i < number; i++) {

                // creates the connection before start the thread to measure only
                // the time to execute the queries, not counting open connection time
                DB db = null;
                try {
                    db = DBFactory.newDB(this.dbName, this.workload.properties);
                } catch (UnknownDBException e) {
                    System.out.println("Unknown DB " + this.dbName);
                    System.exit(0);
                }

                //client = new Client(config, workload);
                client = new ClientThread(db, this.workload.dotransactions, workload, i, number, this.workload.properties, -1, -1);
                clients.add(client);
                try {
                    // starts the thread that will send the queries
                    client.start();
                    // adds a new entry in the history
                    // TODO: define how to save the number of clients
                    timelineHistory.add(new LogEntry(getIntervalFromBeginning(), clients.size()));
                } catch (Exception ex) {
                    Logger.getLogger(ClientManager.class.getName()).log(Level.WARNING, null, "Error when executing a client");
                }

                Logger.getLogger(ClientManager.class.getName()).log(Level.FINE, null, "Created client");
            }
        }
    }

    public void remove() {
        remove(1);
    }

    public void remove(int number) {
        if (number > 0) {
            Random generator = null;
            for (int i = 0; i < number; i++) {
                // selects a random client to be removed
                generator = new Random();
                int index = generator.nextInt(this.clients.size());
                if (index > -1) {
                    this.clients.get(index).setStopRequested(true);
                    this.clients.remove(index);
                    // adds a new entry in the history
                    // TODO: define how to save the number of clients
                    timelineHistory.add(new LogEntry(getIntervalFromBeginning(), clients.size()));
                }
            }
        }
    }

    public int getIntervalFromBeginning() {
        return (int) ((System.nanoTime() - this.workload.startTime) / 1000);
    }

    public void generateTimeline() {
        ArrayList<Entry> originalEntries = new ArrayList<Entry>();

        // makes a deep copy of original entries
        for (int i = 0; i < this.distribution.timeline.size(); i++) {
            originalEntries.add(new Entry(this.distribution.timeline.get(i).time, this.distribution.timeline.get(i).value));
        }

        Entry current;
        Entry next;
        int steps = 0;
        int count = 0;
        // defines the step between each two points
        for (int i = 0; i < originalEntries.size(); i++) {
            // skips the last point
            if (i != originalEntries.size() - 1) {
                // gets the two entries that will be considered as initial and final values
                current = originalEntries.get(i);
                next = originalEntries.get(i + 1);

                // calculates the number of steps
                if (current.time > next.time) {
                    Logger.getLogger(ClientManager.class.getName()).log(Level.SEVERE, null, "Entries in the timeline must have ascending time values");
                } else {
                    // increments the count to insert new items in the correct position
                    count++;

                    // truncates the value if not integer
                    steps = (int) ((next.time - current.time) / INTERPOLATION_STEP) - 1;

                    Entry newItem;
                    // create the intermediate values according to the distribution type
                    for (int j = 0; j < steps; j++) {
                        // calculates the y value (ie. the number of clients) and creates a new entry
                        newItem = new Entry(current.time + (((float) j + 1f) * INTERPOLATION_STEP), Interpolation.getIntermediateValue(current, next, j + 1, INTERPOLATION_STEP, this.distribution.type));
                        // adds the new entry in the right position to keep it ordered ascendly
                        this.distribution.timeline.add(count, newItem);
                        // increments the count to insert new items in the correct position
                        count++;
                    }
                }
            }
        }
    }
}
