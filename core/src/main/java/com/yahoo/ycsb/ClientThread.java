/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb;

import java.util.Properties;

/**
 * A thread for executing transactions or data inserts to the database.
 *
 * @author cooperb
 *
 */
public class ClientThread extends Thread {

    DB _db;
    boolean _dotransactions;
    Workload _workload;
    int _opcount;
    double _target;
    int _opsdone;
    int _threadid;
    int _threadcount;
    Object _workloadstate;
    Properties _props;
    private boolean _stopRequested = false;

    /**
     * Constructor.
     *
     * @param db the DB implementation to use
     * @param dotransactions true to do transactions, false to insert data
     * @param workload the workload to use
     * @param threadid the id of this thread
     * @param threadcount the total number of threads
     * @param props the properties defining the experiment
     * @param opcount the number of operations (transactions or inserts) to do
     * @param targetperthreadperms target number of operations per thread per ms
     */
    public ClientThread(DB db, boolean dotransactions, Workload workload, int threadid, int threadcount, Properties props, int opcount, double targetperthreadperms) {
        _db = db;
        _dotransactions = dotransactions;
        _workload = workload;
        _opcount = opcount;
        _opsdone = 0;
        _target = targetperthreadperms;
        _threadid = threadid;
        _threadcount = threadcount;
        _props = props;
        //System.out.println("Interval = "+interval);
    }

    public int getOpsDone() {
        return _opsdone;
    }
    
    public void setStopRequested(boolean stopRequested){
        this._stopRequested = stopRequested;
    }
    
    public boolean isStopRequested(){
        return this._stopRequested;
    }

    @Override
    public void run() {
        try {
            _db.init();
        } catch (DBException e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return;
        }

        try {
            _workloadstate = _workload.initThread(_props, _threadid, _threadcount);
        } catch (WorkloadException e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            return;
        }

        //spread the thread operations out so they don't all hit the DB at the same time
        try {
            //GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
            //and the sleep() doesn't make sense for granularities < 1 ms anyway
            if ((_target > 0) && (_target <= 1.0)) {
                sleep(Utils.random().nextInt((int) (1.0 / _target)));
            }
        } catch (InterruptedException e) {
            // do nothing.
        }


        // checks if a timeline was provided. If yes, follows BenchXtend approach.
        // otherwise, follows the basic flow of YCSB
        if (_workload.properties.containsKey("distribution") && _workload.properties.getProperty("distribution").equals("true")) {
            // unlike YCSB, Benchxtend runs queries while it is not said to stop.
            // this is make in order to keep the number of clients according to 
            // the distribution defined in the timeline        

            while (!this.isStopRequested()) {
                long st = System.currentTimeMillis();
                if (_dotransactions) {
                    _workload.doTransaction(_db, _workloadstate);
                } else {
                    _workload.doInsert(_db, _workloadstate);
                }

                if (Workload.SLEEP_TIME_BETWEEN_QUERIES > 0) {
                    try {
                        // waits for 0.1 before doing a new transaction
                        Thread.sleep(Workload.SLEEP_TIME_BETWEEN_QUERIES);
                    } catch (InterruptedException ex) {
                        try {
                            _db.cleanup();
                        } catch (DBException e) {
                            e.printStackTrace(System.out);
                        }
                        return;
                    }
                } else {
                    if (Thread.interrupted()) {
                        try {
                            _db.cleanup();
                        } catch (DBException e) {
                            e.printStackTrace(System.out);
                        }
                        return;
                    }
                }
            }
        } else {
            try {
                if (_dotransactions) {
                    long st = System.currentTimeMillis();

                    while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested()) {

                        if (!_workload.doTransaction(_db, _workloadstate)) {
                            break;
                        }

                        _opsdone++;

                        // throttle the operations
                        if (_target > 0) {
                            //this is more accurate than other throttling approaches we have tried,
                            //like sleeping for (1/target throughput)-operation latency,
                            //because it smooths timing inaccuracies (from sleep() taking an int, 
                            //current time in millis) over many operations
                            while (System.currentTimeMillis() - st < ((double) _opsdone) / _target) {
                                try {
                                    sleep(1);
                                } catch (InterruptedException e) {
                                    // do nothing.
                                }

                            }
                        }
                    }
                } else {
                    long st = System.currentTimeMillis();

                    while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested()) {

                        if (!_workload.doInsert(_db, _workloadstate)) {
                            break;
                        }

                        _opsdone++;

                        //throttle the operations
                        if (_target > 0) {
                            //this is more accurate than other throttling approaches we have tried,
                            //like sleeping for (1/target throughput)-operation latency,
                            //because it smooths timing inaccuracies (from sleep() taking an int, 
                            //current time in millis) over many operations
                            while (System.currentTimeMillis() - st < ((double) _opsdone) / _target) {
                                try {
                                    sleep(1);
                                } catch (InterruptedException e) {
                                    // do nothing.
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                System.exit(0);
            }

            try {
                _db.cleanup();
            } catch (DBException e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                return;
            }
        }
    }
}