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
package com.yahoo.ycsb;

import br.ufc.lsbd.benchxtend.configuration.*;
import br.ufc.lsbd.benchxtend.manager.ClientManager;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

//import org.apache.log4j.BasicConfigurator;
/**
 * Main class for executing YCSB.
 */
public class Client {

    public static final String OPERATION_COUNT_PROPERTY = "operationcount";
    public static final String RECORD_COUNT_PROPERTY = "recordcount";
    public static final String WORKLOAD_PROPERTY = "workload";
    /**
     * Indicates how many inserts to do, if less than recordcount. Useful for
     * partitioning the load among multiple servers, if the client is the
     * bottleneck. Additionally, workloads should support the "insertstart"
     * property, which tells them which record to start at.
     */
    public static final String INSERT_COUNT_PROPERTY = "insertcount";
    /**
     * The maximum amount of time (in seconds) for which the benchmark will be
     * run.
     */
    public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

    public static void usageMessage() {
        System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
        System.out.println("Options:");
        System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n"
                + "              \"threadcount\" property using -p");
        System.out.println("  -timeline xmlfile:  load the timeline process and its properties. If passed, timeline \n"
                + "              will overwrite -threads, since threads will be defined according to the timeline");
        System.out.println("  -sla xmlfile:  load the file with expected query times defined in the SLA");
        System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n"
                + "              be specified as the \"target\" property using -p");
        System.out.println("  -load:  run the loading phase of the workload");
        System.out.println("  -t:  run the transactions phase of the workload (default)");
        System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n"
                + "              can also be specified as the \"db\" property using -p");
        System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
        System.out.println("                   be specified, and will be processed in the order specified");
        System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
        System.out.println("                  multiple properties can be specified, and override any");
        System.out.println("                  values in the propertyfile");
        System.out.println("  -s:  show status during run (default: no status)");
        System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
        System.out.println("");
        System.out.println("Required properties:");
        System.out.println("  " + WORKLOAD_PROPERTY + ": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)");
        System.out.println("");
        System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
        System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
        System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
    }

    public static boolean checkRequiredProperties(Properties props) {
        if (props.getProperty(WORKLOAD_PROPERTY) == null) {
            System.out.println("Missing property: " + WORKLOAD_PROPERTY);
            return false;
        }

        if (props.getProperty("propfile") == null) {
            System.out.println("It is mandatory to pass a property file containing the hosts property");
            return false;
        }

        return true;
    }

    /**
     * Exports the measurements to either sysout or a file using the exporter
     * loaded from conf.
     *
     * @throws IOException Either failed to write to output stream or failed to
     * close it.
     */
    public static void exportMeasurements(Properties props, int opcount, long runtime)
            throws IOException {
        MeasurementsExporter exporter = null;
        try {
            // if no destination file is provided the results will be written to stdout
            OutputStream out;
            String exportFile = props.getProperty("exportfile");
            if (exportFile == null) {
                out = System.out;
            } else {
                out = new FileOutputStream(exportFile);
            }

            // if no exporter is provided the default text one will be used
            String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
            try {
                exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
            } catch (Exception e) {
                System.err.println("Could not find exporter " + exporterStr
                        + ", will use default text reporter.");
                e.printStackTrace();
                exporter = new TextMeasurementsExporter(out);
            }

            exporter.write("OVERALL", "RunTime(ms)", runtime);
            double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
            exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

            Measurements.getMeasurements().exportMeasurements(exporter);
        } finally {
            if (exporter != null) {
                exporter.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        String dbname;
        Properties props = new Properties();
        Properties fileprops = new Properties();
        Distribution distribution = null;
        Sla sla = null;
        boolean dotransactions = true;
        int threadcount = 1;
        int target = 0;
        boolean status = false;
        String label = "";

        //parse arguments
        int argindex = 0;

        if (args.length == 0) {
            usageMessage();
            System.exit(0);
        }

        while (args[argindex].startsWith("-")) {
            if (args[argindex].compareTo("-threads") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                int tcount = Integer.parseInt(args[argindex]);
                props.setProperty("threadcount", tcount + "");
                argindex++;
            } else if (args[argindex].compareTo("-timeline") == 0) {
                argindex++;
                // check if it was provided a next argument that is supposed
                // to be the xmlFile that defines the timeline
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }

                // loads the xmlFile and sets the suitable objects of Timeline
                try {
                    // creates object to read configuration xml file
                    XStream xstream = new XStream(new DomDriver());
                    xstream.processAnnotations(Distribution.class);

                    // read from XML
                    distribution = (Distribution) xstream.fromXML(new FileInputStream(new File(args[argindex])));

                    // sets distribution to true to identify that a timeline was provided
                    props.setProperty("distribution", "true");
                    argindex++;
                } catch (Exception ex) {
                    System.out.println("Error when loading timeline file: " + ex.toString());
                    // finishes execution
                    System.exit(0);
                }
            } else if (args[argindex].compareTo("-sla") == 0) {
                argindex++;
                // check if it was provided a next argument that is supposed
                // to be the xmlFile that defines the SLA
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }

                // loads the xmlFile and sets the suitable object of SLA
                try {
                    // creates object to read configuration xml file
                    XStream xstream = new XStream(new DomDriver());
                    xstream.processAnnotations(Sla.class);

                    // read from XML
                    sla = (Sla) xstream.fromXML(new FileInputStream(new File(args[argindex])));

                    // sets distribution to true to identify that an SLA was provided
                    props.setProperty("sla", "true");
                    argindex++;
                } catch (Exception ex) {
                    System.out.println("Error when loading timeline file: " + ex.toString());
                    // finishes execution
                    System.exit(0);
                }
            } else if (args[argindex].compareTo("-target") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                int ttarget = Integer.parseInt(args[argindex]);
                props.setProperty("target", ttarget + "");
                argindex++;
            } else if (args[argindex].compareTo("-load") == 0) {
                dotransactions = false;
                argindex++;
            } else if (args[argindex].compareTo("-t") == 0) {
                dotransactions = true;
                argindex++;
            } else if (args[argindex].compareTo("-s") == 0) {
                status = true;
                argindex++;
            } else if (args[argindex].compareTo("-db") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                props.setProperty("db", args[argindex]);
                argindex++;
            } else if (args[argindex].compareTo("-l") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                label = args[argindex];
                argindex++;
            } else if (args[argindex].compareTo("-P") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                String propfile = args[argindex];
                props.setProperty("propfile", args[argindex]);
                argindex++;

                Properties myfileprops = new Properties();
                FileInputStream stream;
                try {
                    stream = new FileInputStream(propfile);
                    myfileprops.load(stream);
                    stream.close();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    System.exit(0);
                }

                //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
                for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
                    String prop = (String) e.nextElement();

                    fileprops.setProperty(prop, myfileprops.getProperty(prop));
                }

            } else if (args[argindex].compareTo("-p") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                int eq = args[argindex].indexOf('=');
                if (eq < 0) {
                    usageMessage();
                    System.exit(0);
                }

                String name = args[argindex].substring(0, eq);
                String value = args[argindex].substring(eq + 1);
                props.put(name, value);
                //System.out.println("["+name+"]=["+value+"]");
                argindex++;
            } else {
                System.out.println("Unknown option " + args[argindex]);
                usageMessage();
                System.exit(0);
            }

            if (argindex >= args.length) {
                break;
            }
        }

        if (argindex != args.length) {
            usageMessage();
            System.exit(0);
        }

        //set up logging
        //BasicConfigurator.configure();

        //overwrite file properties with properties from the command line

        //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
        for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
            String prop = (String) e.nextElement();

            fileprops.setProperty(prop, props.getProperty(prop));
        }

        props = fileprops;

        if (!checkRequiredProperties(props)) {
            System.exit(0);
        }

        long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

        // get number of threads, target and db
        // threadcount will not be used if a distribution is provided
        threadcount = Integer.parseInt(props.getProperty("threadcount", "1"));
        dbname = props.getProperty("db", "com.yahoo.ycsb.BasicDB");
        target = Integer.parseInt(props.getProperty("target", "0"));

        // TODO: check if it is possible to have throughput definition
        // while there is a timeline defined

        //compute the target throughput
        double targetperthreadperms = -1;
        if (target > 0) {
            double targetperthread = ((double) target) / ((double) threadcount);
            targetperthreadperms = targetperthread / 1000.0;
        }

        System.out.println("YCSB Client 0.1");
        System.out.print("Command line:");
        for (int i = 0; i < args.length; i++) {
            System.out.print(" " + args[i]);
        }
        System.out.println();
        System.err.println("Loading workload...");
        System.out.println("Start time: " + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date()));

        //show a warning message that creating the workload is taking a while
        //but only do so if it is taking longer than 2 seconds 
        //(showing the message right away if the setup wasn't taking very long was confusing people)
        Thread warningthread = new Thread() {
            public void run() {
                try {
                    sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }
                System.err.println(" (might take a few minutes for large data sets)");
            }
        };

        warningthread.start();

        //set up measurements
        Measurements.setProperties(props);
        Measurements.setSla(sla);
        Measurements.setDistribution(distribution);

        //load the workload
        ClassLoader classLoader = Client.class.getClassLoader();

        Workload workload = null;

        try {
            Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

            workload = (Workload) workloadclass.newInstance();
            workload.properties = props;
            workload.dotransactions = dotransactions;
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            System.exit(0);
        }

        try {
            workload.init(props);
        } catch (WorkloadException e) {
            e.printStackTrace();
            e.printStackTrace(System.out);
            System.exit(0);
        }

        warningthread.interrupt();

        //run the workload

        System.err.println("Starting test.");

        int opcount;
        if (dotransactions) {
            opcount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
        } else {
            if (props.containsKey(INSERT_COUNT_PROPERTY)) {
                opcount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
            } else {
                opcount = Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, "0"));
            }
        }

        // 1st case: Follows BenchXtend approach varying the number of clients
        // load phase is let as an activity of basic flow
        if (dotransactions && distribution != null) {
            // instantiates manager to create (and remove) ClientThreads
            ClientManager manager = new ClientManager(distribution, sla, workload);
            manager.init();
        } else {
            // 2nd case: Follows the basic flow of YCSB
            Vector<Thread> threads = new Vector<Thread>();

            for (int threadid = 0; threadid < threadcount; threadid++) {
                DB db = null;
                try {
                    db = DBFactory.newDB(dbname, props);
                } catch (UnknownDBException e) {
                    System.out.println("Unknown DB " + dbname);
                    System.exit(0);
                }

                Thread t = new ClientThread(db, dotransactions, workload, threadid, threadcount, props, opcount / threadcount, targetperthreadperms);

                threads.add(t);
                //t.start();
            }

            StatusThread statusthread = null;

            if (status) {
                boolean standardstatus = false;
                if (props.getProperty("measurementtype", "").compareTo("timeseries") == 0) {
                    standardstatus = true;
                }
                statusthread = new StatusThread(threads, label, standardstatus);
                statusthread.start();
            }

            long st = System.currentTimeMillis();

            for (Thread t : threads) {
                t.start();
            }

            Thread terminator = null;

            if (maxExecutionTime > 0) {
                terminator = new TerminatorThread(maxExecutionTime, threads, workload);
                terminator.start();
            }

            int opsDone = 0;

            for (Thread t : threads) {
                try {
                    // wait for all threads to be completed
                    t.join();
                    opsDone += ((ClientThread) t).getOpsDone();
                } catch (InterruptedException e) {
                }
            }

            long en = System.currentTimeMillis();

            if (terminator != null && !terminator.isInterrupted()) {
                terminator.interrupt();
            }

            if (status) {
                statusthread.interrupt();
            }

            try {
                workload.cleanup();
            } catch (WorkloadException e) {
                e.printStackTrace();
                e.printStackTrace(System.out);
                System.exit(0);
            }

            try {
                exportMeasurements(props, opsDone, en - st);
            } catch (IOException e) {
                System.err.println("Could not export measurements, error: " + e.getMessage());
                e.printStackTrace();
                System.exit(-1);
            }

            System.exit(0);
        }
    }
}
