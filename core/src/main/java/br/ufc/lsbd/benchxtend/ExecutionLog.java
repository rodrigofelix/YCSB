/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rodrigofelix
 */
public class ExecutionLog {

    public static final String EXECUTION = "dasds/execution.log";
    public static final String TIMELINE = "dasd/timeline.log";

    public static void persist(ArrayList entries, String type) {

        // gets the path where jar is being executed
//        String path = ExecutionLog.class.getProtectionDomain().getCodeSource().getLocation().getPath();
//        String decodedPath = path;

//        try {
//            decodedPath = URLDecoder.decode(path, "UTF-8");

            if (entries.size() > 0) {

                // prints log in the regular output
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i) == null) {
                        System.out.println("");
                    } else {
                        System.out.println(entries.get(i).toString());
                    }
                }
                
//                try {
//                    // creates the execution log file
//                    FileWriter stream = new FileWriter(decodedPath + type);
//                    BufferedWriter output = new BufferedWriter(stream);
//
//                    String value = "";
//                    // writes each entry in the log
//                    for (int i = 0; i < entries.size(); i++) {
//                        if (entries.get(i) == null) {
//                            value = "";
//                        } else {
//                            value = entries.get(i).toString();
//                        }
//                        output.write(value);
//                        output.newLine();
//                    }
//
//                    // closes the log instance
//                    output.close();
//                } catch (Exception e) {
//                    System.out.println(e);
//                    Logger.getLogger(ExecutionLog.class.getName()).log(Level.WARNING, "An error occured when saving the execution log file{0}", e.getMessage());
//
//
//                    System.out.println("Entrei na segunda excecao");
//
//                    // prints log in the regular output
//                    for (int i = 0; i < entries.size(); i++) {
//                        if (entries.get(i) == null) {
//                            System.out.println("");
//                        } else {
//                            System.out.println(entries.get(i).toString());
//                        }
//                    }
//                }
            }
//        } catch (UnsupportedEncodingException ex) {
//            Logger.getLogger(ExecutionLog.class.getName()).log(Level.WARNING, "Impossible to get path to save log", ex.getMessage());
//
//
//            System.out.println("Entrei na primeira excecao");
//
//            // prints log in the regular output
//            for (int i = 0; i < entries.size(); i++) {
//                if (entries.get(i) == null) {
//                    System.out.println("");
//                } else {
//                    System.out.println(entries.get(i).toString());
//                }
//            }
//        }
    }
}
