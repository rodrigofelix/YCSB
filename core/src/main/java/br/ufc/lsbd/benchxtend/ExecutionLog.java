/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rodrigofelix
 */
public class ExecutionLog {

    public static String LOG_PATH = "/Users/rodrigofelix/Mestrado/outputs/";
    public static final String EXECUTION = "execution.log";
    public static final String TIMELINE = "timeline.log";

    public static void persist(ArrayList entries, String type) {
        if (entries.size() > 0) {
            try {
                // creates the execution log file
                FileWriter stream = new FileWriter(LOG_PATH + type);
                BufferedWriter output = new BufferedWriter(stream);

                String value = "";
                // writes each entry in the log
                for (int i = 0; i < entries.size(); i++) {
                    if(entries.get(i) == null){
                        value = "";
                    }else{
                        value = entries.get(i).toString();
                    }
                    output.write(value);
                    output.newLine();
                }

                // closes the log instance
                output.close();
            } catch (Exception e) {
                System.out.println(e);
                Logger.getLogger(ExecutionLog.class.getName()).log(Level.INFO, "An error occured when saving the execution log file{0}", e.getMessage());
            }
        }
    }
}
