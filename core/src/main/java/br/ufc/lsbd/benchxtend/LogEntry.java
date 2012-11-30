/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend;

/**
 *
 * @author rodrigofelix
 */
public class LogEntry {
    public int time;
    public int numberClients;
    
    public LogEntry(int time, int numberClients){
        this.time = time;
        this.numberClients = numberClients;
    }
    
    @Override
    public String toString(){
        return this.time + ","+ this.numberClients;
    }
}
