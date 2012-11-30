/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.configuration;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import java.util.ArrayList;

/**
 *
 * @author rodrigofelix
 */
@XStreamAlias("sla")
public class Sla {
    
    @XStreamImplicit
    public ArrayList<Query> queries;
    
    public int getTimeByType(String type){
        for(Query q : queries){
            if (q.type == null ? type == null : q.type.equals(type)){
                return q.time;
            }
        }
        return -1;
    }
    
}
