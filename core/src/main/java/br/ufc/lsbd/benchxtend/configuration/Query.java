/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.configuration;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 *
 * @author rodrigofelix
 */
@XStreamAlias("query")
public class Query {
    
    @XStreamAsAttribute
    public String type;
    
    @XStreamAsAttribute
    public int time;
    
}
