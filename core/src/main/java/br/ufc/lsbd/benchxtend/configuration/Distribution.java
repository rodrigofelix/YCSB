/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import java.util.ArrayList;

/**
 *
 * @author rodrigofelix
 */
@XStreamAlias("distribution")
public class Distribution {
    @XStreamAsAttribute
    public String type;
    public ArrayList<Entry> timeline;
}
