/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.configuration;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.extended.ToAttributedValueConverter;

/**
 *
 * @author rodrigofelix
 */
@XStreamAlias("entry")
@XStreamConverter(value=ToAttributedValueConverter.class, strings={"value"})
public class Entry {
    public float time;
    public int value;
    
    public Entry(){
        this.time = 0f;
        this.value = 1;
    }
    
    public Entry(float time, int value){
        this.time = time;
        this.value = value;        
    }
}
