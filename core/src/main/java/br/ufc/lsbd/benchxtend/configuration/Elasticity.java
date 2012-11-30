/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.configuration;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 *
 * @author rodrigofelix
 */
@XStreamAlias("elasticity")
public class Elasticity {
    
    public float overprovisionWeight;
    
    public float underprovisionWeight;
    
    public float overprovisionLambda;
    
}
