/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufc.lsbd.benchxtend.manager;

import br.ufc.lsbd.benchxtend.configuration.Entry;
import cern.jet.random.Poisson;
import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;
import java.util.Random;

/**
 *
 * @author rodrigofelix
 */
public class Interpolation {

    public static final String POISSON = "poisson";
    public static final String LINEAR = "linear";

    /**
     * Returns the number of clients for such a time
     *
     * @param current
     * @param next
     * @param index
     * @param step
     * @param type
     * @return
     */
    public static int getIntermediateValue(Entry current, Entry next, int index, float step, String type) {
        if (type.equals(POISSON)) {
            return getPoissonValue(current.value);
        } else if (type.equals(LINEAR)) {
            return getLinearValue(current, next, index, step);
        } else {
            return getLinearValue(current, next, index, step);
        }
    }

    private static int getPoissonValue(float lambda) {        
        RandomEngine engine = new DRand((new Random()).nextInt());
        Poisson poisson = new Poisson((double) lambda, engine);
        return poisson.nextInt();
    }

    private static int getLinearValue(Entry current, Entry next, int index, float step) {
        // considering Entry.time the x value and Entry.value the y value
        // calculates the m constant of a linear equation
        float m = (next.value - current.value) / (next.time - current.time);
        // x is the multiplication of index by the size of the step summed with
        // initial value, ie, current.time
        float x = current.time + index * step;
        // gets the y value from the linear equation: y = m * (x - x1) + y1
        return (int) (m * (x - current.time) + current.value);
    }
}
