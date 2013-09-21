/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package MRMinimalist;

/**
 *
 * @author hadoop
 */
public class Points implements Comparable<Points> {
    
    public double x;
    public String Record;
    
    public int compareTo(Points p) {

        if (x < p.x) {
            return -1;
        }

        if (x > p.x) {
            return 1;
        }

        return 0;
    }
}
