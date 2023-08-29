package simpledb.query;

import java.util.Date;

/**
 * The class that denotes values stored in the database.
 *
 * @author Edward Sciore
 */
public class Constant implements Comparable<Constant> {
    private Integer ival = null;
    private String sval = null;

    private Short shortVal = null;

    private Boolean boolVal = null;

    private Date dateVal = null;

    public Constant(Integer ival) {
        this.ival = ival;
    }

    public Constant(String sval) {
        this.sval = sval;
    }

    public Constant(Short shortVal) {
        this.shortVal = shortVal;
    }

    public Constant(Boolean boolVal) {
        this.boolVal = boolVal;
    }

    public Constant(Date dateVal) {
        this.dateVal = dateVal;
    }

    public int asInt() {
        return ival;
    }

    public String asString() {
        return sval;
    }

    public short asShort() {
        return shortVal;
    }

    public Date asDate() {
        return dateVal;
    }

    public Boolean asBoolean() {
        return boolVal;
    }

    public boolean equals(Object obj) {
        Constant c = (Constant) obj;
        return (ival != null) ? ival.equals(c.ival) : sval.equals(c.sval);
    }

    public int compareTo(Constant c) {
        return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
    }

    public int hashCode() {
        return (ival != null) ? ival.hashCode() : sval.hashCode();
    }

    public String toString() {
        return (ival != null) ? ival.toString() : sval.toString();
    }
}
