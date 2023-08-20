package simpledb.query;

/**
 * The class that denotes values stored in the database.
 *
 * @author Edward Sciore
 */
public class Constant implements Comparable<Constant> {
    private Integer ival = null;
    private String sval = null;

    boolean isNull = false;

    public Constant(Integer ival) {
        this.ival = ival;
    }

    public Constant(String sval) {
        this.sval = sval;
    }

    public Constant(boolean isNull) {
        this.isNull = isNull;
    }

    public int asInt() {
        return ival;
    }

    public String asString() {
        return sval;
    }

    public boolean isNull() {
        return isNull;
    }

    public boolean equals(Object obj) {
        if (isNull)
            return false;
        Constant c = (Constant) obj;
        return (ival != null) ? ival.equals(c.ival) : sval.equals(c.sval);
    }

    public int compareTo(Constant c) {
        if (isNull)
            throw new RuntimeException("illegal operation: can not compare null");
        return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
    }

    public int hashCode() {
        if (isNull)
            throw new RuntimeException("illegal operation: can not hash null");
        return (ival != null) ? ival.hashCode() : sval.hashCode();
    }

    public String toString() {
        if (isNull)
            throw new RuntimeException("illegal operation: can not stringify null");
        return (ival != null) ? ival.toString() : sval.toString();
    }
}
