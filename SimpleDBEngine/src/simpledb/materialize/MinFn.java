package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MinFn implements AggregationFn {

    private String fldname;
    private Constant val;

    public MinFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Use the current record of the specified scan
     * to be the first record in the group.
     *
     * @param s the scan to aggregate over.
     */
    @Override
    public void processFirst(Scan s) {
        val = s.getVal(fldname);
    }

    /**
     * Use the current record of the specified scan
     * to be the next record in the group.
     *
     * @param s the scan to aggregate over.
     */
    @Override
    public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) < 0)
            val = newval;
    }

    /**
     * Return the name of the new aggregation field.
     *
     * @return the name of the new aggregation field
     */
    @Override
    public String fieldName() {
        return "minof" + fldname;
    }

    /**
     * Return the computed aggregation value.
     *
     * @return the computed aggregation value
     */
    @Override
    public Constant value() {
        return val;
    }
}
