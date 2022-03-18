package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MinFn implements AggregationFn {

    private String fldname;
    private Constant val;
    private boolean isDistinct;

    /**
     * Create a min aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     * @param isDistinct boolean value indicating whether the aggregate is distinct
     */
    public MinFn(String fldname, boolean isDistinct) {
        this.fldname = fldname;
        this.isDistinct = isDistinct;
    }

    /**
     * Start a new minimum to be the
     * field value in the current record.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    @Override
    public void processFirst(Scan s) {
        val = s.getVal(fldname);
    }

    /**
     * Replace the current minimum by the field value
     * in the current record, if it is smaller.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    @Override
    public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) < 0)
            val = newval;
    }

    /**
     * Return the field's name, prepended by "minof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    @Override
    public String fieldName() {
        return "minof" + fldname;
    }

    /**
     * Return the current minimum.
     * @see simpledb.materialize.AggregationFn#value()
     */
    @Override
    public Constant value() {
        return val;
    }
}
