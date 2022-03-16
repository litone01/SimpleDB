package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class SumFn implements AggregationFn {

    private String fldname;
    private int sum;

    /**
     * Create a sum aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public SumFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new sum.
     * Since SimpleDB does not support null values,
     * every value of record will be added to sum
     * and every record will be counted regardless of the field.
     * The current count is thus set to 1,
     * and the current sum is set to the value of the current record.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    @Override
    public void processFirst(Scan s) {
        sum = s.getInt(fldname);
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always increments the count,
     * and adds the current value to sum regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    @Override
    public void processNext(Scan s) {
        sum += s.getInt(fldname);
    }

    /**
     * Return the field's name, prepended by "sumof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    @Override
    public String fieldName() {
        return "sumof" + fldname;
    }

    /**
     * Return the current sum.
     * @see simpledb.materialize.AggregationFn#value()
     */
    @Override
    public Constant value() {
        return new Constant(sum);
    }
}
