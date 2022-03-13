package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn{
    private String fldname;
    private int sum;
    private int count;
    private int avg;

    public AvgFn(String fldname) {
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
        count = 1;
        sum = s.getInt(fldname);
    }

    /**
     * Use the current record of the specified scan
     * to be the next record in the group.
     *
     * @param s the scan to aggregate over.
     */
    @Override
    public void processNext(Scan s) {
        int curr = s.getInt(fldname);
        sum += curr;
        count++;
        avg = sum/count;
    }

    /**
     * Return the name of the new aggregation field.
     *
     * @return the name of the new aggregation field
     */
    @Override
    public String fieldName() {
        return "avgof" + fldname;
    }

    /**
     * Return the computed aggregation value.
     *
     * @return the computed aggregation value
     */
    @Override
    public Constant value() {
        return new Constant(avg);
    }
}
