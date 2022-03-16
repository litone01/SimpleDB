package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn{
    private String fldname;
    private int sum;
    private int count;
    private int avg;

    /**
     * Create a avg aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public AvgFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new average.
     * Since SimpleDB does not support null values,
     * every value of record will be added to sum
     * and every record will be counted regardless of the field.
     * The current count is thus set to 1,
     * and the current sum is set to the value of the current record.
     * The current avg is set to sum
     * since avg = sum/count = sum/1 = sum in this step.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    @Override
    public void processFirst(Scan s) {
        count = 1;
        sum = s.getInt(fldname);
        avg = sum;
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always increments the count,
     * and adds the current value to sum regardless of the field.
     * Then it updates avg by calculating sum/count.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    @Override
    public void processNext(Scan s) {
        int curr = s.getInt(fldname);
        sum += curr;
        count++;
        avg = sum/count;
    }

    /**
     * Return the field's name, prepended by "avgof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    @Override
    public String fieldName() {
        return "avgof" + fldname;
    }

    /**
     * Return the current avg.
     * @see simpledb.materialize.AggregationFn#value()
     */
    @Override
    public Constant value() {
        return new Constant(avg);
    }
}
