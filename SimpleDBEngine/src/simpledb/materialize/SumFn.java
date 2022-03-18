package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

import java.util.HashSet;
import java.util.Set;

public class SumFn implements AggregationFn {

    private String fldname;
    private int sum;
    private boolean isDistinct;
    private Set<Integer> distinctValues;
    /**
     * Create a sum aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     * @param isDistinct boolean value indicating whether the aggregate is distinct
     */
    public SumFn(String fldname, boolean isDistinct) {
        this.fldname = fldname;
        this.isDistinct = isDistinct;
        distinctValues = new HashSet<>();
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
        if(isDistinct) {
            distinctValues = new HashSet<>();
            distinctValues.add(s.getInt(fldname));
        } else{
            sum = s.getInt(fldname);
        }
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always increments the count,
     * and adds the current value to sum regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    @Override
    public void processNext(Scan s) {

        if(isDistinct) {
            distinctValues.add(s.getInt(fldname));
        } else{
            sum += s.getInt(fldname);
        }
    }

    /**
     * Return the field's name, prepended by "sumof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    @Override
    public String fieldName() {
        if(isDistinct){
            return "sumofdistinct" + fldname;
        } else{
            return "sumof" + fldname;
        }

    }

    /**
     * Return the current sum.
     * @see simpledb.materialize.AggregationFn#value()
     */
    @Override
    public Constant value() {
        if(isDistinct){
            int distinctSum = 0;
            for(Integer i: distinctValues) {
                distinctSum += i;
            }
            return new Constant(distinctSum);
        } else {
            return new Constant(sum);
        }
    }
}
