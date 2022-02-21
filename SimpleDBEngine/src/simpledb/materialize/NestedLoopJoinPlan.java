package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class NestedLoopJoinPlan implements Plan {
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private Transaction tx;


    public NestedLoopJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.fldname1 = fldname1;
        this.p1 = p1;

        this.fldname2 = fldname2;
        this.p2 = p2;

        this.tx = tx;

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    /**
     * Opens a scan corresponding to this plan.
     * The scan will be positioned before its first record.
     *
     * @return a scan
     */
    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new NestedLoopJoinScan(s1, s2, fldname1, fldname2);
    }

    /**
     * Returns an estimate of the number of block accesses
     * that will occur when the scan is read to completion.
     *
     * @return the estimated number of block accesses
     */
    @Override
    public int blocksAccessed() {
        int p1BlocksAccessed = p1.blocksAccessed();
        int p2BlocksAccessed = p2.blocksAccessed();
        return p1BlocksAccessed + p1BlocksAccessed * p2BlocksAccessed;
    }

    /**
     * Returns an estimate of the number of records
     * in the query's output table.
     *
     * @return the estimated number of output records
     */
    @Override
    public int recordsOutput() {
        return p1.recordsOutput();
    }

    /**
     * Returns an estimate of the number of distinct values
     * for the specified field in the query's output table.
     *
     * @param fldname the name of a field
     * @return the estimated number of distinct field values in the output
     */
    @Override
    public int distinctValues(String fldname) {
        if(p1.schema().hasField(fldname)){
            return p1.distinctValues(fldname);
        } else {
            return p2.distinctValues(fldname);
        }
    }

    /**
     * Returns the schema of the query.
     *
     * @return the query's schema
     */
    @Override
    public Schema schema() {
        return sch;
    }
}
