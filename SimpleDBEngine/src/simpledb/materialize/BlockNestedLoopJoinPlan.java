package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class BlockNestedLoopJoinPlan implements Plan {

    private Transaction tx;
    private Plan lhs, rhs;
    private Schema schema = new Schema();
    private String fldname1, fldname2;

    public BlockNestedLoopJoinPlan(Plan lhs, Plan rhs, String fldname1, String fldname2, Transaction tx) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        schema.addAll(lhs.schema());
        schema.addAll(rhs.schema());
    }

    /**
     * Opens a scan corresponding to this plan.
     * The scan will be positioned before its first record.
     *
     * @return a scan
     */
    @Override
    public Scan open() {
        Scan leftscan = lhs.open();
        Scan rightscan = rhs.open();
        TempTable lhstt = copyRecordsFrom(rhs);
        TempTable rhstt = copyRecordsFrom(rhs);
        return new BlockNestedLoopJoinScan(tx, lhstt, rightscan, fldname1, fldname2);
//        return new BlockNestedLoopJoinScan(tx, lhstt, rhstt, fldname1, fldname2);
    }

    /**
     * Returns an estimate of the number of block accesses
     * that will occur when the scan is read to completion.
     *
     * @return the estimated number of block accesses
     */
    @Override
    public int blocksAccessed() {
        int avail = tx.availableBuffs();
        int size = new MaterializePlan(tx, rhs).blocksAccessed();
        int numchunks = size / avail;
        return rhs.blocksAccessed() +
                (lhs.blocksAccessed() * numchunks);
    }

    /**
     * Returns an estimate of the number of records
     * in the query's output table.
     *
     * @return the estimated number of output records
     */
    @Override
    public int recordsOutput() {
        return lhs.recordsOutput() * rhs.recordsOutput();
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
        if (lhs.schema().hasField(fldname))
            return lhs.distinctValues(fldname);
        else
            return rhs.distinctValues(fldname);
    }

    /**
     * Returns the schema of the query.
     *
     * @return the query's schema
     */
    @Override
    public Schema schema() {
        return schema;
    }

    private TempTable copyRecordsFrom(Plan p) {
        Scan   src = p.open();
        Schema sch = p.schema();
        TempTable t = new TempTable(tx, sch);
        UpdateScan dest = (UpdateScan) t.open();
        while (src.next()) {
            dest.insert();
            for (String fldname : sch.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.close();
        return t;
    }
}
