package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.query.*;

public class NestedLoopPlan implements Plan {
    private Transaction tx;
    private Plan lhs, rhs;
    private Schema schema = new Schema();
    private String fldname1, fldname2;

    /**
     * Creates a nested loop plan for the specified queries.
     * 
     * @param lhs the plan for the LHS query
     * @param rhs the plan for the RHS query
     * @param tx  the calling transaction
     */
    public NestedLoopPlan(Transaction tx, Plan lhs, Plan rhs, String fldname1, String fldname2) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = new MaterializePlan(tx, rhs);
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        schema.addAll(lhs.schema());
        schema.addAll(rhs.schema());
    }

    public Scan open() {
        Scan rightScan = rhs.open();
        TempTable tt = copyRecordsFrom(lhs);
        return new NestedLoopScan(tx, rightScan, tt.tableName(), tt.getLayout(), fldname1, fldname2);
    }

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query. The formula is:
     * 
     * <pre>
     * B(product(p1, p2)) = B(p2) + B(p1) * C(p2)
     * </pre>
     * 
     * where C(p2) is the number of chunks of p2.
     * The method uses the current number of available buffers
     * to calculate C(p2), and so this value may differ
     * when the query scan is opened.
     * 
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        // this guesses at the # of chunks
        int avail = tx.availableBuffs();
        int size = new MaterializePlan(tx, lhs).blocksAccessed();
        int numchunks = size / avail;
        return lhs.blocksAccessed() +
                (rhs.blocksAccessed() * numchunks);
    }

    /**
     * Estimates the number of output records in the product.
     * The formula is:
     * 
     * <pre>
     * R(product(p1, p2)) = R(p1) * R(p2)
     * </pre>
     * 
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return lhs.recordsOutput() * rhs.recordsOutput();
    }

    /**
     * Estimates the distinct number of field values in the product.
     * Since the product does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * 
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname))
            return lhs.distinctValues(fldname);
        else
            return rhs.distinctValues(fldname);
    }

    /**
     * Returns the schema of the join,
     * which is the union of the schemas of the underlying queries.
     * 
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }

    private TempTable copyRecordsFrom(Plan p) {
        Scan src = p.open();
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