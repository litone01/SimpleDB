package simpledb.materialize;

import java.util.HashMap;
import java.util.Map;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPartition {

    private Transaction tx;
    private Plan p;
    private String fldname;
    private Map<Integer, UpdateScan> updateScanTables;
    private Map<Integer, TempTable> tempTables;

    private int numPartition;

    /**
     * Create a hash join partition plan for the specified query.
     * 
     * @param p            the plan for the underlying query
     * @param tx           the calling transaction
     * @param fldname      fldname in the join predicate
     * @param numPartition number of partition, usually set to available buffer - 2
     */
    public HashJoinPartition(Transaction tx, Plan p, String fldname, int numPartition) {
        this.tx = tx;
        this.p = p;
        this.fldname = fldname;
        this.numPartition = numPartition;
        updateScanTables = new HashMap<>();
    }

    /**
     * Generate the partitions for hash join
     * 
     * @param p            the plan for the underlying query
     * @param src           scan for one of the table
     */
    public Map<Integer, TempTable>  generatePartition(Scan src) {
        tempTables = new HashMap<>();
        src.beforeFirst();
        if (!src.next())
            return tempTables;

        while (copyToPartition(p, src)) {
        }

        for (UpdateScan currentUpdateScan : updateScanTables.values()) {
            currentUpdateScan.close();
        }

        return tempTables;
    }

    /**
     * copy records to the correct partition
     * 
     * @param p            the plan for the underlying query
     * @param src           scan for one of the table
     */
    private boolean copyToPartition(Plan p, Scan src) {
        Schema sch = p.schema();
        int currentHash = src.getVal(fldname).hashCode() % numPartition;
        UpdateScan currentScan = updateScanTables.get(currentHash);
        if (currentScan == null) {
            TempTable newTempTable = new TempTable(tx, sch);
            tempTables.put(currentHash, newTempTable);
            currentScan = (UpdateScan) newTempTable.open();
            updateScanTables.put(currentHash, currentScan);
        }
        currentScan.insert();
        for (String fldname : sch.fields())
            currentScan.setVal(fldname, src.getVal(fldname));
        return src.next();
    }

}
