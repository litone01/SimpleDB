package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;

public class HashJoinScan implements Scan {
    private String fldname1, fldname2;
    Map<Integer, TempTable> temps2;
    Map<Integer, TempTable> temps1;

    Map<Integer, List<Map<String, Constant>>> inMemoryHashMap;
    List<Map<String, Constant>> matchingS2;


    Scan s1, s2;
    Plan p2;
    int positionS2 = 0;
    boolean hasmore1;
    Map<String, Constant> currentS2Val;

    /**
     * Create a hash join plan for the specified query.
     * 
     * @param p  the plan for the underlying query
     * @param tx the calling transaction
     */
    public HashJoinScan(Map<Integer, TempTable> temps1, Map<Integer, TempTable> temps2, Plan p2, String fldname1, String fldname2) {
        this.temps1 = temps1;
        this.temps2 = temps2;
        this.p2 = p2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        beforeFirst();
    }

    /**
     * Build in memory hash table for the current partition of s2.
     * Position the scan before the first record of s1.
     * 
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        buildNextInMemoryHashTable();
        s1.beforeFirst();
    }

    /**
     * Move to the next record.
     * 
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {

        // match s1 with all matching s2
        while (matchingS2 != null && positionS2 < matchingS2.size()) {
            if (s1.getVal(fldname1).equals(matchingS2.get(positionS2).get(fldname2))) {
                currentS2Val = matchingS2.get(positionS2);
                positionS2++;
                return true;
            }
            positionS2++;
        }

        // probe
        hasmore1 = s1.next();

        while (hasmore1) {
            int s1Hash = s1.getVal(fldname1).hashCode();
            positionS2 = 0;
            matchingS2 = inMemoryHashMap.get(s1Hash);
            if (matchingS2 == null) {
                hasmore1 = s1.next();
                continue;
            }

            return next();
        }
        s1.close();

        // check if there is more s2 partitions
        boolean hasNextPartition = buildNextInMemoryHashTable();
        if (!hasNextPartition) {
            return false;
        }
        return next();
    }

    /**
     * Build in memory hash table for the next partition
     * Assumption: We will always have a buffer size that is big enough to build an in-memory hash table.
     * In the case that we don't have such buffer size, we will use other join algorithm instead of hash join.
     */
    private boolean buildNextInMemoryHashTable() {
        inMemoryHashMap = new HashMap<>();
        TempTable tempTable2 = null;

        if (temps2.size() == 0){
            return false;
        }

        Set<Integer> alltemps2keys = temps2.keySet();

        // build hash table for first s2 partition that has a matching s1 partition
        for (Integer key : alltemps2keys) {

            TempTable tempTable1 = temps1.get(key);
            // skip building if current s2 partition do not have a matching s1 partition
            if (tempTable1 != null) {

                tempTable2 = temps2.get(key);
                s1 = (UpdateScan) tempTable1.open();
                temps2.remove(key);
                break;
            } else {

                temps2.remove(key);
            }

        }

        s2 = (UpdateScan) tempTable2.open();
        s2.beforeFirst();


        boolean hasmore2 = s2.next();
        while (hasmore2) {
            int currentHash = s2.getVal(fldname2).hashCode();

            List<Map<String, Constant>> currentScan = inMemoryHashMap.get(currentHash);

            if (currentScan == null) {
                currentScan = new ArrayList<>();
            }

            Map<String, Constant> s2fields = new HashMap<>();
            for (String fldname : p2.schema().fields())
                s2fields.put(fldname, s2.getVal(fldname));

            currentScan.add(s2fields);



            inMemoryHashMap.put(currentHash, currentScan);
            hasmore2 = s2.next();
        }
        s2.close();
        return true;
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (s1.hasField(fldname))
        return s1.getInt(fldname);
     else
        return currentS2Val.get(fldname).asInt();
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        if (s1.hasField(fldname))
        return s1.getString(fldname);
     else
        return currentS2Val.get(fldname).asString();
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
        return s1.getVal(fldname);
     else
        return currentS2Val.get(fldname);

    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * 
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || currentS2Val.get(fldname) != null;
    }

    /**
     * Close both underlying scans. Do nothing since we have already close all s1 and s2.
     * 
     * @see simpledb.query.Scan#close()
     */
    public void close() {
    }

}
