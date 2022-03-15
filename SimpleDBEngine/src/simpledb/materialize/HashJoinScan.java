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
    Map<Integer, TempTable> temps1;
    Map<Integer, TempTable> temps2;
    Plan p2;
    
    Map<Integer, List<Map<String, Constant>>> inMemoryHashMap;
    List<Map<String, Constant>> matchingS2;
    // Scan for current pair of partition. s1 is from partition 1. s2 is from partition 2.
    Scan s1, s2;
    int positionS2 = 0;
    boolean hasmore1;
    // fldname and value for current matching record from s2
    Map<String, Constant> currentS2Val;

    /**
     * Create a hash join scan for the two sets of partitions.
     * @param temps1 the map of partitions for the LHS query plan
     * @param temps2 the map of partitions for the RHS query plan
     * @param p2 the RHS query plan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     */
    public HashJoinScan(Map<Integer, TempTable> temps1, 
        Map<Integer, TempTable> temps2, Plan p2, String fldname1, String fldname2) {
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
        if (inMemoryHashMap.size() != 0) {
            s1.beforeFirst();
        }
    }

    /**
     * Move to the next record, 
     *  or start the build then probe/join on a new pair of partitions.
     * 
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        // if there is no available/matching s2 partition, no join record
        if (inMemoryHashMap.size() == 0) {
            return false;
        }
        // Probe/Join Phase
        // 1. match the current s1 with all possible matching s2
        while (matchingS2 != null && positionS2 < matchingS2.size()) {
            if (s1.getVal(fldname1).equals(matchingS2.get(positionS2).get(fldname2))) {
                currentS2Val = matchingS2.get(positionS2);
                positionS2++;
                return true;
            }
            positionS2++;
        }

        // 2. recurse with the next value in the current s1 partition if applicable
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

        // Build Phase
        // 1. check if there are more s2 partitions
        // 2. if there are, find the matching s1 and s2 partition
        //    and build the next in_memory hashtable
        boolean hasNextPartition = buildNextInMemoryHashTable();
        if (!hasNextPartition) {
            return false;
        }
        return next();
    }

    /**
     * Build in memory hash table for the next partition.
     * Assumption: We will always have a buffer size that is big enough 
     *  to build an in-memory hash table.
     * In the case that we don't have such buffer size, 
     *  we will use other join algorithm instead of hash join.
     * 
     * @return true if we successfully build an in-memory hash table,
     *         false otherwise.
     */
    private boolean buildNextInMemoryHashTable() {
        inMemoryHashMap = new HashMap<>();
        TempTable tempTable2 = null;
        if (temps2.size() == 0){
            return false;
        }
        Set<Integer> allTemps2Keys = temps2.keySet();

        // build hash table for first s2 partition that has a matching s1 partition
        // 1. find the s2 partition
        for (Integer key : allTemps2Keys) {
            TempTable tempTable1 = temps1.get(key);
            // skip s2 partitions does not have a matching s1 partition
            if (tempTable1 == null) {
                temps2.remove(key);
            } else {
                tempTable2 = temps2.get(key);
                s1 = (UpdateScan) tempTable1.open();
                temps2.remove(key);
                break;
            }
        }
        s2 = (UpdateScan) tempTable2.open();
        s2.beforeFirst();

        // 2. build hash table for the found suitable s2 partition
        boolean hasmore2 = s2.next();
        while (hasmore2) {
            int currentHash = s2.getVal(fldname2).hashCode();
            List<Map<String, Constant>> currentScan = inMemoryHashMap.get(currentHash);
            if (currentScan == null) {
                currentScan = new ArrayList<>();
            }
            Map<String, Constant> s2fields = new HashMap<>();
            for (String fldname : p2.schema().fields()) {
                s2fields.put(fldname, s2.getVal(fldname));
            }
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
     * Do nothing since we have already close all s1 and s2.
     * 
     * @see simpledb.query.Scan#close()
     */
    public void close() {
    }
}
