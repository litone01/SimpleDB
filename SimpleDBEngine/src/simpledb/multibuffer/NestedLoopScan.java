package simpledb.multibuffer;

import simpledb.query.*;

public class NestedLoopScan implements Scan {
    private Scan s1, s2;
    private String fldname1, fldname2;
    private boolean hasmore1, hasmore2;

    /**
    * Create a product scan having the two underlying scans.
    * @param s1 the LHS scan
    * @param s2 the RHS scan
    */
    public NestedLoopScan(Scan s1, Scan s2, String fldname1, String fldname2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        beforeFirst();
    }

    /**
     * Position the scan before its first record.
    * In particular, the LHS scan is positioned before 
    * its first record, and the RHS scan
    * is positioned before its first record.
    * @see simpledb.query.Scan#beforeFirst()
    */
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
        hasmore2 = s2.next();
    }

    /**
    * Move the scan to the next record.
    * The method first check if there are any 
    * @see simpledb.query.Scan#next()
    */
    public boolean next() { 
        hasmore1 = s1.next();
        if (!hasmore1) {
            return false;
        } 

        while (hasmore2) {
            // for the current record at s2, compare with each record at s1
            while (hasmore1) {
                System.out.println("[DEBUG] sid:"+ s2.getVal("sid") +"; s1:" + s1.getVal(fldname1) + "; s2:" + s2.getVal(fldname2));
                // if two records equal, return true
                if (s1.getVal(fldname1).equals(s2.getVal(fldname2))) {
                    return true;
                } else {
                    // else, move to the next record at s1
                    hasmore1 = s1.next();
                }
            }
            // no more s1
            // finished comparing the current chunk of records in s1 with the current record of s2
            // reset s1, move s2 to the next record
            s1.beforeFirst();
            hasmore2 = s2.next();
            hasmore1 = s1.next();
        }
        // reset s2?
        s2.beforeFirst();

        // no more s2 records
        // finished  comparing the current chunk of records in s1 with the whole scan of s2
        return false;
    }

    /** 
     * Return the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
    public int getInt(String fldname) {
        if (s1.hasField(fldname))
        return s1.getInt(fldname);
        else
        return s2.getInt(fldname);
    }

    /** 
     * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
    public String getString(String fldname) {
        if (s1.hasField(fldname))
        return s1.getString(fldname);
        else
        return s2.getString(fldname);
    }

    /** 
     * Return the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
        return s1.getVal(fldname);
        else
        return s2.getVal(fldname);
    }

    /**
     * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    /**
     * Close both underlying scans.
    * @see simpledb.query.Scan#close()
    */
    public void close() {
        s1.close();
        s2.close();
    }
}
