package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.tx.Transaction;


public class NestedLoopJoinScan implements Scan {

    private final Scan s1;
    private final Scan s2;
    private final String fldname1;
    private final String fldname2;
//    private final Transaction tx;

    public NestedLoopJoinScan(Scan s1, Scan s2, String fldname1, String fldname2) {
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.s1 = s1;
        this.s2 = s2;
//        this.tx = tx;
        beforeFirst();
    }

    /**
     * Position the scan before its first record. A
     * subsequent call to next() will return the first record.
     */
    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
        s1.next();
    }

    /**
     * Move the scan to the next record.
     *
     * @return false if there is no next record
     */
    @Override
    public boolean next() {
//        if(!s1.next() && !s2.next()) {
//            return false;
//        } else if(!s2.next()&&s1.next()){
//            s2.beforeFirst();
//            return true;
//        } else{
//            return true;
//        }

        while(true) {
            while(s2.next()) {
                if(s1.getVal(fldname1).equals(s2.getVal(fldname2))) {
                    return true;
                }
            }
            s2.beforeFirst();
            if(!s1.next()) {
                return false;
            }
        }
    }

    /**
     * Return the value of the specified integer field
     * in the current record.
     *
     * @param fldname the name of the field
     * @return the field's integer value in the current record
     */
    @Override
    public int getInt(String fldname) {
        if (s1.hasField(fldname)) {
            return s1.getInt(fldname);
        } else {
            return s2.getInt(fldname);
        }
    }

    /**
     * Return the value of the specified string field
     * in the current record.
     *
     * @param fldname the name of the field
     * @return the field's string value in the current record
     */
    @Override
    public String getString(String fldname) {
        if (s1.hasField(fldname)) {
            return s1.getString(fldname);
        } else {
            return s2.getString(fldname);
        }
    }

    /**
     * Return the value of the specified field in the current record.
     * The value is expressed as a Constant.
     *
     * @param fldname the name of the field
     * @return the value of that field, expressed as a Constant.
     */
    @Override
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname)) {
            return s1.getVal(fldname);
        } else {
            return s2.getVal(fldname);
        }
    }

    /**
     * Return true if the scan has the specified field.
     *
     * @param fldname the name of the field
     * @return true if the scan has that field
     */
    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    /**
     * Close the scan and its subscans, if any.
     */
    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}
