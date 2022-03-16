package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class DistinctScan implements Scan {
    private Scan s;

    /**
     * Create a groupby scan, given a distinct table scan.
     * @param s the distinct scan
     */
    public DistinctScan(Scan s){
        this.s = s;
    }

    /**
     * Position the scan before its first record. A
     * subsequent call to next() will return the first record.
     */
    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    /**
     * Move the scan to the next record.
     *
     * @return false if there is no next record
     */
    @Override
    public boolean next() {
        return s.next();
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
        return s.getInt(fldname);
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
        return s.getString(fldname);
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
        return s.getVal(fldname);
    }

    /**
     * Return true if the scan has the specified field.
     *
     * @param fldname the name of the field
     * @return true if the scan has that field
     */
    @Override
    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    /**
     * Close the scan and its subscans, if any.
     */
    @Override
    public void close() {
        s.close();
    }
}
