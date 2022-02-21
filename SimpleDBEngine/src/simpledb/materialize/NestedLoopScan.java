package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.multibuffer.ChunkScan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class NestedLoopScan implements Scan {
    private Transaction tx;
    private Scan lhsscan = null, rhsscan, blockNestedLoopScan;
    private String filename;
    private Layout layout;
    private int chunksize, nextblknum, filesize;
    private String fldname1, fldname2;

    /**
     * Creates the scan class for the nested loop scan.
     * 
     * @param lhsscan the RHS scan
     * @param tblname the LHS table name
     * @param layout  the metadata for the LHS table
     * @param tx      the current transaction
     */
    public NestedLoopScan(Transaction tx, Scan rhsscan, String tblname, 
            Layout layout, String fldname1, String fldname2) {
        this.tx = tx;
        this.rhsscan = rhsscan;
        this.filename = tblname + ".tbl";
        this.layout = layout;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        filesize = tx.size(filename);
        int available = tx.availableBuffs();
        chunksize = BufferNeeds.bestFactor(available, filesize);
        beforeFirst();
    }

    /**
     * Positions the scan before the first record.
     * The LHS scan is positioned before the first record of the first chunk,
     * and the RHS scan could be positioned before the first record.
     * 
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        nextblknum = 0;
        useNextChunk();
    }

    /**
     * Moves to the next record in the current scan.
     * If there are no more records in the current chunk,
     * then move to the next LHS record and the beginning of that chunk.
     * If there are no more LHS records, then move to the next chunk
     * and begin again.
     * 
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        while (!blockNestedLoopScan.next())
            if (!useNextChunk())
                return false;
        return true;
    }

    /**
     * Closes the current scans.
     * 
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        blockNestedLoopScan.close();
    }

    /**
     * Returns the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        return blockNestedLoopScan.getVal(fldname);
    }

    /**
     * Returns the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        return blockNestedLoopScan.getInt(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        return blockNestedLoopScan.getString(fldname);
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * 
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return blockNestedLoopScan.hasField(fldname);
    }

    private boolean useNextChunk() {
        if (nextblknum >= filesize)
            return false;
        if (lhsscan != null)
            lhsscan.close();
        int end = nextblknum + chunksize - 1;
        if (end >= filesize)
            end = filesize - 1;
        lhsscan = new ChunkScan(tx, filename, layout, nextblknum, end);
        rhsscan.beforeFirst();
        blockNestedLoopScan = 
            new BlockNestedLoopScan(lhsscan, rhsscan, fldname1, fldname2);
        nextblknum = end + 1;
        return true;
    }
}
