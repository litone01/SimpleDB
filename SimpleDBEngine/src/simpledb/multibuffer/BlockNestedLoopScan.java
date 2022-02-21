package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

public class BlockNestedLoopScan implements Scan {
    private Transaction tx;
    private Scan rhsscan, lhsscan=null, nestedLoopScan;
    private String filename;
    private Layout layout;
    private int chunksize, nextblknum, filesize;
    private String fldname1, fldname2;
    
    /**
    * Creates the scan class for the product of the RHS scan and a table.
    * @param rhsscan the RHS scan
    * @param layout the metadata for the LHS table
    * @param tx the current transaction
    */
    public BlockNestedLoopScan(Transaction tx, Scan rhsscan, String tblname, Layout layout, String fildname1, String fildname2) {
        this.tx = tx;
        this.filename = tblname + ".tbl";
        this.layout = layout;
        this.rhsscan = rhsscan;
        this.fldname1 = fildname1;
        this.fldname2 = fildname2;
        filesize = tx.size(filename);
        int available = tx.availableBuffs();
        chunksize = BufferNeeds.bestFactor(available, filesize);
        beforeFirst();
    }
    
    /**
    * Positions the scan before the first record.
    * That is, the LHS scan is positioned at its first record,
    * and the RHS scan is positioned before the first record of the first chunk.
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
    * @see simpledb.query.Scan#next()
    */
    public boolean next() {
        while (!nestedLoopScan.next()) 
            if (!useNextChunk())
                return false;
        return true;
    }
    
    /**
    * Closes the current scans.
    * @see simpledb.query.Scan#close()
    */
    public void close() {
        nestedLoopScan.close();
    }
    
    /** 
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
    public Constant getVal(String fldname) {
        return nestedLoopScan.getVal(fldname);
    }
    
    /** 
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
    public int getInt(String fldname) {
        return nestedLoopScan.getInt(fldname);
    }
    
    /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
    public String getString(String fldname) {
        return nestedLoopScan.getString(fldname);
    }
    
    /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
    public boolean hasField(String fldname) {
        return nestedLoopScan.hasField(fldname);
    }
    
    /**
     * Move on to the next chunk of lhs records.
     * Note that this is different from MultiBufferProductJoinScan, 
     * where they do a chunkscan on rhs and product scan on lhs.
     * 
     * @return true if there are more chunks of lhs records
     */
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
        nestedLoopScan = new NestedLoopScan(lhsscan, rhsscan, fldname1, fldname2);
        nextblknum = end + 1;
        return true;
    }
}
