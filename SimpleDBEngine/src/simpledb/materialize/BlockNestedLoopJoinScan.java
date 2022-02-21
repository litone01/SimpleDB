package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.multibuffer.ChunkScan;
import simpledb.query.Constant;
import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class BlockNestedLoopJoinScan implements Scan {
    private Transaction tx;
    private int chunksize;

    private String fldname1, fldname2;
    private int outerBlockNum, innerBlockNum, filesize1, filesize2;
    private Layout layout1, layout2;

    private TempTable lhs, rhs;
    private String filename1, filename2;

    private Scan outerBlock;
    private Scan innerBlock;
    private Scan nestedLoopScan;

    private Scan rhsscan;
    private Scan lhsscan;

    public BlockNestedLoopJoinScan(Transaction tx, TempTable lhs, TempTable rhs, String fldname1, String fldname2) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;

        this.filename1 = lhs.tableName() + ".tbl";
        this.filesize1 = tx.size(filename1);
        this.filename2 = rhs.tableName() + ".tbl";
        this.filesize2 = tx.size(filename2);

        this.layout1 = lhs.getLayout();
        this.layout2 = rhs.getLayout();

        int available = tx.availableBuffs();
        chunksize = BufferNeeds.bestFactor(available, filesize1);

        beforeFirst();
    }

    public BlockNestedLoopJoinScan(Transaction tx, TempTable lhs, Scan rhsScan, String fldname1, String fldname2) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;

        this.filename1 = lhs.tableName() + ".tbl";
        this.filesize1 = tx.size(filename1);

        this.layout1 = lhs.getLayout();

        this.rhsscan = rhsScan;

        int available = tx.availableBuffs();
        chunksize = BufferNeeds.bestFactor(available, filesize1);

        beforeFirst();
    }

    /**
     * Position the scan before its first record. A
     * subsequent call to next() will return the first record.
     */
    @Override
    public void beforeFirst() {
        outerBlockNum = 0;
        innerBlockNum = 0;
        useNextChunk();
//        makeNextBlockJoin();
    }

    /**
     * Move the scan to the next record.
     *
     * @return false if there is no next record
     */
    @Override
    public boolean next() {
//        if(this.nestedLoopScan.next()){
//            return true;
//        } else if(makeNextBlockJoin()){
//            this.nestedLoopScan.next();
//            return true;
//        } else{
//            return false;
//        }
        while(!nestedLoopScan.next()){
            if(!useNextChunk()){
                return false;
            }
        }
        return true;
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
        return nestedLoopScan.getInt(fldname);
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
        return nestedLoopScan.getString(fldname);
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
        return nestedLoopScan.getVal(fldname);
    }

    /**
     * Return true if the scan has the specified field.
     *
     * @param fldname the name of the field
     * @return true if the scan has that field
     */
    @Override
    public boolean hasField(String fldname) {
        return nestedLoopScan.hasField(fldname);
    }

    /**
     * Close the scan and its subscans, if any.
     */
    @Override
    public void close() {
        nestedLoopScan.close();
    }

    private boolean useNextChunk() {
        if (outerBlockNum >= filesize1)
            return false;
        if (lhsscan != null)
            lhsscan.close();
        int end = outerBlockNum + chunksize - 1;
        if (end >= outerBlockNum)
            end = filesize1 - 1;
        lhsscan = new ChunkScan(tx, filename1, layout1, outerBlockNum, end);
        rhsscan.beforeFirst();
        nestedLoopScan = new NestedLoopJoinScan(lhsscan, rhsscan, fldname1, fldname2);
        outerBlockNum = end + 1;
        return true;
    }

//    public Scan getNextOuterBlock() {
//        if(outerBlockNum >= filesize1){
//            return null;
//        }
//
//        Scan outerChunk = new ChunkScan(this.tx, filename1, layout1, outerBlockNum, outerBlockNum);
//        outerBlockNum++;
//        return outerChunk;
//    }
//
//    public Scan getNextInnerBlock() {
//        if (innerBlock != null)
//            innerBlock.close();
//        Scan innerChunk =  new ChunkScan(this.tx, filename2, layout2, innerBlockNum, innerBlockNum);
//        innerBlockNum++;
//        return innerChunk;
//    }
//
//    public boolean makeNextBlockJoin() {
//        if(outerBlockNum>=filesize1) {
//            if(innerBlockNum >= filesize2) {
//                return false;
//            } else {
//                innerBlock = getNextInnerBlock();
//            }
//        } else{
//            if(innerBlockNum >= filesize2) {
//                innerBlock.beforeFirst();
//                outerBlock = getNextOuterBlock();
//            } else{
//                innerBlock = getNextInnerBlock();
//                outerBlock = getNextOuterBlock();
//            }
//        }
//        nestedLoopScan = new NestedLoopJoinScan(outerBlock, innerBlock, fldname1, fldname2);
//        return true;
//    }
}
