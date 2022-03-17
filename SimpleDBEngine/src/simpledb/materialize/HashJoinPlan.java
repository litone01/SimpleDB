package simpledb.materialize;

import java.util.Map;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {

   private Transaction tx;
   private Plan p1, p2;
   private String fldname1, fldname2; 
   private int NUM_PARTITION;
   private Schema schema = new Schema();
   
   /**
    * Create a hash join plan for the specified queries.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx the calling transaction
    */
   public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
      this.tx = tx;
      this.p1 = p1;
      this.p2 = p2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      // Assumption: We use 1 input buffer. No output buffer is used as we use iterator.
      NUM_PARTITION = tx.availableBuffs() - 1;
      schema.addAll(p1.schema());
      schema.addAll(p2.schema());
   }

   /**
    * The method first hashs both plans (tables) into respective partitions.
    * It then returns a hash join scan of the two sets of partitions. 
    */
    public Scan open() {
      Scan src1 = p1.open();
      HashJoinPartition partitionPlan1 = new HashJoinPartition(tx, p1, fldname1, NUM_PARTITION);
      Map<Integer, TempTable> partition1 = partitionPlan1.generatePartition(src1);
      src1.close();

      Scan src2 = p2.open();
      HashJoinPartition partitionPlan2 = new HashJoinPartition(tx, p2, fldname2, NUM_PARTITION);
      Map<Integer, TempTable> partition2 = partitionPlan2.generatePartition(src2);
      src2.close();
      return new HashJoinScan(partition1, partition2, p2, fldname1, fldname2);
   }

   /**
    * Return the number of block acceses required.
    * This is calculated based on the formula:
    *    3 * (|p1| + |p2|)
    *    where |p1| is the number of pages for table represented by p1
    *          |p2| is the number of pages for table represented by p2
    * As usual, we assume that we have enough buffers to hold the 
    *    in-memory hash table for the largest partition present.
    */
   @Override
   public int blocksAccessed() {
      Plan mp1 = new MaterializePlan(tx, p1);
      Plan mp2 = new MaterializePlan(tx, p2);
      return 3 * (mp1.blocksAccessed() + mp2.blocksAccessed());
   }

   /**
    * Return the number of records in the join.
    * Assuming that there is a uniform distribution.
    */
   @Override
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                           p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
   }

   /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    */
   @Override
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }

   @Override
   public Schema schema() {
      return schema;
   }

   /**
    * Return the string representation of this query plan
    */
   public String toString() {
      return "( " + p1.toString() + " hash join " + p2.toString() + 
      " on " + fldname1 + "=" + fldname2 + " )";
   }
}
