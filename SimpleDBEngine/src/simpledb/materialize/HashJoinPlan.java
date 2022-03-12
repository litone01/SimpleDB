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
    * Create a hash join plan for the specified query.
    * @param p the plan for the underlying query
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
    * This method is where most of the action is.
    * @see simpledb.plan.Plan#open()
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

   @Override
   public int blocksAccessed() {
      int sizep1 = new MaterializePlan(tx, p1).blocksAccessed();
      int sizep2 = new MaterializePlan(tx, p2).blocksAccessed();
      return 3 * (sizep1 + sizep2);
   }

   @Override
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
         p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
   }

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

    
}
