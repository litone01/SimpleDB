package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private RecordComparator comp;
   private boolean isOrderByClauseSpecified;
   private List<OrderByPair> orderByFields;
   private int numberOfSortedRuns = -1;
   
   /**
    * Create a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
      isOrderByClauseSpecified = false;
   }
   
   /**
    * Overloaded constructor, create a sort plan for the specified query.
    * @param orderByFields the fields to sort by based on the order by clause
    * @param tx the calling transaction
    * @param p the plan for the underlying query
    */
    public SortPlan(List<OrderByPair> orderByFields, Transaction tx, Plan p) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      isOrderByClauseSpecified = true;
      this.orderByFields = orderByFields;
      comp = new RecordComparator(orderByFields, isOrderByClauseSpecified);
   }

   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > 1)
         runs = doAMergeIteration(runs);
      return new SortScan(runs, comp);
   }
   
   /**
    * Returns an estimate of the number of block accesses that 
    * will occur when the scan is read to completion.
    * The number of blocks in the sorted table,
    * is the same as it would be in a
    * materialized table, given by mp.blocksAccessed().
    * The one-time cost
    * of materializing and sorting the records is done by fist getting the number of sorted runs.
    * If the splitIntoSortedRun method was called before we invoke blockAccessed(), 
    * the numbeOfSortedRuns will be the actual number of sorted runs. 
    * If not, we return an estimate of blockAccessed/bufferSize even if this sort plan do not 
    * sort in the same way as what we learnt in lecture (generate a certain number of sorted runs based on buffer available).
    * After getting the actual/estimate number of sorted runs, we take the number of iteration to be log based 2 of the number of sorted runs, 
    * as the sorting are done in a way that is similar to merge sort (merging two runs each time).
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      // include the cost of sorting
      if (numberOfSortedRuns == -1) {
         numberOfSortedRuns = (int) Math.ceil(mp.blocksAccessed() / SimpleDB.BUFFER_SIZE);
      }
      int numIteration = (int) Math.ceil(Math.log(numberOfSortedRuns) / Math.log(2)) + 1;
      return 2 * mp.blocksAccessed() * numIteration;
   }
   
   /**
    * Return the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Return the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      numberOfSortedRuns = 1;
      TempTable currenttemp = new TempTable(tx, sch);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
         // start a new run
         currentscan.close();
         numberOfSortedRuns++;
         currenttemp = new TempTable(tx, sch);
         temps.add(currenttemp);
         currentscan = (UpdateScan) currenttemp.open();
      }
      currentscan.close();
      return temps;
   }
   
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }
   
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(tx, sch);
      UpdateScan dest = result.open();
      
      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
            hasmore1 = copy(src1, dest);
         else
            hasmore2 = copy(src2, dest);
      
      if (hasmore1)
         while (hasmore1)
         hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
         hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }
   
   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }

   /**
    * Return the string representation of order by plan
    */
    @Override
    public String toString() {
       if (isOrderByClauseSpecified) {
          return p.toString() + " order by " + orderByFields.toString();
       } else {
          return p.toString();
       }
    }
}
