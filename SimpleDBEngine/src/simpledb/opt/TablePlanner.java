package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.NestedLoopPlan;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
      // Plan p = makeIndexJoin(current, currsch);
      // If used block nested loop join as default, uncomment this
      Plan p = makeNestedLoopJoin(current, currsch);
      // If used merge join as default, uncomment this
      // Plan p = makeMergeJoin(current, currsch);
      // If used hash join as default, uncomment this
      // Plan p = makeHashJoin(current, currsch);
      // if (p == null)
      //    p = makeProductJoin(current, currsch);
      // Plan p = makeProductJoin(current, currsch);
      return p;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            String indexType = ii.getIndexType();
            System.out.println(indexType + " index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val, fldname);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   // Note that myplan is the LHS of the join, and current is the RHS of the join
   // Similarly, fldname is the LHS field name, while the currfield is the RHS field name
   private Plan makeNestedLoopJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String currfield = mypred.equatesWithField(fldname);
         if (currfield != null && currsch.hasField(currfield)) {
            Operator opr = mypred.getMatchedOperatorByTermFieldNames(fldname, currfield);
            Plan nestedLoopJoinPlan = 
               new NestedLoopPlan(tx, current, myplan, currfield, fldname, opr);
            nestedLoopJoinPlan = addSelectPred(nestedLoopJoinPlan);
            return addJoinPred(nestedLoopJoinPlan, currsch);
         }
      }
      return null;
   }

   private Plan makeHashJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String currfield = mypred.equatesWithField(fldname);
         if (currfield != null && currsch.hasField(currfield)) {
            Plan hashJoinPlan = 
               new HashJoinPlan(tx, current, myplan, currfield, fldname);
            hashJoinPlan = addSelectPred(hashJoinPlan);
            return addJoinPred(hashJoinPlan, currsch);
         }
      }
      return null;
   }
     
   private Plan makeMergeJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String currfield = mypred.equatesWithField(fldname);
         if (currfield != null && currsch.hasField(currfield)) {
            Plan mergeJoinPlan = new MergeJoinPlan(tx, current, myplan, currfield, fldname);
            mergeJoinPlan = addSelectPred(mergeJoinPlan);
            Plan result = addJoinPred(mergeJoinPlan, currsch);
            return result;
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
