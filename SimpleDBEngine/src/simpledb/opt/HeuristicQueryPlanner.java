package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;
import simpledb.materialize.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;
   
   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      
      // Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
         tableplanners.add(tp);
      }
      
      // Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();

      System.out.println("1------------");
      System.out.println(currentplan.toString());
      
      // Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null) {
            currentplan = p;
         } else { // no applicable join
            currentplan = getLowestProductPlan(currentplan);
         }
      }

      System.out.println("2------------");
      System.out.println(currentplan.toString());
      
      // Execute group by and aggregate functions if applicable
      if(!data.aggregationFns().isEmpty() || !data.groupByFields().isEmpty()){
         currentplan = new GroupByPlan(tx, currentplan, data.groupByFields(), data.aggregationFns());
      }

      System.out.println("3------------");
      System.out.println(currentplan.toString());

      // Project on the field names and return
      currentplan = new ProjectPlan(currentplan, data.fields());

      System.out.println("4------------");
      System.out.println(currentplan.toString());

      // Remove any duplicate output tuples if distinct is specified
      if(data.isDistinct()){
         currentplan = new DistinctPlan(currentplan, data.fields(), tx);
      }

      System.out.println("5------------");
      System.out.println(currentplan.toString());

      // Add a SortPlan if an order by clause is specified
      if (!data.orderByClause().isEmpty()) {
         System.out.println("[Log]: Adding a sort plan on fields " + data.orderByClause().toString());
         currentplan = new SortPlan(data.orderByClause(), tx, currentplan);
      }

      System.out.println("6------------");
      System.out.println(currentplan.toString());
      
      return currentplan;
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeJoinPlan(current);
         if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
            besttp = tp;
            bestplan = plan;
         }
      }
      if (bestplan != null) {
         tableplanners.remove(besttp);
      }
      return bestplan;
   }
   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }
}
