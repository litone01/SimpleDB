package simpledb.materialize;

import simpledb.query.*;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The <i>count</i> aggregation function.
 * @author Edward Sciore
 */
public class CountFn implements AggregationFn {
   private String fldname;
   private int count;
   private boolean isDistinct;
   private Set<Constant> distinctValues;
   
   /**
    * Create a count aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    * @param isDistinct boolean value indicating whether the aggregate is distinct
    */
   public CountFn(String fldname, boolean isDistinct) {
      this.fldname = fldname;
      this.isDistinct = isDistinct;
      distinctValues = new HashSet<>();
   }
   
   /**
    * Start a new count.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current count is thus set to 1.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
      if (isDistinct) {
         distinctValues = new HashSet<>();
         distinctValues.add(s.getVal(fldname));
      } else {
         count = 1;
      }
//      if(isDistinct){
//         System.out.println(s.getVal(fldname) + " " + s.getVal("gradyear"));
//      }
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the count,
    * regardless of the field.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
      if(isDistinct){
         distinctValues.add(s.getVal(fldname));
      } else{
         count++;
      }
   }
   
   /**
    * Return the field's name, prepended by "countof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      if(isDistinct){
         return "countofdistinct" + fldname;
      } else {
         return "countof" + fldname;
      }
   }
   
   /**
    * Return the current count.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      if(isDistinct){
         int distinctCount = distinctValues.size();
         return new Constant(distinctCount);
      } else{
         return new Constant(count);
      }
   }
}
