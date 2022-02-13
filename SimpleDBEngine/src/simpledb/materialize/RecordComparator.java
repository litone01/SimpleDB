package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<String> fields;
   private List<OrderByPair> orderByFields;
   private boolean isOrderByClauseSpecified = false;

   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   public RecordComparator(List<String> fields) {
      this.fields = fields;
   }

   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
    public RecordComparator(List<OrderByPair> orderByFields, boolean isOrderByClauseSpecified) {
      this.orderByFields = orderByFields;
      this.isOrderByClauseSpecified = isOrderByClauseSpecified;
      for (OrderByPair orderByField : orderByFields) {
        System.out.println("[SORT BY] fieldname: " + orderByField.field() + ", order: " + orderByField.order());
      }
   }

   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
      if (isOrderByClauseSpecified) {
         return compareByOrderByClause(s1, s2);
      } else {
         return compareByFields(s1, s2);
      }
   }

   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    *    different values, those values are used as the result
    *    of the comparison.
    * If the two records have the same values for all
    *    sort fields, then the method returns 0.
    * This by default sorts based on ascending order.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   private int compareByFields(Scan s1, Scan s2) {
      for (String fldname : fields) {
         Constant val1 = s1.getVal(fldname);
         Constant val2 = s2.getVal(fldname);
         int result = val1.compareTo(val2);
         if (result != 0)
            return result;
      }
      return 0;
   }


   /**
    * Used only when the order clause is specified.
    * The sort fields are considered in turn based on 
    *    their position in the order by clause specified, from left to right,
    *    and its order type, 'asc' or 'desc'.
    * When a field is encountered for which the records have
    *    different values, those values are used as the result
    *    of the comparison.
    * If the two records have the same values for all
    *    order by fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the order by pair list
    */
   private int compareByOrderByClause(Scan s1, Scan s2) {
      for (OrderByPair orderByPair : orderByFields) {
         String fldname = orderByPair.field();
         Constant val1 = s1.getVal(fldname);
         Constant val2 = s2.getVal(fldname);
         switch (orderByPair.order()) {
            case ASC:
               int result = val1.compareTo(val2);
               if (result != 0)
                  return result;
               break;
            case DESC:
               result = val2.compareTo(val1);
               if (result != 0)
                  return result;
               break;
         }
         
      }
      return 0;
   }
}
