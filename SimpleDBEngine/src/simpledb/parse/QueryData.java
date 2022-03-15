package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   // If no order by clause is specified, orderByClause would be an empty list.
   private List<OrderByPair> orderByClause;
   private List<AggregationFn> aggregationFns;
   private List<String> groupByFields;

   /**
    * Saves the field and table list and predicate and the order by clause.
    */
    public QueryData(List<String> fields, Collection<String> tables, List<AggregationFn> aggregationFns, Predicate pred, List<String> groupByFields ,List<OrderByPair> orderByClause) {
//       fields.addAll(groupByFields);
       this.fields = fields;
      this.tables = tables;
      this.aggregationFns = aggregationFns;
      this.pred = pred;
      this.groupByFields = groupByFields;
      this.orderByClause = orderByClause;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   /**
    * Returns the order by clause that describes the field names
    * and their corresponding order by types. 
    * If the order by clause is not set, an empty list will be returned.
    * @return a list of OrderByPair objects, if order by clause is presnet,
    *         otherwise empty list.
    */
   public List<OrderByPair> orderByClause() {
      return orderByClause;
   }

   public List<AggregationFn> aggregationFns() {
      return aggregationFns;
   }

   public List<String> groupByFields() {
      return groupByFields;
   }

   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma

      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma

      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;

      if (!groupByFields.isEmpty()) {
         result += "group by ";
         for (String groupby : groupByFields) {
            result += groupby + ", ";
         }
         result = result.substring(0, result.length()-2); //remove final comma
      }

      if (!orderByClause.isEmpty()) {
         result += "order by ";
         for (OrderByPair obp : orderByClause) {
            result += obp.toString() + ", ";
         }
         result = result.substring(0, result.length()-2); //remove final comma
      }


      return result;
   }
}
