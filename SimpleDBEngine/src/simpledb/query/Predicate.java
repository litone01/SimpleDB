package simpledb.query;

import java.util.*;

import simpledb.plan.Plan;
import simpledb.record.*;

/**
 * A predicate is a Boolean combination of terms.
 * @author Edward Sciore
 *
 */
public class Predicate {
   private List<Term> terms = new ArrayList<Term>();
   // We assume that a query with where clause like "t1.a = t2.b and t2.b > t1.a",
   //    where pair of 'a' and 'b' fields are used more than 1 time, 
   //    is invalid.

   /**
    * Create an empty predicate, corresponding to "true".
    */
   public Predicate() {}

   /**
    * Create a predicate containing a single term.
    * @param t the term
    */
   public Predicate(Term t) {
      terms.add(t);
   }

   /**
    * Modifies the predicate to be the conjunction of
    * itself and the specified predicate.
    * @param pred the other predicate
    */
   public void conjoinWith(Predicate pred) {
      terms.addAll(pred.terms);
   }

   /**
    * Returns true if the predicate evaluates to true
    * with respect to the specified scan.
    * @param s the scan
    * @return true if the predicate is true in the scan
    */
   public boolean isSatisfied(Scan s) {
      for (Term t : terms)
         if (!t.isSatisfied(s))
            return false;
      return true;
   }

   /** 
    * Calculate the extent to which selecting on the predicate 
    * reduces the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * predicate cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */ 
   public int reductionFactor(Plan p) {
      int factor = 1;
      for (Term t : terms)
         factor *= t.reductionFactor(p);
      return factor;
   }

   /**
    * Return the subpredicate that applies to the specified schema.
    * @param sch the schema
    * @return the subpredicate applying to the schema
    */
   public Predicate selectSubPred(Schema sch) {
      Predicate result = new Predicate();
      for (Term t : terms)
         if (t.appliesTo(sch))
            result.terms.add(t);
      if (result.terms.size() == 0)
         return null;
      else
         return result;
   }

   /**
    * Return the subpredicate consisting of terms that apply
    * to the union of the two specified schemas, 
    * but not to either schema separately.
    * @param sch1 the first schema
    * @param sch2 the second schema
    * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
    */
   public Predicate joinSubPred(Schema sch1, Schema sch2) {
      Predicate result = new Predicate();
      Schema newsch = new Schema();
      newsch.addAll(sch1);
      newsch.addAll(sch2);
      for (Term t : terms)
         if (!t.appliesTo(sch1)  &&
               !t.appliesTo(sch2) &&
               t.appliesTo(newsch))
            result.terms.add(t);
      if (result.terms.size() == 0)
         return null;
      else
         return result;
   }

   /**
    * Determine if there is a term of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      for (Term t : terms) {
         Constant c = t.equatesWithConstant(fldname);
         if (c != null)
            return c;
      }
      return null;
   }

   /**
    * Determine if there is a term of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      for (Term t : terms) {
         String s = t.equatesWithField(fldname);
         if (s != null)
            return s;
      }
      return null;
   }

   /**
    * Returns the matched operator for the given pair of fields in the term.
    * Before this method is called, 
    *    equatesWithField() should have been called to obtain a corresponding field name.
    * Thus, we assume that there must be a matching term and operator when this method is called.
    * @param lhsFieldName the name of the LHS field used in the term
    * @param rhsFieldName the name of the RHS field used in the term
    * @return the matched operator
    * @throws RuntimeException if unable to find a matched operator or a matched term
    */
   public Operator getMatchedOperatorByTermFieldNames
      (String lhsFieldName, String rhsFieldName) {
         Term matchedTerm = getTermWithFieldThatEquates(lhsFieldName, rhsFieldName);
         if (matchedTerm == null) {
            throw new RuntimeException("Error: cannot find matching term"); 
         }

         Operator matchedOperator = matchedTerm.getOperator();
         if (matchedTerm.getLHSAsFieldName().equals(lhsFieldName) 
         && matchedTerm.getRHSAsFieldName().equals(rhsFieldName)) {
            return matchedOperator;
         } else if (matchedTerm.getLHSAsFieldName().equals(rhsFieldName) 
         && matchedTerm.getRHSAsFieldName().equals(lhsFieldName)) {
            return matchedOperator.getReverseOperator();
         } else {
            throw new RuntimeException("Error: cannot find matching operator");
         }
   }


   /**
    * Helper method.
    * Determine if there is a term 
    * where F1 and F2 are the specified fields
    * If so, the method returns this term
    * If not, the method returns null.
    * @param fieldName1 the name of the 1st field
    * @param fieldName2 the name of the 2nd field
    * @return the term, or null
    */
   private Term getTermWithFieldThatEquates(String fieldName1, String fieldName2) {
      for (Term t : terms) {
         String s = t.equatesWithField(fieldName2);
         if (s != null && s.equals(fieldName1)) {
            return t;
         } 
      }
      return null;
   }

   public String toString() {
      Iterator<Term> iter = terms.iterator();
      if (!iter.hasNext()) 
         return "";
      String result = iter.next().toString();
      while (iter.hasNext())
         result += " and " + iter.next().toString();
      return result;
   }
}
