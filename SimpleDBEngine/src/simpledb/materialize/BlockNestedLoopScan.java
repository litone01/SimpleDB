package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class BlockNestedLoopScan implements Scan {
   private Scan s1, s2;
   private String fldname1, fldname2;

   /**
    * Create a block nested loop scan.
    * 
    * @param s1 the LHS scan in chunk
    * @param s2 the RHS scan
    */
   public BlockNestedLoopScan(Scan s1, Scan s2, 
         String fldname1, String fldname2) {
      this.s1 = s1;
      this.s2 = s2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      beforeFirst();
   }

   /**
    * Position the scan before its first record.
    * In particular, the LHS scan is positioned at
    * its first record, and the RHS scan
    * is positioned before its first record.
    * 
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s1.beforeFirst();
      s2.beforeFirst();
      s2.next();
   }

   /**
    * Move the scan to the next record. Assume LHS record is in buffer (chunked)
    * The method moves to the next LHS record, if possible. And keep moving until
    * there is the first match
    * Otherwise, it moves to the next RHS record and the first LHS record.
    * If there are no more RHS records, the method returns false.
    * 
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      Constant currS2 = s2.getVal(fldname2);

      while (s1.next()) {
         if (s1.getVal(fldname1).equals(currS2)) {
            return true;
         }
      }

      boolean hasMoreS2 = s2.next();
      if (!hasMoreS2) {
         return false;
      }
      s1.beforeFirst();
      return next();
   }

   /**
    * Return the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * 
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      if (s1.hasField(fldname))
         return s1.getInt(fldname);
      else
         return s2.getInt(fldname);
   }

   /**
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * 
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      if (s1.hasField(fldname))
         return s1.getString(fldname);
      else
         return s2.getString(fldname);
   }

   /**
    * Return the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (s1.hasField(fldname))
         return s1.getVal(fldname);
      else
         return s2.getVal(fldname);
   }

   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * 
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname) || s2.hasField(fldname);
   }

   /**
    * Close both underlying scans.
    * 
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
      s2.close();
   }
}
