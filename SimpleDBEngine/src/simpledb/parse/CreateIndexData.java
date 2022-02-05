package simpledb.parse;

/**
 * The parser for the <i>create index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {
   private String idxname, tblname, fldname, indexType;
   
   /**
    * Saves the table and field names of the specified index. IndexType is defaulted to hash when "... using indexType" is not provided
    */
   public CreateIndexData(String idxname, String tblname, String fldname) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.indexType = "hash";
   }

   /**
    * Saves the table, field names, and indexType of the specified index.
    */
    public CreateIndexData(String idxname, String tblname, String fldname, String indexType) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.indexType = indexType;
   }
   
   /**
    * Returns the name of the index.
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }

   /**
    * Returns the type of the indexed column, either hash or btree.
    * @return the type of the indexed column
    */
    public String indexType() {
      return indexType;
   }
}

