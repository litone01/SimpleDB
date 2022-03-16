package simpledb.oldtest;

import simpledb.tx.Transaction;

import java.util.LinkedHashMap;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class OrderByClauseTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_orderByClauseTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
            
			// NOTE: COMMENT OUT this once the first one! Dont create the tables again!
			OldTestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            String qry;
            // LinkedHashMap (we want to preserve insertion order) for fieldName and its corresponding type
            // In this test, we have:
            // 1. sname: STRING
            // 2. gradyear: INT
            // 3. majorid: INT
            LinkedHashMap<String, String> fieldNameAndType = new LinkedHashMap<String, String>();
            fieldNameAndType.put("sname", "STRING");
            fieldNameAndType.put("gradyear", "INT");
            fieldNameAndType.put("majorid", "INT");

            // Test query on order by clause
            // When order by type is not specified.
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid ";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid, gradyear";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid , gradyear ";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid , gradyear desc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear ";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear ;";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            qry = "select sname, gradyear, majorid from student order by majorid desc, gradyear ;";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Single order by clause ascending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid asc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            
            // Single order by clause descending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Multiple order by clause descending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear desc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Multiple order by clause ascending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid asc, gradyear asc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Multiple order by clause mixed
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by gradyear asc, majorid desc";
			OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Multiple order by clause mixed, no where clause
            qry = "select sname, gradyear, majorid from student order by gradyear asc, majorid desc";
			OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // no where clause, no order by clause
            qry = "select sname, gradyear, majorid from student";
			OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // no order by clause
            qry = "select sname, gradyear, majorid from student where majorid > 0";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Multiple table
            // Single order by clause ascending, multiple table
            qry = "select sname, gradyear, majorid from student, dept where did = majorid order by majorid asc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Multiple order by clause mixed, multiple table
            qry = "select sname, gradyear, majorid from student, dept where did = majorid order by majorid asc, gradyear desc";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // // Multiple order by clause mixed, multiple table, no where clause
            // qry = "select sname, gradyear, majorid from student, dept order by majorid asc, gradyear desc";
            // TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // // multiple table, no where clause and no order by clause
            // qry = "select sname, gradyear, majorid from student, dept";
            // TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // multiple table, no order by clause
            qry = "select sname, gradyear, majorid from student, dept where did = majorid";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}