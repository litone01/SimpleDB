package simpledb.test;

import simpledb.tx.Transaction;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class OrderByClauseTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_orderByClauseTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
            
			// NOTE: COMMENT OUT this once the first one! Dont create the tables again!
			TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            String qry;

            // Test query on order by clause
            // When order by type is not specified.
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid ";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid, gradyear";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid , gradyear ";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid , gradyear desc";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear ";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear ;";
            TestUtil.executeQuery(qry, db);

            qry = "select sname, gradyear, majorid from student order by majorid desc, gradyear ;";
            TestUtil.executeQuery(qry, db);

            // Single order by clause ascending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid asc";
            TestUtil.executeQuery(qry, db);
            
            // Single order by clause descending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc";
            TestUtil.executeQuery(qry, db);

            // Multiple order by clause descending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear desc";
            TestUtil.executeQuery(qry, db);

            // Multiple order by clause ascending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid asc, gradyear asc";
            TestUtil.executeQuery(qry, db);

            // Multiple order by clause mixed
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by gradyear asc, majorid desc";
			TestUtil.executeQuery(qry, db);

            // Multiple order by clause mixed, no where clause
            qry = "select sname, gradyear, majorid from student order by gradyear asc, majorid desc";
			TestUtil.executeQuery(qry, db);

            // no where clause, no order by clause
            qry = "select sname, gradyear, majorid from student";
			TestUtil.executeQuery(qry, db);

            // no order by clause
            qry = "select sname, gradyear, majorid from student where majorid > 0";
            TestUtil.executeQuery(qry, db);

            // Multiple table
            // Single order by clause ascending, multiple table
            qry = "select sname, gradyear, majorid from student, dept where did = majorid order by majorid asc";
            TestUtil.executeQuery(qry, db);

            // Multiple order by clause mixed, multiple table
            qry = "select sname, gradyear, majorid from student, dept where did = majorid order by majorid asc, gradyear desc";
            TestUtil.executeQuery(qry, db);

            // // Multiple order by clause mixed, multiple table, no where clause
            // qry = "select sname, gradyear, majorid from student, dept order by majorid asc, gradyear desc";
            // TestUtil.executeQuery(qry, db);

            // // multiple table, no where clause and no order by clause
            // qry = "select sname, gradyear, majorid from student, dept";
            // TestUtil.executeQuery(qry, db);

            // multiple table, no order by clause
            qry = "select sname, gradyear, majorid from student, dept where did = majorid";
            TestUtil.executeQuery(qry, db);

		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
