package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class OrderByClauseTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_orderByClauseTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
			
			TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            String qry;
            // Test query on order by clause
            // Single order by clause ascending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid asc";
            TestUtil.executeSelectQuery(qry, tx, planner);
            
            // Single order by clause descending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // Multiple order by clause descending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid desc, gradyear desc";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // Multiple order by clause ascending
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by majorid asc, gradyear asc";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // Multiple order by clause mixed
            qry = "select sname, gradyear, majorid from student where majorid > 0 order by gradyear asc, majorid desc";
			TestUtil.executeSelectQuery(qry, tx, planner);

            // Multiple order by clause mixed, no where clause
            qry = "select sname, gradyear, majorid from student order by gradyear asc, majorid desc";
			TestUtil.executeSelectQuery(qry, tx, planner);

            // no where clause, no order by clause
            qry = "select sname, gradyear, majorid from student";
			TestUtil.executeSelectQuery(qry, tx, planner);

            // no order by clause
            qry = "select sname, gradyear, majorid from student where majorid > 0";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // Multiple table
            // Single order by clause ascending, multiple table
            qry = "select sname, gradyear, majorid from student, dept where did = majorid order by majorid asc";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // Multiple order by clause mixed, multiple table
            qry = "select sname, gradyear, majorid from student, dept where did = majorid order by majorid asc, gradyear desc";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // // Multiple order by clause mixed, multiple table, no where clause
            // qry = "select sname, gradyear, majorid from student, dept order by majorid asc, gradyear desc";
            // TestUtil.executeSelectQuery(qry, tx, planner);

            // // multiple table, no where clause and no order by clause
            // qry = "select sname, gradyear, majorid from student, dept";
            // TestUtil.executeSelectQuery(qry, tx, planner);

            // multiple table, no order by clause
            qry = "select sname, gradyear, majorid from student, dept where did = majorid";
            TestUtil.executeSelectQuery(qry, tx, planner);

			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
