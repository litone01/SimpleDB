package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class NonEqualityOperatorTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_nonEqualityOperatorTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
			
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
			TestUtil.createSampleStudentDBWithoutIndex(planner, tx);

            // Test query on non-equality operators
            // Section A. with space between the expressions in the where clause
            // 1. = operator
            String qry = "select sname, gradyear from student where majorid = 10";
            TestUtil.executeQuery(qry, db);
            
            // 2. > operator
            qry = "select sname, gradyear from student where majorid > 10";
            TestUtil.executeQuery(qry, db);

            // 3. <= operator
            qry = "select sname, gradyear from student where majorid <= 50";
            TestUtil.executeQuery(qry, db);

            // 4. >= operator
            qry = "select sname, gradyear from student where majorid >= 50";
            TestUtil.executeQuery(qry, db);

            // 5. < operator
            qry = "select sname, gradyear from student where majorid < 50";
			TestUtil.executeQuery(qry, db);

            // 6. > operator
            qry = "select sname, gradyear from student where majorid > 10";
            TestUtil.executeQuery(qry, db);

            // 7. mixed operators and multiple predicate
            qry = "select sname, gradyear from student where majorid != 10 and gradyear < 2020";
            TestUtil.executeQuery(qry, db);

            // 8. mixed operators and multiple tables
            qry = "select sname, gradyear from student, dept where did = majorid and dname != 'math'";
            TestUtil.executeQuery(qry, db);

            // Section B. with no space between the expressions in the where clause
            // 1. = operator
            qry = "select sname, gradyear from student where majorid=10";
            TestUtil.executeQuery(qry, db);

            // 2. > operator
            qry = "select sname, gradyear from student where majorid>10";
            TestUtil.executeQuery(qry, db);

            // 3. <= operator
            qry = "select sname, gradyear from student where majorid<=50";
            TestUtil.executeQuery(qry, db);

            // 4. <> operator
            qry = "select sname, gradyear from student where majorid<>10";
            TestUtil.executeQuery(qry, db);

            // 5. mixed operators and multiple predicate
            qry = "select sname, gradyear from student where majorid!=10 and gradyear<2020";
            TestUtil.executeQuery(qry, db);

            // 6. mixed operators and multiple tables
            qry = "select sname, gradyear from student, dept where did=majorid and dname!='math'";
            TestUtil.executeQuery(qry, db);

		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
