package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class NonEqualityOperatorTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_nonEqualityOperatorTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
			
			TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            // Test query on non-equality operators
            // Section A. with space between the expressions in the where clause
            // 1. = operator
            String qry = "select sname, gradyear from student where majorid = 10";
            TestUtil.executeSelectQuery(qry, tx, planner);
            
            // 2. > operator
            qry = "select sname, gradyear from student where majorid > 10";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 3. <= operator
            qry = "select sname, gradyear from student where majorid <= 50";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 4. >= operator
            qry = "select sname, gradyear from student where majorid >= 50";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 5. < operator
            qry = "select sname, gradyear from student where majorid < 50";
			TestUtil.executeSelectQuery(qry, tx, planner);

            // 6. > operator
            qry = "select sname, gradyear from student where majorid > 10";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 7. mixed operators and multiple predicate
            qry = "select sname, gradyear from student where majorid != 10 and gradyear < 2020";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 8. mixed operators and multiple tables
            qry = "select sname, gradyear from student, dept where did = majorid and dname != 'math'";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // Section B. with no space between the expressions in the where clause
            // 1. = operator
            qry = "select sname, gradyear from student where majorid=10";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 2. > operator
            qry = "select sname, gradyear from student where majorid>10";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 3. <= operator
            qry = "select sname, gradyear from student where majorid<=50";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 4. <> operator
            qry = "select sname, gradyear from student where majorid<>10";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 5. mixed operators and multiple predicate
            qry = "select sname, gradyear from student where majorid!=10 and gradyear<2020";
            TestUtil.executeSelectQuery(qry, tx, planner);

            // 6. mixed operators and multiple tables
            qry = "select sname, gradyear from student, dept where did=majorid and dname!='math'";
            TestUtil.executeSelectQuery(qry, tx, planner);

			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
