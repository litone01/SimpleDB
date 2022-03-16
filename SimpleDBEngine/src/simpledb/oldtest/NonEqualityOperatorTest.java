package simpledb.oldtest;

import simpledb.tx.Transaction;

import java.util.LinkedHashMap;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class NonEqualityOperatorTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_nonEqualityOperatorTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
			
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
			OldTestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            // LinkedHashMap (we want to preserve insertion order) for fieldName and its corresponding type
            // In this test, we have:
            // 1. sname: STRING
            // 2. gradyear: INT
            LinkedHashMap<String, String> fieldNameAndType = new LinkedHashMap<String, String>();
            fieldNameAndType.put("sname", "STRING");
            fieldNameAndType.put("gradyear", "INT");

            // Test query on non-equality operators
            // Section A. with space between the expressions in the where clause
            // 1. = operator
            String qry = "select sname, gradyear from student where majorid = 10";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            
            // 2. > operator
            qry = "select sname, gradyear from student where majorid > 10";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 3. <= operator
            qry = "select sname, gradyear from student where majorid <= 50";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 4. >= operator
            qry = "select sname, gradyear from student where majorid >= 50";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 5. < operator
            qry = "select sname, gradyear from student where majorid < 50";
			OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 6. > operator
            qry = "select sname, gradyear from student where majorid > 10";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 7. mixed operators and multiple predicate
            qry = "select sname, gradyear from student where majorid != 10 and gradyear < 2020";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 8. mixed operators and multiple tables
            qry = "select sname, gradyear from student, dept where did = majorid and dname != 'math'";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Section B. with no space between the expressions in the where clause
            // 1. = operator
            qry = "select sname, gradyear from student where majorid=10";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 2. > operator
            qry = "select sname, gradyear from student where majorid>10";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 3. <= operator
            qry = "select sname, gradyear from student where majorid<=50";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 4. <> operator
            qry = "select sname, gradyear from student where majorid<>10";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 5. mixed operators and multiple predicate
            qry = "select sname, gradyear from student where majorid!=10 and gradyear<2020";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // 6. mixed operators and multiple tables
            qry = "select sname, gradyear from student, dept where did=majorid and dname!='math'";
            OldTestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
