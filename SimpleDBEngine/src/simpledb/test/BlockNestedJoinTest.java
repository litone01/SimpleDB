package simpledb.test;

import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry; 
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class BlockNestedJoinTest {
    public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_BNLJoinTest");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
			
			// TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            String[][] columns1 = {{"sname", "string"}, {"majorid", "int"}, {"did", "int"}};

            String qry = "select sname, majorid, did from student, dept where majorid = did";
            TestUtil.executeCustomisedSelectQuery(qry, tx, planner, columns1);
            // expected:
            // sname   majorid did     
            // joe     10      10
            // max     10      10
            // lee     10      10
            // amy     20      20
            // sue     20      20
            // kim     20      20
            // pat     20      20
            // bob     30      30
            // art     30      30
            
            String[][] columns2 = {{"sid", "int"}, {"sname", "string"}, {"majorid", "int"}, {"did", "int"}, {"deptid", "int"}};
            qry = "select sid, sname, majorid, did, deptid from student, dept, course where majorid = did and did = DeptId";
            TestUtil.executeCustomisedSelectQuery(qry, tx, planner, columns2);
            // expected:
            // sid     sname   majorid did     deptid  
            // 1       joe     10      10      10
            // 1       joe     10      10      10
            // 3       max     10      10      10
            // 3       max     10      10      10
            // 9       lee     10      10      10
            // 9       lee     10      10      10
            // 2       amy     20      20      20
            // 2       amy     20      20      20      
            // 4       sue     20      20      20
            // 4       sue     20      20      20
            // 6       kim     20      20      20
            // 6       kim     20      20      20
            // 8       pat     20      20      20
            // 8       pat     20      20      20
            // 5       bob     30      30      30
            // 5       bob     30      30      30
            // 7       art     30      30      30
            // 7       art     30      30      30

			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
