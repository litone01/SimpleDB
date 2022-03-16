package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class DistinctTest {
    public static void main(String[] args) {
        try {
            SimpleDB db = new SimpleDB("db_distinctTest");
            Transaction tx  = db.newTx();
            Planner planner = db.planner();
            
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
            // Also, check if the join method has been set the correct join algorithm
            TestUtil.createSampleStudentDBWithoutIndex(planner, tx);

            String qry;
            qry = "select distinct majorid, gradyear from student where gradyear < 2022 and majorid = 10 order by gradyear, majorid";
            TestUtil.executeQuery(qry, db);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
