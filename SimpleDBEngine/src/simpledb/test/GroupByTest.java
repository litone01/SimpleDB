package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class GroupByTest {
    public static void main(String[] args) {
        try {
            SimpleDB db = new SimpleDB("db_groupbyTest");
            Transaction tx  = db.newTx();
            Planner planner = db.planner();
            
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
            // Also, check if the join method has been set the correct join algorithm
            TestUtil.createSampleStudentDBWithoutIndex(planner, tx);

            String qry;
            qry = "select sid from student group by sid";
            TestUtil.executeQuery(qry, db);

            qry = "select count(sid) from student";
            TestUtil.executeQuery(qry, db);

            qry = "select sname from student group by sname";
            TestUtil.executeQuery(qry, db);

            qry = "select sid from student group by sid, sname";
            TestUtil.executeQuery(qry, db);

            qry = "select sname from student group by sid, sname";
            TestUtil.executeQuery(qry, db);

            qry = "select sid, sname from student group by sid, sname";
            TestUtil.executeQuery(qry, db);

            qry = "select majorid, count(gradyear), max(gradyear), min(gradyear), avg(gradyear), sum(gradyear) from student where sid > 1 group by majorid";
            TestUtil.executeQuery(qry, db);

            qry = "select count(sid), max(gradyear), min(gradyear), avg(gradyear), sum(gradyear) from student group by majorid";
            TestUtil.executeQuery(qry, db);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
