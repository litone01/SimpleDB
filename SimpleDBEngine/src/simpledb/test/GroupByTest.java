package simpledb.test;
import simpledb.tx.Transaction;

import java.util.LinkedHashMap;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;
public class GroupByTest {
    public static void main(String[] args) {
        try {
            SimpleDB db = new SimpleDB("db_groupbyTest");
            Transaction tx  = db.newTx();
            Planner planner = db.planner();
            
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
            // Also, check if the join method has been set the correct join algorithm
            TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            // LinkedHashMap (we want to preserve insertion order) for fieldName and its corresponding type
            // In this test, we have:
            // 1. sid: INT
            // 2. sname: STRING
            LinkedHashMap<String, String> sid = new LinkedHashMap<String, String>();
            sid.put("sid", "INT");
            LinkedHashMap<String, String> sname = new LinkedHashMap<String, String>();
            sname.put("sname", "STRING");
            LinkedHashMap<String, String> sidAndSname = new LinkedHashMap<String, String>();
            sidAndSname.put("sid", "INT");
            sidAndSname.put("sname", "STRING");

            String qry;
            qry = "select sid from student, dept group by sid";
            TestUtil.executeSelectQuery(qry, tx, planner, sid);

            qry = "select sname from student, dept group by sname";
            TestUtil.executeSelectQuery(qry, tx, planner, sname);

            qry = "select sid from student, dept group by sid, sname";
            TestUtil.executeSelectQuery(qry, tx, planner, sid);

            qry = "select sname from student, dept group by sid, sname";
            TestUtil.executeSelectQuery(qry, tx, planner, sname);

            qry = "select sid, sname from student, dept group by sid, sname";
            TestUtil.executeSelectQuery(qry, tx, planner, sidAndSname);

            tx.commit();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
