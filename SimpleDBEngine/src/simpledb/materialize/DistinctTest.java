package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class DistinctTest {
    public static void main(String[] args) {
        try {
            // analogous to the driver
            SimpleDB db = new SimpleDB("studentdb");

            // analogous to the connection
            Transaction tx  = db.newTx();
            Planner planner = db.planner();

            // analogous to the statement
            String qry = "select distinct majorid, gradyear from student where gradyear < 2022 and majorid = 10 order by gradyear, majorid";
            Plan p = planner.createQueryPlan(qry, tx);

            // analogous to the result set
            Scan s = p.open();

            while (s.next()) {
                int majorid = s.getInt("majorid");
                int gradyear = s.getInt("gradyear");//in lower case

                System.out.println(majorid + "\t" + gradyear + "\t");
            }
            s.close();
            tx.commit();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
