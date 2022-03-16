package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class AggregationTest {
    public static void main(String[] args) {
        try {
            // analogous to the driver
            SimpleDB db = new SimpleDB("db_studentdb");

            // analogous to the connection
            Transaction tx  = db.newTx();
            Planner planner = db.planner();

            // analogous to the statement
            String qry = "select majorid, count(sid), max(gradyear), min(gradyear) from student group by majorid";
            Plan p = planner.createQueryPlan(qry, tx);

            // analogous to the result set
            Scan s = p.open();

//            System.out.println("Name\tMajor");
            while (s.next()) {
                int countofsid = s.getInt("countofsid"); //SimpleDB stores field names
                int majorid = s.getInt("majorid"); //in lower case
                int maxofgradyear = s.getInt("maxofgradyear");
                int minofgradyear = s.getInt("minofgradyear");

                System.out.println(majorid + "\t" + countofsid + "\t" + maxofgradyear + "\t" + minofgradyear);
            }
            s.close();
            tx.commit();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
