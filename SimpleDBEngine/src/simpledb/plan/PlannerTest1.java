package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

public class PlannerTest1 {
   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("db_plannertest1");
      Transaction tx = db.newTx();
      Planner planner = db.planner();
      String cmd = "create table T1(A int, B varchar(9))";
      planner.executeUpdate(cmd, tx);

      int n = 200;
      System.out.println("Inserting " + n + " random records.");
      for (int i=0; i<n; i++) {
         int a = (int) Math.round(Math.random() * 50);
         String b = "rec" + a;
         cmd = "insert into T1(A,B) values(" + a + ", '" + b + "')";
         planner.executeUpdate(cmd, tx);
      }

      // does not work
      String qry = "select B from T1 where A>10 order by B desc";
      // String qry = "select B from T1 where A > 10";
      Plan p = planner.createQueryPlan(qry, tx);
      Scan s = p.open();
      while (s.next())
         System.out.println(s.getString("b")); 
      s.close();
      tx.commit();
   }
}

