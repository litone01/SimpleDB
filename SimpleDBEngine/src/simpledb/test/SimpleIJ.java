package simpledb.test;

import java.sql.*;
import java.util.Scanner;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.record.Schema;
import simpledb.jdbc.embedded.EmbeddedMetaData;
public class SimpleIJ {
   public static void main(String[] args) {
      Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
      String s = sc.nextLine();
      // Driver d = (s.contains("//")) ? new NetworkDriver() : new EmbeddedDriver();      

      try {
         SimpleDB db = new SimpleDB(s);
         Transaction tx  = db.newTx();
         Planner planner = db.planner();

         System.out.print("\nSQL> ");
         while (sc.hasNextLine()) {
            // process one line of input
            String cmd = sc.nextLine().trim();
            if (cmd.startsWith("exit")) {
               break;
            }
            else if (cmd.startsWith("select")) {
               doQuery(tx, planner, cmd);
               
            } else {
               doUpdate(tx, planner, cmd);
               
            }
            System.out.print("\nSQL> ");
         }
      }
      catch (Exception e) {
         e.printStackTrace();
      }
      sc.close();
   }

   private static void doQuery(Transaction tx, Planner planner, String cmd) {
      try {
         Plan p = planner.createQueryPlan(cmd, tx);
         Scan s = p.open();
         Schema sch = p.schema();
         ResultSetMetaData md = new EmbeddedMetaData(sch);
         int numcols = md.getColumnCount();
         int totalwidth = 0;

         // print header
         for(int i=1; i<=numcols; i++) {
            String fldname = md.getColumnName(i);
            int width = md.getColumnDisplaySize(i);
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for(int i=0; i<totalwidth; i++)
            System.out.print("-");
         System.out.println();

         // print records
         while(s.next()) {
            for (int i=1; i<=numcols; i++) {
               String fldname = md.getColumnName(i);
               int fldtype = md.getColumnType(i);
               String fmt = "%" + md.getColumnDisplaySize(i);
               if (fldtype == Types.INTEGER) {
                  int ival = s.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               }
               else {
                  String sval = s.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
         s.close();
         tx.commit();
      }
      catch (Exception e) {
         e.printStackTrace();      
      }
   }

   private static void doUpdate(Transaction tx, Planner planner, String cmd) {
      try {
         int howmany = planner.executeUpdate(cmd, tx);
         System.out.println(howmany + " records processed");
         tx.commit();
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }
}