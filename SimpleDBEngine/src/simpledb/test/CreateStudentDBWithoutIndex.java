package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class CreateStudentDBWithoutIndex {
	public static void main(String[] args) {
		try {
			SimpleDB db = new SimpleDB("db_studentdb");
			Transaction tx  = db.newTx();
			Planner planner = db.planner();
			
			TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
