package simpledb.materialize;

import simpledb.index.Index;
import simpledb.index.planner.IndexJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.Map;

public class NestedLoopJoinTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();

        // Find the index on StudentId.
        Map<String, IndexInfo> indexes = mdm.getIndexInfo("enroll", tx);
        IndexInfo sidIdx = indexes.get("studentid");

        // Get plans for the Student and Enroll tables
        Plan studentplan = new TablePlan(tx, "student", mdm);
        Plan enrollplan = new TablePlan(tx, "enroll", mdm);

        // Two different ways to use the index in simpledb:
//        useIndexManually(studentplan, enrollplan, sidIdx, "sid");
//        useIndexScan(studentplan, enrollplan, sidIdx, "sid");
        useNestedLoopJoinScan(studentplan, enrollplan, "sid", "studentid", tx);

        tx.commit();
    }


    private static void useNestedLoopJoinScan(Plan p1, Plan p2, String joinfiled1, String joinfield2, Transaction tx) {
        // Open an index join scan on the table.
        Plan nestedLoopJoinPlan = new NestedLoopJoinPlan(tx,p1,p2,joinfiled1, joinfield2);
        Scan s = nestedLoopJoinPlan.open();

        while (s.next()) {
            System.out.println(s.getInt("sid"));
        }
        s.close();
    }
}
