package simpledb.experiment;

import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class RunExperiment {
    private static final String db_base_name = "db_experiment_";
    // private static final String[] num_of_rows = {"100", "1000", "5000", "10000", "15000"};
    private static final String[] num_of_rows = {"100", "1000", "5000"};
    private static String current_num_of_rows = "100";
    public static void main(String[] args) {
        // InsertSingleData();
        InsertAllData();
    }

    private static void InsertAllData() {
        for (String n : num_of_rows) {
            ExperimentUtils.InsertExperimentData(n);
            VerifyInsert(n);
        }
    }

    private static void InsertSingleData() {
        ExperimentUtils.InsertExperimentData(current_num_of_rows);
        VerifyInsert(current_num_of_rows);
    }

    // Select the count of ids for each of the tables
    private static void VerifyInsert(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name + db_child_name);
        String qry;
        qry = "select count(sid) from student";
        TestUtil.executeQuery(qry, db);
        qry = "select count(stid) from staff";
        TestUtil.executeQuery(qry, db);
        qry = "select count(cid) from course";
        TestUtil.executeQuery(qry, db);
        qry = "select count(secid) from section";
        TestUtil.executeQuery(qry, db);
        qry = "select count(eid) from enroll";
        TestUtil.executeQuery(qry, db);
    }
}
