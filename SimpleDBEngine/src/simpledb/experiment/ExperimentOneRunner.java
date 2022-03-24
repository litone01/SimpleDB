package simpledb.experiment;

import java.util.ArrayList;

import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class ExperimentOneRunner implements Runnable {
    private SimpleDB db;
    private String db_child_name;

    public ExperimentOneRunner(SimpleDB db, String db_child_name) {
        this.db = db;
        this.db_child_name = db_child_name;
    }

    private void ExecuteExperimentOne(SimpleDB db, String db_child_name) {
        String qry;
        qry = "select sid, sname, eid, studentid from student, enroll where sid = studentid";
        // Default to run the query 3 times and take average
        int n = 3;
        ArrayList<Double> times = new ArrayList<Double>();
        for (int i = 0; i < n; i++) {
            long startTime = System.currentTimeMillis();
            TestUtil.executeQueryExperiment(qry, db);
            long endTime = System.currentTimeMillis();
            Double duration = (endTime - startTime) / 1000.0;
            times.add(duration);
        }
        Double avgTime = getAvarageTime(times);
        System.out.println("# of rows: "+ db_child_name + ", Query: " + qry );
        System.out.println("Average time: " + avgTime + " s");
        for (int i = 0; i < times.size(); i++) {
            System.out.println("Run#" + i + " Time: " + times.get(i) + " s");
        } 
    }

    private Double getAvarageTime(ArrayList<Double> times) {
        Double sum = 0.0;
        for (Double t : times) {
            sum += t;
        }
        return sum / times.size();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            ExecuteExperimentOne(db, db_child_name);
            // exit current thread
            break;
        }
    }
}
