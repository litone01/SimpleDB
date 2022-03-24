package simpledb.experiment;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class RunExperiment {
    private static final String db_base_name = "db_experiment_";
    private static final String db_base_name_experiement_two = "db_experiment_2_";
    // private static final String[] num_of_rows_to_test = {"100", "1000", "5000", "10000", "15000"};
    // private static final String[] num_of_rows_to_test = {"10000", "15000"};
    private static final String[] num_of_rows_to_test = {"10000"};
    // private static final String[] index_types = {"hash", "btree"};
    private static final String[] index_types = {"btree"};
    private static final String output_base_path = "C:\\Users\\Jiaxiang\\git\\cs3223-project\\output";
    private static final int MAX_TIMEOUT = 10 * 60 * 1000; // 10 minutes

    public static void main(String[] args) throws FileNotFoundException {
        
        // Test with index
        for (String index_type : index_types) {
            for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
                PrintStream fileStream = new PrintStream(output_base_path+"\\"+curr_num_of_rows_to_test+"_"+ index_type +"_output.txt");
                System.setOut(fileStream);
                // InsertSingleDataWithIndex(index_type, curr_num_of_rows_to_test);
                RunExperimentOne(curr_num_of_rows_to_test, index_type);
            }
        }

        // // Test with index and timeout
        // for (String index_type : index_types) {
        //     for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
        //         PrintStream fileStream = new PrintStream(output_base_path+"\\"+curr_num_of_rows_to_test+"_"+ index_type +"_output.txt");
        //         System.setOut(fileStream);
        //         // InsertSingleDataWithIndex(index_type, curr_num_of_rows_to_test);
        //         RunExperimentOneWithTimeout(curr_num_of_rows_to_test, index_type);
        //     }
        // }

        // // Test without Index, Experiement 2
        // for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
        //     PrintStream fileStream = new PrintStream(output_base_path+"\\"+curr_num_of_rows_to_test+"_output.txt");
        //     System.setOut(fileStream);
        //     InsertSingleData(curr_num_of_rows_to_test);
        //     RunExperiementTwo(curr_num_of_rows_to_test);
        // }

        // // Test with index, Experiement 2
        // for (String index_type : index_types) {
        //     for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
        //         PrintStream fileStream = new PrintStream(output_base_path+"\\"+curr_num_of_rows_to_test+"_"+ index_type +"_output.txt");
        //         System.setOut(fileStream);
        //         InsertSingleDataWithIndexExperimentTwo(index_type, curr_num_of_rows_to_test);
        //         RunExperimentTwo(curr_num_of_rows_to_test, index_type);
        //     }
        // }

    }

    private static void Insert(String index_type) {
        // without Index
        // InsertAllData();
        // InsertSingleData();

        // with Index
        // InsertAllDataWithIndex(index_type);
        // InsertSingleDataWithIndex(index_type);
    }

    private static void InsertAllData() {
        for (String n : num_of_rows_to_test) {
            ExperimentUtils.InsertExperimentData(n);
            VerifyInsert(n);
        }
    }

    private static void InsertSingleData(String current_num_of_rows) {
        ExperimentUtils.InsertExperimentData(current_num_of_rows);
        VerifyInsert(current_num_of_rows);
    }

    private static void InsertAllDataWithIndex(String index_type) {
        for (String n : num_of_rows_to_test) {
            ExperimentUtils.InsertExperimentDataWithIndex(n, index_type);
            VerifyInsertWithIndex("index_" + index_type + "_" + n);
        }
    }

    private static void InsertSingleDataWithIndex(String index_type, String current_num_of_rows) {
        ExperimentUtils.InsertExperimentDataWithIndex(current_num_of_rows, index_type);
        VerifyInsertWithIndex("index_" + index_type + "_" + current_num_of_rows);
    }

    private static void InsertSingleDataWithIndexExperimentTwo(String index_type, String current_num_of_rows) {
        ExperimentUtils.InsertExperimentTwoDataWithIndex(current_num_of_rows, index_type);
        VerifyInsertWithIndexExperiementTwo("index_" + index_type + "_" + current_num_of_rows);
    }

    // Select the count of ids for each of the tables
    private static void VerifyInsert(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name + db_child_name);
        ExecuteVerifyInsert(db);
    }

     // Select the count of ids for each of the tables
    private static void VerifyInsertWithIndex(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name + db_child_name);
        ExecuteVerifyInsert(db);
    }

    private static void VerifyInsertWithIndexExperiementTwo(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name_experiement_two + db_child_name);
        ExecuteVerifyInsert(db);
    }

    private static void ExecuteVerifyInsert(SimpleDB db) {
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

    private static void RunExperimentOne(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name + db_child_name);
        ExecuteExperimentOne(db, db_child_name);
    }

    private static void RunExperimentOne(String db_child_name, String index_type) {
        SimpleDB db = new SimpleDB(db_base_name + "index_" + index_type + "_" + db_child_name);
        ExecuteExperimentOne(db, db_child_name);
    }

    private static void ExecuteExperimentOne(SimpleDB db, String db_child_name) {
        String qry;
        qry = "select sid, sname, eid, studentid from enroll, student where sid = studentid";
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

    private static Double getAvarageTime(ArrayList<Double> times) {
        Double sum = 0.0;
        for (Double t : times) {
            sum += t;
        }
        return sum / times.size();
    }

    private static void RunExperimentOneWithTimeout(String db_child_name, String index_type) {
        SimpleDB db = new SimpleDB(db_base_name + "index_" + index_type + "_" + db_child_name);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future future = executor.submit(new ExperimentOneRunner(db, db_child_name));
        try {
            future.get(MAX_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            System.out.println("Timeout: " + db_base_name + "index_" + index_type + "_" + db_child_name);
            future.cancel(true);
        } catch (InterruptedException e) {
            System.out.println("InterruptedException: " + db_base_name + "index_" + index_type + "_" + db_child_name);
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.out.println("ExecutionException: " + db_base_name + "index_" + index_type + "_" + db_child_name);
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }
    }


    private static void RunExperiementTwo(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name + db_child_name);
        ExecuteExperimentTwo(db, db_child_name);
    }

    private static void RunExperimentTwo(String db_child_name, String index_type) {
        SimpleDB db = new SimpleDB(db_base_name_experiement_two + "index_" + index_type + "_" + db_child_name);
        ExecuteExperimentTwo(db, db_child_name);
    }

    private static void ExecuteExperimentTwo(SimpleDB db, String db_child_name) {
        String qry;
        qry = "select sid, studentid, sectionid, secid, staffid, stid from student, enroll, section, staff where sid = studentid and sectionid = secid and staffid = stid";
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

    private static void ExecuteExperimentWithJDBC(String db_child_name, String index_type) {
        String url = "jdbc:simpledb:"  + db_base_name + "index_" + index_type + "_" + db_child_name;
        String qry;
        qry = "select sid, sname, eid, studentid from enroll, student where sid = studentid";
        // Default to run the query 3 times and take average
        int n = 1;
        ArrayList<Double> times = new ArrayList<Double>();
        for (int j = 0; j < n; j++) {
            long startTime = System.currentTimeMillis();
            ExecuteWithJDBC(url, qry);
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

    private static void ExecuteWithJDBC(String url, String qry) {
        Driver d = new EmbeddedDriver();
        try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(qry)) {

            ResultSetMetaData md = rs.getMetaData();
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
            int count = 0;
            // print records
            while(rs.next()) {
                count++;
                for (int i=1; i<=numcols; i++) {
                    String fldname = md.getColumnName(i);
                    int fldtype = md.getColumnType(i);
                    String fmt = "%" + md.getColumnDisplaySize(i);
                    if (fldtype == Types.INTEGER) {
                        int ival = rs.getInt(fldname);
                        System.out.format(fmt + "d", ival);
                    }
                    else {
                        String sval = rs.getString(fldname);
                        System.out.format(fmt + "s", sval);
                    }
                }
                System.out.print(" " + count);
                System.out.println();
            }
            
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
