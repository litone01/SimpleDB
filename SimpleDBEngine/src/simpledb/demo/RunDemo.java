package simpledb.demo;

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

public class RunDemo {
    private static final String db_base_name = "db_demo_";
    private static final String db_base_name_demo_two = "db_demo_2_";
    private static final String[] num_of_rows_to_test = {"50"};
    private static final String[] index_types = {"hash", "btree"};
    // private static final String[] index_types = {"btree"};

    public static void main(String[] args) throws FileNotFoundException {
        // 1. Insert Demo data for single table
        // db_demo_index_hash_50 will be created
        // db_demo_index_btree_50 will be created
        for (String index_type : index_types) {
            InsertDataSingleTable(index_type);
        }

        // 2. Insert Demo data for multi table join
        // db_demo_2_index_hash_50 will be created
        // db_demo_2_index_btree_50 will be created
        for (String index_type : index_types) {
            InsertDataMultiTable(index_type);
        }
    }

    private static void InsertDataSingleTable(String index_type) {
        for (String n : num_of_rows_to_test) {
            DemoUtils.InsertDemoDataSingleTable(n, index_type);
            VerifyInsertSingleTable("index_" + index_type + "_" + n);
        }
    }

    private static void InsertDataMultiTable(String index_type) {
        for (String n : num_of_rows_to_test) {
            DemoUtils.InsertDemoDataMultiTable(n, index_type);
            VerifyInsertMultiTable("index_" + index_type + "_" + n);
        }
    }

     // Select the count of ids for each of the tables
    private static void VerifyInsertSingleTable(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name + db_child_name);
        ExecuteVerifyInsert(db);
    }

    private static void VerifyInsertMultiTable(String db_child_name) {
        SimpleDB db = new SimpleDB(db_base_name_demo_two + db_child_name);
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


    // Backup methods on running demo
    // public static void main(String[] args) throws FileNotFoundException {
        // Test with index
        // for (String index_type : index_types) {
        //     for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
        //         // InsertSingleDataWithIndex(index_type, curr_num_of_rows_to_test);
        //         RunDemoOneWithIndex(curr_num_of_rows_to_test, index_type);
        //     }
        // }

        // // Test without Index, Experiement 2
        // for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
        //     InsertSingleData(curr_num_of_rows_to_test);
        //     RunDemoTwo(curr_num_of_rows_to_test);
        // }

        // // Test with index, Experiement 2
        // for (String index_type : index_types) {
        //     for (String curr_num_of_rows_to_test : num_of_rows_to_test) {
        //         InsertSingleDataWithIndexDemoTwo(index_type, curr_num_of_rows_to_test);
        //         RunDemoTwoWithIndex(curr_num_of_rows_to_test, index_type);
        //     }
        // }
    // }

    // private static void RunDemoOne(String db_child_name) {
    //     SimpleDB db = new SimpleDB(db_base_name + db_child_name);
    //     ExecuteDemoOne(db, db_child_name);
    // }

    // private static void RunDemoOneWithIndex(String db_child_name, String index_type) {
    //     SimpleDB db = new SimpleDB(db_base_name + "index_" + index_type + "_" + db_child_name);
    //     ExecuteDemoOne(db, db_child_name);
    // }

    // // Two-way join
    // private static void ExecuteDemoOne(SimpleDB db, String db_child_name) {
    //     String qry;
    //     qry = "select sid, sname, eid, studentid from enroll, student where sid = studentid";
    //     // Default to run the query 3 times and take average
    //     int n = 1;
    //     ArrayList<Double> times = new ArrayList<Double>();
    //     for (int i = 0; i < n; i++) {
    //         long startTime = System.currentTimeMillis();
    //         TestUtil.executeQuery(qry, db);
    //         long endTime = System.currentTimeMillis();
    //         Double duration = (endTime - startTime) / 1000.0;
    //         times.add(duration);
    //     }
    //     Double avgTime = getAvarageTime(times);
    //     System.out.println("# of rows: "+ db_child_name + ", Query: " + qry );
    //     System.out.println("Average time: " + avgTime + " s");
    //     for (int i = 0; i < times.size(); i++) {
    //         System.out.println("Run#" + i + " Time: " + times.get(i) + " s");
    //     }
    // }

    // private static Double getAvarageTime(ArrayList<Double> times) {
    //     Double sum = 0.0;
    //     for (Double t : times) {
    //         sum += t;
    //     }
    //     return sum / times.size();
    // }

    // // 4 way join
    // private static void RunDemoTwo(String db_child_name) {
    //     SimpleDB db = new SimpleDB(db_base_name + db_child_name);
    //     ExecuteDemoTwo(db, db_child_name);
    // }
    // // 4 way join with index
    // private static void RunDemoTwoWithIndex(String db_child_name, String index_type) {
    //     SimpleDB db = new SimpleDB(db_base_name_demo_two + "index_" + index_type + "_" + db_child_name);
    //     ExecuteDemoTwo(db, db_child_name);
    // }

    // private static void ExecuteDemoTwo(SimpleDB db, String db_child_name) {
    //     String qry;
    //     qry = "select sid, studentid, sectionid, secid, staffid, stid from student, enroll, section, staff where sid = studentid and sectionid = secid and staffid = stid";
    //     // Default to run the query 3 times and take average
    //     int n = 1;
    //     ArrayList<Double> times = new ArrayList<Double>();
    //     for (int i = 0; i < n; i++) {
    //         long startTime = System.currentTimeMillis();
    //         TestUtil.executeQuery(qry, db);
    //         long endTime = System.currentTimeMillis();
    //         Double duration = (endTime - startTime) / 1000.0;
    //         times.add(duration);
    //     }
    //     Double avgTime = getAvarageTime(times);
    //     System.out.println("# of rows: "+ db_child_name + ", Query: " + qry );
    //     System.out.println("Average time: " + avgTime + " s");
    //     for (int i = 0; i < times.size(); i++) {
    //         System.out.println("Run#" + i + " Time: " + times.get(i) + " s");
    //     }
    // }


    // // Additional Methods that can be used 
    // private static void ExecuteDemoWithJDBC(String db_child_name, String index_type) {
    //     String url = "jdbc:simpledb:"  + db_base_name + "index_" + index_type + "_" + db_child_name;
    //     String qry;
    //     qry = "select sid, sname, eid, studentid from enroll, student where sid = studentid";
    //     // Default to run the query 3 times and take average
    //     int n = 1;
    //     ArrayList<Double> times = new ArrayList<Double>();
    //     for (int j = 0; j < n; j++) {
    //         long startTime = System.currentTimeMillis();
    //         ExecuteWithJDBC(url, qry);
    //         long endTime = System.currentTimeMillis();
    //         Double duration = (endTime - startTime) / 1000.0;
    //         times.add(duration);
    //     }

    //     Double avgTime = getAvarageTime(times);
    //     System.out.println("# of rows: "+ db_child_name + ", Query: " + qry );
    //     System.out.println("Average time: " + avgTime + " s");
    //     for (int i = 0; i < times.size(); i++) {
    //         System.out.println("Run#" + i + " Time: " + times.get(i) + " s");
    //     }
    // }

    // private static void ExecuteWithJDBC(String url, String qry) {
    //     Driver d = new EmbeddedDriver();
    //     try (Connection conn = d.connect(url, null);
    //         Statement stmt = conn.createStatement();
    //         ResultSet rs = stmt.executeQuery(qry)) {

    //         ResultSetMetaData md = rs.getMetaData();
    //         int numcols = md.getColumnCount();
    //         int totalwidth = 0;
    
    //         // print header
    //         for(int i=1; i<=numcols; i++) {
    //             String fldname = md.getColumnName(i);
    //             int width = md.getColumnDisplaySize(i);
    //             totalwidth += width;
    //             String fmt = "%" + width + "s";
    //             System.out.format(fmt, fldname);
    //         }
    //         System.out.println();
    //         for(int i=0; i<totalwidth; i++)
    //             System.out.print("-");
    //         System.out.println();
    //         int count = 0;
    //         // print records
    //         while(rs.next()) {
    //             count++;
    //             for (int i=1; i<=numcols; i++) {
    //                 String fldname = md.getColumnName(i);
    //                 int fldtype = md.getColumnType(i);
    //                 String fmt = "%" + md.getColumnDisplaySize(i);
    //                 if (fldtype == Types.INTEGER) {
    //                     int ival = rs.getInt(fldname);
    //                     System.out.format(fmt + "d", ival);
    //                 }
    //                 else {
    //                     String sval = rs.getString(fldname);
    //                     System.out.format(fmt + "s", sval);
    //                 }
    //             }
    //             System.out.print(" " + count);
    //             System.out.println();
    //         }
            
    //     }
    //     catch(Exception e) {
    //         e.printStackTrace();
    //     }
    // }
}
