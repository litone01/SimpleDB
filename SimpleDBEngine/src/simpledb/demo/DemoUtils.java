package simpledb.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.server.SimpleDB;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

public class DemoUtils {
    // Replace the path to your local absolute path
    final static Path rootTestDataFolder = Path.of("C:\\Users\\Jiaxiang\\git\\cs3223-project\\fake_data");
    final static Path rootDbFolder = Path.of("C:\\Users\\Jiaxiang\\git\\cs3223-project");
    final static String[] filenames = {"STUDENT.sql", "COURSE.sql", "STAFF.sql", "ENROLL.sql", "SECTION.sql"};
    final static Map<String, String> createHashIndexQueries = new HashMap<String, String>();
    final static Map<String, String> createBtreeIndexQueries = new HashMap<String, String>();
    final static Map<String, List<String>> createHashIndexQueriesMultiTable = new HashMap<>();
    final static Map<String, List<String>> createBtreeIndexQueriesMultiTable = new HashMap<>();
    final static int max_inserts_per_transaction_for_index = 1;

    public static void InsertDemoDataSingleTable(String dataset_folder_name, String index_type) {
        // if db_demo_{dataset_folder_name} exists, print warning and stop
        if (new File(rootDbFolder.resolve("db_demo_index_" + index_type + "_" + dataset_folder_name).toString()).exists()) {
            System.out.println("Warning: " + "db_demo_index_" + index_type + "_" + dataset_folder_name + " exists, skip inserting data");
            return;
        }
        InitCreateIndexQueriesSingleTable();
        SimpleDB db = new SimpleDB("db_demo_index_" + index_type + "_" + dataset_folder_name);
        for (final String filename : filenames) {
            InsertFileDataSingleTable(rootTestDataFolder.resolve(dataset_folder_name).resolve(filename), db, index_type);
        }
    }

    private static void InitCreateIndexQueriesSingleTable() {
        // both will always be initialized together
        if (!createHashIndexQueries.isEmpty()) {
            return;
        }
        // Hash index
        createHashIndexQueries.put("SECTION", "create index courseidh on section(courseid) using hash");
        // Btree index
        createBtreeIndexQueries.put("SECTION", "create index courseidb on section(courseid) using btree");
    }

    private static void InsertFileDataSingleTable(Path file, SimpleDB db, String index_type) {
        Planner planner = db.planner();
        Transaction tx = db.newTx();
        final String file_name = file.getFileName().toString();
        final String table_name = file_name.substring(0, file_name.lastIndexOf('.'));
        BufferedReader reader;
        String query;
        int query_count = 0;
        try {
            reader = Files.newBufferedReader(file);
            while ((query = reader.readLine()) != null) {
                query_count++;
                // create index on the id column after table is created
                if (query_count == 2 && createHashIndexQueries.containsKey(table_name)) {
                    if (index_type.equals("hash")) {
                        planner.executeUpdate(createHashIndexQueries.get(table_name), tx);
                    } else if (index_type.equals("btree")) {
                        planner.executeUpdate(createBtreeIndexQueries.get(table_name), tx);
                    }
                    query_count++;
                }
                planner.executeUpdate(query, tx);

                // // Btree index cannot insert multiple values in one transcation
                // // We commit and start a new transaction after {max_inserts_per_transaction_for_index} inserts
                // if (query_count % max_inserts_per_transaction_for_index == 0) {
                //     // System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries");
                //     tx.commit();
                //     planner = db.planner();
                //     tx = db.newTx();
                // }
            }
            reader.close();
            System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries with index type " + index_type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tx.commit();
    }

    // For multi-table join demo.
    public static void InsertDemoDataMultiTable(String dataset_folder_name, String index_type) {
         // if db_demo_{dataset_folder_name} exists, print warning and stop
         if (new File(rootDbFolder.resolve("db_demo_2_index_" + index_type + "_" + dataset_folder_name).toString()).exists()) {
            System.out.println("Warning: " + "db_demo_2_index_" + index_type + "_" + dataset_folder_name + " exists, skip inserting data");
            return;
        }
        InitCreateIndexQueriesMultiTable();
        SimpleDB db = new SimpleDB("db_demo_2_index_" + index_type + "_" + dataset_folder_name);
        for (final String filename : filenames) {
            InsertFileDataWithIndexDemoTwo(rootTestDataFolder.resolve(dataset_folder_name).resolve(filename), db, index_type);
        }
    }

    private static void InitCreateIndexQueriesMultiTable() {
        // both will always be initialized together
        if (!createHashIndexQueriesMultiTable.isEmpty()) {
            return;
        }
        // Hash index
        List<String> qry = new ArrayList<>();
        qry.add("create index sidh on student(sid) using hash");
        createHashIndexQueriesMultiTable.put("STUDENT", qry);
        qry = new ArrayList<>();
        qry.add("create index stidh on staff(stid) using hash");
        createHashIndexQueriesMultiTable.put("STAFF", qry);
        qry = new ArrayList<>();
        qry.add("create index studentidh on enroll(studentid) using hash");
        qry.add("create index sectionidh on enroll(sectionid) using hash");
        createHashIndexQueriesMultiTable.put("ENROLL", qry);
        qry = new ArrayList<>();
        qry.add("create index staffidh on section(staffid) using hash");
        qry.add("create index secidh on section(secid) using hash");
        createHashIndexQueriesMultiTable.put("SECTION", qry);

        // Btree index
        qry = new ArrayList<>();
        qry.add("create index sidb on student(sid) using btree");
        createBtreeIndexQueriesMultiTable.put("STUDENT", qry);
        qry = new ArrayList<>();
        qry.add("create index stidb on staff(stid) using btree");
        createBtreeIndexQueriesMultiTable.put("STAFF", qry);
        qry = new ArrayList<>();
        qry.add("create index studentidb on enroll(studentid) using btree");
        qry.add("create index sectionidb on enroll(sectionid) using btree");
        createBtreeIndexQueriesMultiTable.put("ENROLL", qry);
        qry = new ArrayList<>();
        qry.add("create index staffidb on section(staffid) using btree");
        qry.add("create index secidb on section(secid) using btree");
        createBtreeIndexQueriesMultiTable.put("SECTION", qry);
    }

     // For multi-table join demo.
     private static void InsertFileDataWithIndexDemoTwo(Path file, SimpleDB db, String index_type) {
        Planner planner = db.planner();
        Transaction tx = db.newTx();
        final String file_name = file.getFileName().toString();
        final String table_name = file_name.substring(0, file_name.lastIndexOf('.'));
        BufferedReader reader;
        String query;
        int query_count = 0;
        try {
            reader = Files.newBufferedReader(file);
            while ((query = reader.readLine()) != null) {
                query_count++;
                // create index on the id column after table is created
                if (query_count == 2 && createHashIndexQueriesMultiTable.containsKey(table_name)) {
                    if (index_type.equals("hash")) {
                        for (String q : createHashIndexQueriesMultiTable.get(table_name)) {
                            planner.executeUpdate(q, tx);
                            query_count++;
                        }
                    } else if (index_type.equals("btree")) {
                        for (String q : createBtreeIndexQueriesMultiTable.get(table_name)) {
                            planner.executeUpdate(q, tx);
                            query_count++;
                        }
                    } else {
                        throw new RuntimeException("Index type " + index_type + " is not supported for table " + table_name);
                    }
                }
                planner.executeUpdate(query, tx);

                // // Btree index cannot insert multiple values in one transcation
                // // We commit and start a new transaction after {max_inserts_per_transaction_for_index} inserts
                // if (query_count % max_inserts_per_transaction_for_index == 0) {
                //     // System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries");
                //     tx.commit();
                //     planner = db.planner();
                //     tx = db.newTx();
                // }
            }
            reader.close();
            System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries with index type " + index_type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tx.commit();
    }

}
