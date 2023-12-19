package simpledb.experiment;

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

public class ExperimentUtils {
    // Replace the path to your local absolute path
    final static Path rootTestDataFolder = Path.of("C:\\Users\\Jiaxiang\\git\\cs3223-project\\fake_data");
    final static Path rootDbFolder = Path.of("C:\\Users\\Jiaxiang\\git\\cs3223-project");
    final static String[] filenames = {"STUDENT.sql", "COURSE.sql", "STAFF.sql", "ENROLL.sql", "SECTION.sql"};
    final static Map<String, String> createHashIndexQueries = new HashMap<String, String>();
    final static Map<String, String> createBtreeIndexQueries = new HashMap<String, String>();
    final static Map<String, List<String>> createHashIndexQueriesExperimentTwo = new HashMap<>();
    final static int max_inserts_per_transaction_for_index = 1;

    public static void InsertExperimentData(String dataset_folder_name) {
        // if db_experiment_{dataset_folder_name} exists, print warning and stop
        if (new File(rootDbFolder.resolve("db_experiment_" + dataset_folder_name).toString()).exists()) {
            System.out.println("Warning: " + "db_experiment_" + dataset_folder_name + " exists, skip inserting data");
            return;
        }
        SimpleDB db = new SimpleDB("db_experiment_" + dataset_folder_name);
        for (final String filename : filenames) {
            InsertFileData(rootTestDataFolder.resolve(dataset_folder_name).resolve(filename), db);
        }
    }

    private static void InsertFileData(Path file, SimpleDB db) {
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
                planner.executeUpdate(query, tx);
                query_count++;
            }
            reader.close();
            System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries");
        } catch (IOException e) {
            e.printStackTrace();
        }
        tx.commit();
    }

    // Index will be added when data is inserted
    // Index type can only be "hash" or "btree"
    public static void InsertExperimentDataWithIndex(String dataset_folder_name, String index_type) {
        // if db_experiment_{dataset_folder_name} exists, print warning and stop
        if (new File(rootDbFolder.resolve("db_experiment_index_" + index_type + "_" + dataset_folder_name).toString()).exists()) {
            System.out.println("Warning: " + "db_experiment_index_" + index_type + "_" + dataset_folder_name + " exists, skip inserting data");
            return;
        }
        InitCreateIndexQueries();
        SimpleDB db = new SimpleDB("db_experiment_index_" + index_type + "_" + dataset_folder_name);
        for (final String filename : filenames) {
            InsertFileDataWithIndex(rootTestDataFolder.resolve(dataset_folder_name).resolve(filename), db, index_type);
        }
    }

    private static void InitCreateIndexQueries() {
        // both will always be initialized together
        if (!createHashIndexQueries.isEmpty()) {
            return;
        }
        // Hash index
        createHashIndexQueries.put("STUDENT", "create index sidh on student(sid) using hash");
        createHashIndexQueries.put("STAFF", "create index stidh on staff(stid) using hash");
        createHashIndexQueries.put("ENROLL", "create index studentidh on enroll(studentid) using hash");
        createHashIndexQueries.put("SECTION", "create index staffidh on section(staffid) using hash");
        // Btree index
        createBtreeIndexQueries.put("STUDENT", "create index sidb on student(sid) using btree");
        createBtreeIndexQueries.put("STAFF", "create index stidb on staff (stid) using btree");
        createBtreeIndexQueries.put("ENROLL", "create index studentidb on enroll(studentid) using btree");
        createBtreeIndexQueries.put("SECTION", "create index staffidb on section(staffid) using btree");
    }

    // Index will be added when data is inserted
    private static void InsertFileDataWithIndex(Path file, SimpleDB db, String index_type) {
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

                // Btree index cannot insert multiple values in one transcation
                // We commit and start a new transaction after {max_inserts_per_transaction_for_index} inserts
                if (query_count % max_inserts_per_transaction_for_index == 0) {
                    // System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries");
                    tx.commit();
                    planner = db.planner();
                    tx = db.newTx();
                }
            }
            reader.close();
            System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries with index type " + index_type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tx.commit();
    }

    public static void InsertExperimentTwoDataWithIndex(String dataset_folder_name, String index_type) {
         // if db_experiment_{dataset_folder_name} exists, print warning and stop
         if (new File(rootDbFolder.resolve("db_experiment_2_index_" + index_type + "_" + dataset_folder_name).toString()).exists()) {
            System.out.println("Warning: " + "db_experiment_2_index_" + index_type + "_" + dataset_folder_name + " exists, skip inserting data");
            return;
        }
        InitCreateIndexQueriesExperimentTwo();
        SimpleDB db = new SimpleDB("db_experiment_2_index_" + index_type + "_" + dataset_folder_name);
        for (final String filename : filenames) {
            InsertFileDataWithIndexExperimentTwo(rootTestDataFolder.resolve(dataset_folder_name).resolve(filename), db, index_type);
        }
    }


    private static void InitCreateIndexQueriesExperimentTwo() {
        // both will always be initialized together
        if (!createHashIndexQueriesExperimentTwo.isEmpty()) {
            return;
        }
        // Hash index
        List<String> qry = new ArrayList<>();
        qry.add("create index sidh on student(sid) using hash");
        createHashIndexQueriesExperimentTwo.put("STUDENT", qry);
        qry = new ArrayList<>();
        qry.add("create index stidh on staff(stid) using hash");
        createHashIndexQueriesExperimentTwo.put("STAFF", qry);
        qry = new ArrayList<>();
        qry.add("create index studentidh on enroll(studentid) using hash");
        qry.add("create index sectionidh on enroll(sectionid) using hash");
        createHashIndexQueriesExperimentTwo.put("ENROLL", qry);
        qry = new ArrayList<>();
        qry.add("create index staffidh on section(staffid) using hash");
        qry.add("create index secidh on section(secid) using hash");
        createHashIndexQueriesExperimentTwo.put("SECTION", qry);
    }

     // Index will be added when data is inserted
     private static void InsertFileDataWithIndexExperimentTwo(Path file, SimpleDB db, String index_type) {
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
                if (query_count == 2 && createHashIndexQueriesExperimentTwo.containsKey(table_name)) {
                    if (index_type.equals("hash")) {
                        for (String q : createHashIndexQueriesExperimentTwo.get(table_name)) {
                            planner.executeUpdate(q, tx);
                            query_count++;
                        }
                    } else {
                        throw new RuntimeException("Index type " + index_type + " is not supported for table " + table_name);
                    }
                }
                planner.executeUpdate(query, tx);

                // Btree index cannot insert multiple values in one transcation
                // We commit and start a new transaction after {max_inserts_per_transaction_for_index} inserts
                if (query_count % max_inserts_per_transaction_for_index == 0) {
                    // System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries");
                    tx.commit();
                    planner = db.planner();
                    tx = db.newTx();
                }
            }
            reader.close();
            System.out.println("Inserted data from file " + file_name + " into table " + table_name + " with " + query_count + " queries with index type " + index_type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tx.commit();
    }

}
