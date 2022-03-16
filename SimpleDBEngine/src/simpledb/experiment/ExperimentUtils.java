package simpledb.experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

import simpledb.server.SimpleDB;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

public class ExperimentUtils {
    // Replace the path to your local absolute path
    final static Path rootTestDataFolder = Path.of("C:\\Users\\Jiaxiang\\git\\cs3223-project\\fake_data");
    final static Path rootDbFolder = Path.of("C:\\Users\\Jiaxiang\\git\\cs3223-project");
    final static String[] filenames = {"STUDENT.sql", "COURSE.sql", "STAFF.sql", "ENROLL.sql", "SECTION.sql"};

    public static void InsertExperimentData(String dataset_folder_name) {
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
}
