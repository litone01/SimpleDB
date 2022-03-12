package simpledb.test;

import java.util.LinkedHashMap;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.tx.Transaction;

public class TestUtil {
    // Note that only for type, only INT and STRING are supported and the spelling must be exact, i.e. all CAPS
    public static void executeSelectQuery(String qry, Transaction tx, Planner planner, LinkedHashMap<String, String> fieldNameAndTypes) {
        System.out.println(qry);
        Plan p = planner.createQueryPlan(qry, tx);
        Scan scan = p.open();
        // print header
        for (String fieldName : fieldNameAndTypes.keySet()) {
            System.out.print(fieldName + "\t");
        }
        System.out.println();

        // print data based on field name and type
        while (scan.next()) {
            for (String fieldName : fieldNameAndTypes.keySet()) {
                String fieldType = fieldNameAndTypes.get(fieldName);
                if (fieldType.equals("INT")) {
                    System.out.print(scan.getInt(fieldName) + "\t");
                } else if (fieldType.equals("STRING")) {
                    System.out.print(scan.getString(fieldName) + "\t");
                } else {
                    System.out.print("unknown type" + "\t");
                }
            }
            System.out.println();
        }
        scan.close();
        System.out.println("end of select query\n");
        
    }

    public static void createSampleStudentDBWithoutIndex(Planner planner, Transaction tx) {
        String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
        planner.executeUpdate(s, tx);
        System.out.println("Table STUDENT created.");

        s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
        String[] studvals = {"(1, 'joe', 10, 2021)",
            "(2, 'amy', 20, 2020)",
            "(3, 'max', 10, 2022)",
            "(4, 'sue', 20, 2022)",
            "(5, 'bob', 30, 2020)",
            "(6, 'kim', 20, 2020)",
            "(7, 'art', 30, 2021)",
            "(8, 'pat', 20, 2019)",
            "(9, 'lee', 10, 2021)"};
        for (int i=0; i<studvals.length; i++)
        planner.executeUpdate(s + studvals[i], tx);
        System.out.println("STUDENT records inserted.");
    
        s = "create table DEPT(DId int, DName varchar(8))";
        planner.executeUpdate(s, tx);
        System.out.println("Table DEPT created.");
    
        s = "insert into DEPT(DId, DName) values ";
        String[] deptvals = {"(10, 'compsci')",
                            "(20, 'math')",
                            "(30, 'drama')"};
        for (int i=0; i<deptvals.length; i++)
            planner.executeUpdate(s + deptvals[i], tx);
        System.out.println("DEPT records inserted.");
    
        s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
        planner.executeUpdate(s, tx);
        System.out.println("Table COURSE created.");
    
        s = "insert into COURSE(CId, Title, DeptId) values ";
        String[] coursevals = {"(12, 'db systems', 10)",
                            "(22, 'compilers', 10)",
                            "(32, 'calculus', 20)",
                            "(42, 'algebra', 20)",
                            "(52, 'acting', 30)",
                            "(62, 'elocution', 30)"};
        for (int i=0; i<coursevals.length; i++)
            planner.executeUpdate(s + coursevals[i], tx);
        System.out.println("COURSE records inserted.");
    
        s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
        planner.executeUpdate(s, tx);
        System.out.println("Table SECTION created.");
    
        s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
        String[] sectvals = {"(13, 12, 'turing', 2018)",
                            "(23, 12, 'turing', 2019)",
                            "(33, 32, 'newton', 2019)",
                            "(43, 32, 'einstein', 2017)",
                            "(53, 62, 'brando', 2018)"};
        for (int i=0; i<sectvals.length; i++)
            planner.executeUpdate(s + sectvals[i], tx);
        System.out.println("SECTION records inserted.");
    
        s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
        planner.executeUpdate(s, tx);
        System.out.println("Table ENROLL created.");
        

        s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
        String[] enrollvals = {"(14, 1, 13, 'A')",
                            "(24, 1, 43, 'C' )",
                            "(34, 2, 43, 'B+')",
                            "(44, 4, 33, 'B' )",
                            "(54, 4, 53, 'A' )",
                            "(64, 6, 53, 'A' )"};
        for (int i=0; i<enrollvals.length; i++)
            planner.executeUpdate(s + enrollvals[i], tx);
        System.out.println("ENROLL records inserted.");

        // TODO: should tx.commit() be here?
        // tx.commit();
    }
}
