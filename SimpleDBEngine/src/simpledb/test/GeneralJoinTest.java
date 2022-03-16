package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class GeneralJoinTest {
    public static void main(String[] args) {
        try {
            SimpleDB db = new SimpleDB("db_generalJoinTest");
            Transaction tx  = db.newTx();
            Planner planner = db.planner();
            
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
            // Also, check if the join method has been set the correct join algorithm
            TestUtil.createSampleStudentDBWithoutIndex(planner, tx);

            String qry;
            // Test query on general join
            // Section A. with = only
            // 1. one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did";
            TestUtil.executeQuery(qry, db);

            // Section B. with non-equality operators only
            // != operator, one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid != did";
            TestUtil.executeQuery(qry, db);
            // != operator, two predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid != did and sname != dname";
            TestUtil.executeQuery(qry, db);
            // > operator, one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid > did";
            TestUtil.executeQuery(qry, db);
            // <= operator, one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid <= did";
            TestUtil.executeQuery(qry, db);
            // > and <, two predicates
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid > did and sid < did";
            TestUtil.executeQuery(qry, db);
            // switched table order, two predicates
            qry = "select sid, sname, majorid, did, dname from dept, student where majorid > did and sid < did";
            TestUtil.executeQuery(qry, db);

            // Section C. with mixed = and non-equality operators
            // = and !=, two predicates
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did and sid != did";
            TestUtil.executeQuery(qry, db);
            // = and >, two predicates, did and sid order are swapped
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did and did > sid";
            TestUtil.executeQuery(qry, db);
            // = and <, three predicates, Invalid case
            // qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did and did < sid and sid = did";
            // TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            
            // Section D. with multiple tables and mixed join operators
            // =, three predicates, three tables (student, dept, course)
            qry = "select sid, sname, majorid, did, dname, title from student, dept, course where majorid = did and did = deptid";
            TestUtil.executeQuery(qry, db);

            // =, three predicate, three tables (student, enroll, section)
            qry = "select sname, grade, sid, studentid, sectionid, sectid from student, enroll, section where sid = studentid and sectionid = sectid";
            TestUtil.executeQuery(qry, db);

            // =, four predicate, four tables (student, enroll, section, course)
            qry = "select sname, grade, sid, studentid, sectionid, sectid, courseid, cid, title from student, enroll, section, course where sid = studentid and sectionid = sectid and courseid = cid";
            TestUtil.executeQuery(qry, db);

            // =, five predicate, five tables (student, enroll, section, course, dept)
            qry = "select sname, grade, sid, studentid, sectionid, sectid, courseid, cid, title, deptid, did, dname from student, enroll, section, course, dept where sid = studentid and sectionid = sectid and courseid = cid and deptid = did";
            TestUtil.executeQuery(qry, db);

            // =, five predicate, five tables (student, enroll, section, course, dept), and a non-join related predicate
            qry = "select sname, grade, sid, studentid, sectionid, sectid, courseid, cid, title, deptid, did, dname from student, enroll, section, course, dept where sid = studentid and sectionid = sectid and courseid = cid and deptid = did and deptid > 20";
            TestUtil.executeQuery(qry, db);

            // mixed operators, five tables
            qry = "select sname, grade, sid, studentid, sectionid, sectid, courseid, cid, title, deptid, did, dname from student, enroll, section, course, dept where sid = studentid and sectionid < sectid and courseid >= cid and deptid != did and sid = 1";
            TestUtil.executeQuery(qry, db);

            // selected fields are not used in where clause
            qry = "select sname, grade from student, enroll, section, course, dept where sid = studentid and sectionid = sectid and courseid = cid and deptid = did";
            TestUtil.executeQuery(qry, db);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}

