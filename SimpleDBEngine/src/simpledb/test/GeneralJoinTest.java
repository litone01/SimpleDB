package simpledb.test;

import simpledb.tx.Transaction;

import java.util.LinkedHashMap;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.test.TestUtil;

public class GeneralJoinTest {
    public static void main(String[] args) {
        try {
            SimpleDB db = new SimpleDB("db_generalJoinTest");
            Transaction tx  = db.newTx();
            Planner planner = db.planner();
            
            // NOTE: COMMENT OUT this once the first one! Dont create the tables again!
            // Also, check if the join method has been set the correct join algorithm
            // TestUtil.createSampleStudentDBWithoutIndex(planner, tx);
            
            // LinkedHashMap (we want to preserve insertion order) for fieldName and its corresponding type
            // In this test, we have:
            // 1. sid: INT
            // 2. sname: STRING
            // 3. majorid: INT
            // 4. did: INT
            // 5. dname: STRING
            LinkedHashMap<String, String> fieldNameAndType = new LinkedHashMap<String, String>();
            fieldNameAndType.put("sid", "INT");
            fieldNameAndType.put("sname", "STRING");
            fieldNameAndType.put("majorid", "INT");
            fieldNameAndType.put("did", "INT");
            fieldNameAndType.put("dname", "STRING");

            String qry;
            // Test query on general join
            // Section A. with = only
            // 1. one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Section B. with non-equality operators only
            // != operator, one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid != did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // != operator, two predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid != did and sname != dname";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // > operator, one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid > did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // <= operator, one predicate
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid <= did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // > and <, two predicates
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid > did and sid < did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // switched table order, two predicates
            qry = "select sid, sname, majorid, did, dname from dept, student where majorid > did and sid < did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);

            // Section C. with mixed = and non-equality operators
            // = and !=, two predicates
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did and sid != did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // = and >, two predicates, did and sid order are swapped
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did and did > sid";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            // = and <, three predicates
            qry = "select sid, sname, majorid, did, dname from student, dept where majorid = did and did < sid and sid = did";
            TestUtil.executeSelectQuery(qry, tx, planner, fieldNameAndType);
            
            // Section D. with multiple tables and mixed join operators
            // 1. sid: INT; 2.sname: STRING; 3. majorid: INT; 4. did: INT; 5. dname: STRING; 6. title: STRING
            LinkedHashMap<String, String> studentAndDeptAndCourse = new LinkedHashMap<String, String>();
            studentAndDeptAndCourse.put("sid", "INT");
            studentAndDeptAndCourse.put("sname", "STRING");
            studentAndDeptAndCourse.put("majorid", "INT");
            studentAndDeptAndCourse.put("did", "INT");
            studentAndDeptAndCourse.put("dname", "STRING");
            studentAndDeptAndCourse.put("title", "STRING");
            // =, three predicates, three tables (student, dept, course)
            qry = "select sid, sname, majorid, did, dname, title from student, dept, course where majorid = did and did = deptid";
            TestUtil.executeSelectQuery(qry, tx, planner, studentAndDeptAndCourse);

            // sname: STRING; grade: STRING; sid: INT; studentid: INT; sectionid: INT; sectid: INT;
            LinkedHashMap<String, String> studentAndEnrollAndSection = new LinkedHashMap<String, String>(); 
            studentAndEnrollAndSection.put("sname", "STRING");
            studentAndEnrollAndSection.put("grade", "STRING");
            studentAndEnrollAndSection.put("sid", "INT");
            studentAndEnrollAndSection.put("studentid", "INT");
            studentAndEnrollAndSection.put("sectionid", "INT");
            studentAndEnrollAndSection.put("sectid", "INT");
            // =, three predicate, three tables (student, enroll, section)
            qry = "select sname, grade, sid, studentid, sectionid, sectid from student, enroll, section where sid = studentid and sectionid = sectid";
            TestUtil.executeSelectQuery(qry, tx, planner, studentAndEnrollAndSection);

            // TODO: should consider reducing the selected fields if everything is tested ok
            // sname: STRING; grade: STRING; sid: INT; studentid: INT; sectionid: INT; sectid: INT; courseid: INT; cid: INT; title: STRING
            LinkedHashMap<String, String> studentAndEnrollAndSectionAndCourse = new LinkedHashMap<String, String>(); 
            studentAndEnrollAndSectionAndCourse.put("sname", "STRING");
            studentAndEnrollAndSectionAndCourse.put("grade", "STRING");
            studentAndEnrollAndSectionAndCourse.put("sid", "INT");
            studentAndEnrollAndSectionAndCourse.put("studentid", "INT");
            studentAndEnrollAndSectionAndCourse.put("sectionid", "INT");
            studentAndEnrollAndSectionAndCourse.put("sectid", "INT");
            studentAndEnrollAndSectionAndCourse.put("courseid", "INT");
            studentAndEnrollAndSectionAndCourse.put("cid", "INT");
            studentAndEnrollAndSectionAndCourse.put("title", "STRING");
            // =, four predicate, four tables (student, enroll, section, course)
            qry = "select sname, grade, sid, studentid, sectionid, sectid, courseid, cid, title from student, enroll, section, course where sid = studentid and sectionid = sectid and courseid = cid";
            TestUtil.executeSelectQuery(qry, tx, planner, studentAndEnrollAndSectionAndCourse);

            // TODO: should consider reducing the selected fields if everything is tested ok
            // sname: STRING; grade: STRING; sid: INT; studentid: INT; sectionid: INT; sectid: INT; courseid: INT; cid: INT; title: STRING; deptid: INT; did: INT; dname: STRING
            LinkedHashMap<String, String> allFiveTables = new LinkedHashMap<String, String>(); 
            allFiveTables.put("sname", "STRING");
            allFiveTables.put("grade", "STRING");
            allFiveTables.put("sid", "INT");
            allFiveTables.put("studentid", "INT");
            allFiveTables.put("sectionid", "INT");
            allFiveTables.put("sectid", "INT");
            allFiveTables.put("courseid", "INT");
            allFiveTables.put("cid", "INT");
            allFiveTables.put("title", "STRING");
            allFiveTables.put("deptid", "INT");
            allFiveTables.put("did", "INT");
            allFiveTables.put("dname", "STRING");
            // =, five predicate, five tables (student, enroll, section, course, dept)
            qry = "select sname, grade, sid, studentid, sectionid, sectid, courseid, cid, title, deptid, did, dname from student, enroll, section, course, dept where sid = studentid and sectionid = sectid and courseid = cid and deptid = did";
            TestUtil.executeSelectQuery(qry, tx, planner, allFiveTables);

            tx.commit();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}

