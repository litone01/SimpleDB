package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class DistinctPlan implements Plan {
    private Plan p;
    private Schema sch = new Schema();
    private Transaction tx;
    private List<String> distinctFields;
    private RecordComparator comp;
    private boolean onlyOneRun;
    private int numberOfSortedRuns = -1;

    public DistinctPlan(Plan p, List<String> distinctFields, Transaction tx) {
        this.onlyOneRun = true;
        this.p = p;
        this.sch = p.schema();
        this.distinctFields = distinctFields;
        this.tx = tx;
        this.comp = new RecordComparator(distinctFields);
    }

    /**
     * Opens a scan corresponding to this plan.
     * The scan will be positioned before its first record.
     * Implements the optimized sort-based duplicate removal algorithm.
     *
     * @return a scan
     */
    @Override
    public Scan open() {
        Scan src = p.open();
        //split into sorted runs
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 1) {
            // if the records are already sorted,
            // there will be no actual splitting and we only have one run
            // then we will never enter this loop to do any merging and duplicate removal
            runs = doAMergeIteration(runs);
        }

        List<TempTable> results = new ArrayList<>();

        if(runs.size()==0){
            return new DistinctScan(results);
        }

        Scan result;
        TempTable temp = runs.get(0);
        if(onlyOneRun) {
            // if there is only one run
            // we will explicitly handle it
            results.add(removeDuplicates(temp));
        } else {
            results.add(temp);
        }
        return new DistinctScan(results);
    }

    /**
     * Returns a temp table which has its duplicates removed
     * This method is only used if no splitting occurs and we only have one sorted table.
     * Then we will call this routine to remove duplicates.
     *
     * @return a temp table which has its duplicates removed
     */
    private TempTable removeDuplicates(TempTable p) {
        Scan src = p.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore = src.next();

        if (hasmore) {
            hasmore = copy(src, dest);
            while (hasmore) {
                if(comp.compare(src, dest)!=0 ){
                    hasmore = copy(src, dest);
                } else {
                    hasmore = src.next();
                }
            }
        }

        src.close();
        dest.close();
        return result;
    }
    /**
     * Return the number of blocks in the distinct table,
     * which is the same as it would be in a
     * materialized table.
     * It does not include the one-time cost
     * of materializing and sorting the records.
     * @return the estimated number of block accesses
     */
    @Override
    public int blocksAccessed() {
        Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
        if (numberOfSortedRuns == -1) {
           numberOfSortedRuns = (int) Math.ceil(mp.blocksAccessed() / SimpleDB.BUFFER_SIZE);
        }
        int numIteration = (int) Math.ceil(Math.log(numberOfSortedRuns) / Math.log(SimpleDB.BUFFER_SIZE - 1)) + 1;
        return 2 * mp.blocksAccessed() * numIteration;
    }

    /**
     * Returns an estimate of the number of records
     * in the distinct table.
     * The output valus is the upper bound estimate of the precise value.
     *
     * @return the estimated number of output records
     */
    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Returns an estimate of the number of distinct values
     * for the specified field in the query's output table.
     *
     * @param fldname the name of a field
     * @return the estimated number of distinct field values in the output
     */
    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the query.
     *
     * @return the query's schema
     */
    @Override
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next()) {
            return temps;
        }
        numberOfSortedRuns = 1;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) {
            if (comp.compare(src, currentscan) < 0) {
                onlyOneRun = false;
                // start a new run
                currentscan.close();
                numberOfSortedRuns++;
                currenttemp = new TempTable(tx, sch);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
            }
        }
        currentscan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1)
            result.add(runs.get(0));
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        //merge and remove duplicates
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        //initialize dest
        if (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0)
                hasmore1 = copy(src1, dest);
            else
                hasmore2 = copy(src2, dest);
        }

        while (hasmore1 && hasmore2) {

            if (comp.compare(src1, src2) < 0)
                if(comp.compare(src1, dest) != 0) {
                    hasmore1 = copy(src1, dest);
                } else{
                    hasmore1 = src1.next();
                }
            else {
                // src2 <= src1
                // in both cases of < and =, we check src2
                // (when src1=src2, give priority to src2)
                if(comp.compare(src2, dest) != 0){
                    hasmore2 = copy(src2, dest);
                } else{
                    hasmore2 = src2.next();
                }
            }
        }

        if (hasmore1) {
            while (hasmore1)
                if(comp.compare(src1, dest)!=0 ){
                    hasmore1 = copy(src1, dest);
                } else {
                    hasmore1 = src1.next();
                }

        } else {
            while (hasmore2) {
                if(comp.compare(src2, dest)!=0){
                    hasmore2 = copy(src2, dest);
                } else {
                    hasmore2 = src2.next();
                }
            }
        }


        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields()) {
            dest.setVal(fldname, src.getVal(fldname));
        }
        return src.next();
    }

    /**
    * Return the string representation of order by plan
    */
    @Override
    public String toString() {
        return "( " + p.toString() + " distinct " + distinctFields.toString() + " )";
    }

}
