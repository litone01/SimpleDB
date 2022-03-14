package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class DistinctPlan implements Plan {
    private Plan p;
    private Schema sch = new Schema();
    private Transaction tx;
    private List<String> distinctFields;
    private RecordComparator comp;

    public DistinctPlan(Plan p, List<String> distinctFields, Transaction tx) {
        this.p = p;
        this.sch = p.schema();
        this.distinctFields = distinctFields;
        this.tx = tx;
        this.comp = new RecordComparator(distinctFields);
    }

    /**
     * Opens a scan corresponding to this plan.
     * The scan will be positioned before its first record.
     *
     * @return a scan
     */
    @Override
    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 1)
            runs = doAMergeIteration(runs);
        Scan result = runs.get(0).open();


        return new DistinctScan(result);
    }

    /**
     * Returns an estimate of the number of block accesses
     * that will occur when the scan is read to completion.
     *
     * @return the estimated number of block accesses
     */
    @Override
    public int blocksAccessed() {
        //not done
        // p.blocksAccessed() + blocks accessed during sorting and merging
        return p.blocksAccessed();
    }

    /**
     * Returns an estimate of the number of records
     * in the query's output table.
     *
     * @return the estimated number of output records
     */
    @Override
    public int recordsOutput() {
        //number of distinct records
        //not done
        //need to change
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
        if (!src.next())
            return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan))
            if (comp.compare(src, currentscan) < 0) {
                // start a new run
                currentscan.close();
                currenttemp = new TempTable(tx, sch);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
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
        //merge and remove duplicates?
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

        while (hasmore1 && hasmore2)

            if (comp.compare(src1, src2) < 0)
                if(comp.compare(src1, dest) != 0) {
                    hasmore1 = copy(src1, dest);
                } else{
                    hasmore1 = src1.next();;
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

        if (hasmore1)
            while (hasmore1)
                if(comp.compare(src1, dest)!=0 ){
                    hasmore1 = copy(src1, dest);
                } else {
                    hasmore1 = src1.next();
                }

        else
            while (hasmore2)
                if(comp.compare(src2, dest)!=0){
                    hasmore2 = copy(src2, dest);
                } else {
                    hasmore2 = src2.next();
                }


        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
        return src.next();
    }

}
