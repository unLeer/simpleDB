package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int ioCostPerPage;
    private int tableid;
    private HeapFile dbfile;
    private HashMap<Integer, Integer[]> statmap;//value0:min value1:max
    private HashMap<Integer, Object> histmap;
    private TupleDesc td;
    private TransactionId tid;
    private int ntups;

    //private DbFileIterator iterator;
        

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbfile = (HeapFile)Database.getCatalog().getDbFile(tableid);
        this.td = dbfile.getTupleDesc();
        this.ntups = 0;
        
        statmap = new HashMap<Integer, Integer[]>();
        histmap = new HashMap<Integer, Object>();
        tid = new TransactionId();

        DbFileIterator iterator = dbfile.iterator(tid);
        process(iterator);
    }

    private void process(DbFileIterator iterator){
        try{
            iterator.open();
            //initial
            for(int i=0; i<td.getFieldnums(); i++){
                if(td.getFieldType(i) != Type.INT_TYPE) continue;
                statmap.put(i, new Integer[]{Integer.MAX_VALUE, Integer.MIN_VALUE});
            }

            //get value
            while(iterator.hasNext()){
                Tuple tuple= iterator.next();
                ntups++;
                for(int i=0; i<td.getFieldnums();i++){
                    if(!statmap.containsKey(i)) continue;

                    Integer[] value = statmap.get(i);
                    IntField intfield = (IntField)tuple.getField(i);
                    value[0] = (Integer)Math.min(value[0], intfield.getValue());
                    value[1] =(Integer) Math.max(value[1], intfield.getValue());
                }
            }

            //new histogram
            for(int i=0; i<td.getFieldnums(); i++){
                if(td.getFieldType(i) == Type.INT_TYPE){
                    IntHistogram inthist = new IntHistogram(NUM_HIST_BINS, statmap.get(i)[0], statmap.get(i)[1]);
                    histmap.put(i, inthist);
                }
                else if(td.getFieldType(i) == Type.STRING_TYPE){
                    StringHistogram stringhist = new StringHistogram(NUM_HIST_BINS);
                    histmap.put(i, stringhist);
                }
            }

            //add object to histogram
            iterator.rewind();
            while(iterator.hasNext()){
                Tuple t = iterator.next();
                for(int i=0; i<td.getFieldnums(); i++){
                    Object obj = histmap.get(i);
                    if(obj instanceof IntHistogram){
                        IntHistogram inthist = (IntHistogram)obj;
                        IntField intfield = (IntField)t.getField(i);
                        inthist.addValue(intfield.getValue());
                    }
                    else{
                        StringHistogram stringhist = (StringHistogram)obj;
                        StringField stringfield = (StringField)t.getField(i);
                        stringhist.addValue(stringfield.getValue());
                    }
                }
            }
        }catch(DbException e){
            e.printStackTrace();
            System.out.println("faled to process");
        }catch(TransactionAbortedException e){
            e.printStackTrace();
            
            System.out.println("faled to process, TransactionAbortedException");
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return dbfile.numPages()*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(ntups*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if(constant instanceof IntField){
            int value = ((IntField)constant).getValue();
            IntHistogram hist = (IntHistogram)histmap.get(field);
            return hist.estimateSelectivity(op, value);
        }
        else{
            String value = ((StringField)constant).getValue();
            StringHistogram hist = (StringHistogram)histmap.get(field);
            return hist.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
