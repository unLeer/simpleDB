package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field, Integer> gbmap;
    //when op is "avg"
    private HashMap<Field, Integer> assistCountmap;
    private HashMap<Field, Integer> assistSummap;

    private TupleDesc td;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     *            分类的field
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     *            统计的field
     * @param what
     *            the aggregation operator
     */
    public TupleDesc getTupleDesc(){
        return td;
    }

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        gbmap = new HashMap<Field, Integer>();
        if(what.toString().equals("avg")){
            //辅助map
            assistCountmap = new HashMap<Field, Integer>();
            assistSummap = new HashMap<Field, Integer>();
        }
        if(gbfieldtype == null){
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            td = new TupleDesc(typeAr);
        }
        else{
            Type[] typeAr = new Type[2];
            typeAr[0] = gbfieldtype;
            typeAr[1] = Type.INT_TYPE;
            td = new TupleDesc(typeAr);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        if(gbfield == Aggregator.NO_GROUPING) key = null;
        else  key = tup.getField(gbfield);

        int value = ((IntField)tup.getField(afield)).getValue();
        if(gbmap.containsKey(key)){
            int formervalue = gbmap.get(key);
            gbmap.put(key, CalculateOp(value, formervalue, key));
        }
        else{
            if(op.toString().equals("min")){
                gbmap.put(key, CalculateOp(value, Integer.MAX_VALUE,  key));
            }
            else if(op.toString().equals("max")){

                gbmap.put(key, CalculateOp(value, Integer.MIN_VALUE,  key));
            }
            else{
                gbmap.put(key, CalculateOp(value, 0,  key));
            }
        }
    }

    public int CalculateOp(int value, int formervalue, Field key){
        if(op.toString().equals("min")){
            return Math.min(value, formervalue);
        }
        if(op.toString().equals("max")){
            return Math.max(value, formervalue);
        }
        if(op.toString().equals("sum")){
            return value+formervalue;
        }
        if(op.toString().equals("avg")){
            if(!assistCountmap.containsKey(key)){
                assistCountmap.put(key,1);
                assistSummap.put(key,value);
                return value;
            }
            else{
                int newcount = assistCountmap.get(key)+1;
                int newsum = assistSummap.get(key)+value;
                assistCountmap.put(key, newcount);
                assistSummap.put(key,newsum);
                return newsum/newcount;
            }
        }
        if(op.toString().equals("count")){
            return formervalue+1;
        }

        throw new IllegalStateException("impossible to reach here");
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new IntegerAggregatorIterator();
    }

    public class IntegerAggregatorIterator implements DbIterator{
        private Iterator<Map.Entry<Field, Integer>> iter;
        @Override
        public void open()
            throws DbException, TransactionAbortedException{
            iter = gbmap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if(!hasNext()) throw new NoSuchElementException();
            Tuple ans = new Tuple(getTupleDesc());
            Map.Entry<Field,Integer> entry = iter.next();

            if(gbfield == Aggregator.NO_GROUPING){
                ans.setField(0,new IntField(entry.getValue()));
                return ans;
            }
            else{
                ans.setField(0, entry.getKey());
                ans.setField(1, new IntField(entry.getValue()));
                return ans;
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException{
            open();
        }


        @Override
        public TupleDesc getTupleDesc(){
            return IntegerAggregator.this.getTupleDesc();
        }

        @Override
        public void close(){
            iter = null;
        }

    }

}
