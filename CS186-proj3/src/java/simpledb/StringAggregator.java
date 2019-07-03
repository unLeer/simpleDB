package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 * only available for Op.COUNT
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field, Integer> gbmap;

    private TupleDesc td;

    @Override
    public TupleDesc getTupleDesc(){
        return td;
    }
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(!what.toString().equals("count")) throw new IllegalArgumentException("have to be Op.COUNT");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        gbmap = new HashMap<Field, Integer>();

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
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here Field key;
        Field key;
        if(gbfield == Aggregator.NO_GROUPING) key = null;
        else  key = tup.getField(gbfield);

        if(gbmap.containsKey(key)){
            gbmap.put(key, gbmap.get(key)+1);
        }
        else{
            gbmap.put(key, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator();
    }

    public class StringAggregatorIterator implements DbIterator{
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
            return StringAggregator.this.getTupleDesc();
        }

        @Override
        public void close(){
            iter = null;
        }

    }


}
