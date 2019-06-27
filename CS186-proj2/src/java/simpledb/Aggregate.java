package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */

    private DbIterator[] children;
    private int aIndex;
    private int gbIndex;
    private Aggregator.Op op;
    private Aggregator aggregator;

    private DbIterator iterator;

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        children = new DbIterator[1];
        children[0] = child;
        this.aIndex = afield;
        this.gbIndex = gfield;
        this.op = aop;

        //考虑到setchildren的可能，可能需要重置aggregator,但又觉得不可能，如果children和child在aggregtor中不保持一致，这没有意义了
        //但是考虑到rewind()功能与setChildren()功能，应该需要能够对aggregator进行清零的操作
        //所以Aggregator的初始化应该在open()中进行
        //通过child的tupleDesc设置aggregator的type
//        if(child.getTupleDesc().getFieldType(aIndex) == Type.INT_TYPE){
//            aggregator = new IntegerAggregator(gbIndex, gbIndex == -1 ? null : child.getTupleDesc().getFieldType(gbIndex), aIndex, op);
//        }
//        else if(child.getTupleDesc().getFieldType(aIndex) == Type.STRING_TYPE){
//            if(op.toString().equals("count")) throw new IllegalArgumentException("have to be Op.CONUT");
//
//            aggregator = new StringAggregator(gbIndex, gbIndex == -1 ? null : child.getTupleDesc().getFieldType(gbIndex), aIndex, op);
//        }
//
//        //设置tupleDesc的fieldname
//        if(gbIndex == -1){
//            aggregator.getTupleDesc().getTDItemAr()[0].fieldName = aggregateFieldName();
//        }
//        else{
//            aggregator.getTupleDesc().getTDItemAr()[1].fieldName = aggregateFieldName();
//            aggregator.getTupleDesc().getTDItemAr()[0].fieldName = groupFieldName();
//            
//        }
    }

    public void initialAggregator(DbIterator child){
        if(child.getTupleDesc().getFieldType(aIndex) == Type.INT_TYPE){
            aggregator = new IntegerAggregator(gbIndex, gbIndex == -1 ? null : child.getTupleDesc().getFieldType(gbIndex), aIndex, op);
        }
        else if(child.getTupleDesc().getFieldType(aIndex) == Type.STRING_TYPE){
            if(!op.toString().equals("count")) throw new IllegalArgumentException("have to be Op.CONUT");

            aggregator = new StringAggregator(gbIndex, gbIndex == -1 ? null : child.getTupleDesc().getFieldType(gbIndex), aIndex, op);
        }

        //设置tupleDesc的fieldname
        if(gbIndex == -1){
            aggregator.getTupleDesc().getTDItemAr()[0].fieldName = aggregateFieldName();
        }
        else{
            aggregator.getTupleDesc().getTDItemAr()[1].fieldName = aggregateFieldName();
            aggregator.getTupleDesc().getTDItemAr()[0].fieldName = groupFieldName();
            
        }

    }


    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        if(gbIndex == -1) return Aggregator.NO_GROUPING;
        return gbIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        if(gbIndex == -1) return null;
        return children[0].getTupleDesc().getFieldName(gbIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return aIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return children[0].getTupleDesc().getFieldName(aIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
            // some code goes here
            super.open();
            initialAggregator(children[0]);
            for(DbIterator di : children){
                di.open();
                while(di.hasNext()){
                    aggregator.mergeTupleIntoGroup(di.next());
                }
            }
            iterator = aggregator.iterator();
            iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(iterator.hasNext()) return iterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        iterator = null;
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        if(aggregator == null){
            initialAggregator(children[0]);
        }
        return aggregator.getTupleDesc();
    }

    public void close() {
	// some code goes here
        aggregator = null;
        iterator = null;
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        this.children = children;
    }
    
}
