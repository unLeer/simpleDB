package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator[] children;
    private JoinPredicate jp;

    //blocknestedloopJoin将表1存入内存，考虑到内存大小有限制可能存不下，取131072的缓冲区大小
    public static final int blockMemory = 131071;


    private Tuple outerflag;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.jp = p;
        children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return null;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return null;
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc temp = children[0].getTupleDesc();
        for(int i=1;i<children.length;i++){
            temp = TupleDesc.merge(temp, children[i].getTupleDesc());
        }
        return temp;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
                super.open();
                for(DbIterator di : children){
                    di.open();
                }
                outerflag = children[0].hasNext() ? children[0].next() : null;
    }

    public void close() {
        // some code goes here
        for(DbIterator di : children){
            di.close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
         for(DbIterator di : children){
            di.rewind();
        }
        outerflag = children[0].hasNext() ? children[0].next() : null;
   }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     *对于JoinPredicate中已经指定的column满足条件的，将两个tuple并列copy输出
     外层循环应该较小

     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(outerflag == null && !children[0].hasNext()) return null;
        Tuple t;
        while(outerflag != null){
            while(children[1].hasNext()){
                if(jp.filter(outerflag, t = children[1].next())){
                   return  mergeTuples(outerflag, t);
                }
            }
            if(children[0].hasNext()) outerflag = children[0].next();
            else outerflag = null;
            children[1].rewind();
        }
        return null;
    }

    
    public Tuple mergeTuples(Tuple t1, Tuple t2){
        Tuple ans = new Tuple(getTupleDesc());
        for(int i=0; i<ans.getTupleDesc().numFields(); i++){
            if(i<t1.getFields().length) ans.setField(i,t1.getField(i));
            else{
                ans.setField(i, t2.getField(i-(t1.getFields().length)));
            }
        }
        return ans;
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
