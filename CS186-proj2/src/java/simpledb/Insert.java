package simpledb;

import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator[] children;

    private int tableid;
    private TransactionId tid;

    private TupleDesc td;
    private int inserRecord;

    private int callrecord;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
            children = new DbIterator[1];
            children[0] = child;
            this.tid = t;
            this.tableid = tableid;
            this.inserRecord=0;
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            this.callrecord = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        inserRecord = 0;
        BufferPool bp = Database.getBufferPool();
        for(DbIterator child : children){
            child.open();
            while(child.hasNext()){
                    Tuple t = child.next();
                try{
                    bp.insertTuple(this.tid, this.tableid, t);
                    inserRecord++;
                }catch(IOException e){
                    throw new DbException("fail to insert tuple");
                }
            }
            child.close();
        }
        callrecord = 0;
    }

    public void close() {
        // some code goes here
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        super.close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(callrecord != 0) return null;
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(this.inserRecord));
        callrecord++;
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        close();
        this.children = children;
    }
}
