package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator[] children;
    private TransactionId tid;

    private TupleDesc td;
    private int delRecord;

    private int callrecord;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        children = new DbIterator[1];
        children[0] = child;

        this.tid = t;
        delRecord = 0;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        callrecord = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        for(DbIterator child : children){
            child.open();
            while(child.hasNext()){
                Database.getBufferPool().deleteTuple(tid, child.next());
                delRecord++;
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
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(callrecord != 0) return null;
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(this.delRecord));
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
        this.children = children;
    }

}
