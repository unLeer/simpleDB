package simpledb;

import java.util.*;
import java.io.*;


/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    HeapPageId pid;
    TupleDesc td;
    byte header[];
    Tuple tuples[];

    TransactionId lasttid;
    boolean isdirty;

    int numSlots;

    byte[] oldData;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        try{
            // allocate and read the actual records of this page
            tuples = new Tuple[numSlots];
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();
        //System.out.println("empty slots:"+getNumEmptySlots()+"/"+numSlots);

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        int tuplesize = this.td.getSize();
        int Pagesize = BufferPool.PAGE_SIZE;
        int ans =(int) Math.floor((double)(Pagesize*8/(tuplesize*8+1)));
        return ans;

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        // some code goes here
        int ans =(int) Math.ceil((double)getNumTuples()/8);
        return ans;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            return new HeapPage(pid,oldData);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        oldData = getPageData().clone();
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.PAGE_SIZE;
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        int tuplenum = t.getRecordId().tupleno();
        if(!t.getRecordId().getPageId().equals(this.pid) || !isSlotUsed(tuplenum)){
            throw new DbException("tuple is not on this tuple");
        }
        //error, maybe a tuple can be not used while it still can be deleted
        //if(!isSlotUsed(tuplenum)) throw new IllegalArgumentException("tuple is not used and cannot be deleted");
        markSlotUsed(tuplenum, false);
        tuples[tuplenum] = null;
        //if(isSlotUsed(tuplenum)) throw new DbException("fail to delete this tuple");
        
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if(getNumEmptySlots() == 0) throw new DbException("the page is full");
        for(int i=0; i<getNumTuples(); i++){
            if(isSlotUsed(i)) continue;

            markSlotUsed(i,true);
            if(!isSlotUsed(i)) throw new DbException("fail to insert");
            tuples[i] = t;
            t.setRecordId(new RecordId(this.pid, i));
            break;
        }
    }
    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        this.lasttid = dirty ? tid : null;
        this.isdirty = dirty;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        if(this.isdirty) return lasttid;
        else return null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int count = 0;
        for(int i=0; i<this.tuples.length; i++){
            if(!isSlotUsed(i)) count++;
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     * start from 0?
     * the ith slot is 1
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        int headerIndex = i/8;
        int bitIndex = i%8; //大端模式 从右往左写入

        return isOne(header[headerIndex], bitIndex);
    }

    public boolean isOne(byte bite,int bitIndex){
        //I think this part has nothing to do with "big-endian"
        //while the truth doesn't think so
        //return (byte)(bite << bitIndex ) < 0;
        return (byte)(bite << (7-bitIndex)) < 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * false:  from used to not-used
     * true:  from not-used to used
     */
    private void markSlotUsed(int tuplenum, boolean value) {
        // some code goes here
        // not necessary for lab1
        byte head = this.header[tuplenum/8];
        int shift = tuplenum%8;

        if(!value){
            //00010000 ->  11101111 -> & 
            //byte changed =(byte)(head & (~(0b00000001 << shift)));
            byte changed =(byte)(head & (~(1 << shift)));
            this.header[tuplenum/8] = changed;
        }
        else if(value){
            //00010000 -> |
            //byte changed = (byte)(head | (0b00000001 << shift));
            byte changed = (byte)(head | ((byte)1 << shift));
            this.header[tuplenum/8] = changed;
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        //System.out.println("empty slots:"+ getNumEmptySlots()+"/"+numSlots);
        return new TupleIterator();
    }

    private class TupleIterator implements Iterator<Tuple> {
        private   int pos = 0;
        
        @Override
        public boolean hasNext(){
            while(pos < HeapPage.this.numSlots && !isSlotUsed(pos)) pos++;

            return pos <HeapPage.this.numSlots; 
        }

        @Override
        public Tuple next(){
            if(!hasNext()) throw new NoSuchElementException();
            return tuples[pos++];
        }
//        private int index = 0;//tuple数组的下标变化
//        private int usedTuplesNum = getNumTuples() - getNumEmptySlots();
//
//        @Override
//        public boolean hasNext() {
//            return index < getNumTuples() && pos < usedTuplesNum;
//        }
//
//        @Override
//        public Tuple next() {
//            if (!hasNext()) {
//                throw new NoSuchElementException();
//            }
//            for (; !isSlotUsed(index); index++) {
//            }//直到找到在使用的(对应的slot非空的)tuple，再返回
//            pos++;
//            return tuples[index++];
//        }
    }

}
