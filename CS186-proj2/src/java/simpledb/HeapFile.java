package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private TupleDesc td;
    private File file;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.td = td;
        this.file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *tableid 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    //from file, pid, new an instance of Page
    public Page readPage(PageId pid)  {
        // some code goes here
        byte[] buf = new byte[BufferPool.PAGE_SIZE];
        Page page = null;
        try{
            FileInputStream fis = new FileInputStream(this.file);
            int pageNum = pid.pageNumber();
            int count = 0;
            int n;
            while(count != pageNum){
                n = fis.read(buf);
                count++;
            }
            while((n = fis.read(buf)) == -1){
            }

            page = new HeapPage((HeapPageId)pid, buf);
        }catch(IOException e){
            e.printStackTrace();
        }
        return page;        // or use RandomAccessFile

        //use randomAccessFile
//        Page page = null;
//        int pageNum = pid.pageNumber();
//        byte[] data = new byte[BufferPool.PAGE_SIZE];
//        try{
//            RandomAccessFile raf = new RandomAccessFile(this.file,"r");
//
//            raf.seek(pageNum*BufferPool.PAGE_SIZE);
//            raf.read(data);
//
//            page = new HeapPage((HeapPageId)pid, data);
//        }
//        catch(IOException e){
//            e.printStackTrace();
//        }
//        return page;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pagesize = BufferPool.PAGE_SIZE;
        return (int)Math.ceil((double)this.file.length()/pagesize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator {
        int PagePos;
        Iterator<Tuple> pageIterator;
        TransactionId tid;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException{
            PagePos = 0;
            HeapPageId pid = new HeapPageId(getId(), PagePos);
            pageIterator = getPage(pid).iterator();
        }

        private HeapPage getPage(PageId pid) throws TransactionAbortedException, DbException{
            return (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        }


        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            if(pageIterator == null) return false;
            while(PagePos < numPages() && !pageIterator.hasNext()){
                PagePos++;
                if(PagePos == numPages()) break;
                pageIterator = getPage(new HeapPageId(getId(), PagePos)).iterator();
            }
            return PagePos < numPages();
        }

        @Override
        public Tuple next() throws DbException,TransactionAbortedException, NoSuchElementException{
            if(!hasNext()) throw new NoSuchElementException();
            return pageIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException{
            open();
        }

        @Override
        public void close(){
            PagePos = 0;
            pageIterator = null;
        }

    }

}

