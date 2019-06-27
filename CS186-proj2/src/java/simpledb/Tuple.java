package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private Field[] fields;
    private TupleDesc td;
    private RecordId rid;

    public Field[] getFields(){
        return this.fields == null ? null : this.fields;
    }


    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        if(td == null || td.getFieldnums() < 1) throw new IllegalArgumentException("invalid TupleDesc td");
        this.td = td;
        this.fields = new Field[this.td.getFieldnums()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid == null ? null : this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        if(rid == null) throw new IllegalArgumentException("invalid RecordId rid");
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(f == null   || i<0 || i>= this.td.getFieldnums()) throw new IllegalArgumentException("invalid field value f");
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if( i<0 || i>= this.td.getFieldnums()) throw new IllegalArgumentException("invalid Index i");
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        // throw new UnsupportedOperationException("Implement this");
        StringBuffer stringbuffer = new StringBuffer();
        for(int i = 0; i<this.fields.length; i++){
            if(i == this.fields.length - 1){
                stringbuffer.append(this.fields[i].toString() + "\n");
            }else{
                stringbuffer.append(this.fields[i].toString() + "\t");
            }
        }

        return stringbuffer.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new FieldIterator();
    }

    public class FieldIterator implements Iterator<Field>{
        private int pos = 0;

        @Override
        public boolean hasNext(){
            return pos<fields.length;
        }

        @Override 
        public Field next(){
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            return fields[pos++];
        }
    }

}
