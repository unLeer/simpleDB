package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private int Fieldnums;
    private TDItem[] TDItemAr;

    public int getFieldnums(){
        return Fieldnums;
    }

    public TDItem[] getTDItemAr(){
        return TDItemAr;
    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public Type fieldType;
        
        /**
         * The name of the field
         * */
        public String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        @Override 
        public boolean equals(Object o){
            if(this == o) return true;
            if(o instanceof TDItem){
                TDItem target = (TDItem) o;
                boolean namebool = (target.fieldName == null && this.fieldName == null) || (target.fieldName != null && target.fieldName.equals(this.fieldName));
                boolean typebool = target.fieldType.equals(this.fieldType);
                return namebool && typebool;
            }
            else return false;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemIterator();
    }

    public class TDItemIterator implements Iterator<TDItem>{
        private int pos = 0;

       @Override 
       public boolean hasNext(){
           return pos<Fieldnums; 
       }

       @Override 
       public TDItem next(){
           if(!hasNext()){
               throw new  NoSuchElementException();
           }
           return TDItemAr[pos++];
       }
    } 

    private static final long serialVersionUID = 1L;

    public TupleDesc(TDItem[] tditemar){
        if(tditemar.length == 0 || tditemar == null) throw new IllegalArgumentException("至少要有一个元素");

        this.Fieldnums = tditemar.length;
        this.TDItemAr = tditemar;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if(typeAr.length == 0){
            throw new IllegalArgumentException("类型数组至少包含一个元素");
        }
        if(typeAr.length != fieldAr.length){
            throw new IllegalArgumentException("类型数组与域数组长度应该相同");
        }


        this.Fieldnums = typeAr.length;
        this.TDItemAr = new TDItem[Fieldnums];
        for(int i = 0; i<Fieldnums; i++){
            TDItemAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return Fieldnums;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>=this.Fieldnums){
            throw new NoSuchElementException();
        }
        return TDItemAr[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>=this.Fieldnums){
            throw new NoSuchElementException();
        }
        return TDItemAr[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null) throw new NoSuchElementException();
        for(int i = 0; i<Fieldnums; i++){
            if(TDItemAr[i].fieldName.equals(name)) return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(TDItem td : TDItemAr){
            size+=td.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TDItem[] ar1 = td1.getTDItemAr();
        TDItem[] ar2 = td2.getTDItemAr();

        TDItem[] target = new TDItem[td1.getFieldnums()+td2.getFieldnums()];
        System.arraycopy(ar1,0,target,0,ar1.length);
        System.arraycopy(ar2,0,target,ar1.length,ar2.length);

        return new TupleDesc(target);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(this == o) return true;
        if(!( o instanceof TupleDesc)) return false;

        
        TupleDesc target = (TupleDesc) o;
        if(this.Fieldnums != target.getFieldnums()) return false;
        for(int i = 0; i<this.Fieldnums; i++){
            if(!this.TDItemAr[i].equals(target.getTDItemAr()[i])) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String ans = new String();
        for(TDItem td : this.TDItemAr){
            ans+= td.toString() + ", ";
        }
        ans += this.Fieldnums + " in total";

        return ans;
    }
}
