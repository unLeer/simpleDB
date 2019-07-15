package simpledb;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    private int buckets;
    private int min;
    private int max;
    private int[] histogram;
    private int width;
    private int ntups;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        histogram = new int[buckets];

        width =(int)Math.ceil( (double)(max-min + 1)/buckets); //including min&max
        Arrays.fill(histogram, 0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int num = (v - min)/width;
        histogram[num]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int bucketno;
        if(v>=min && v<=max) {
            bucketno = (v-min)/width;
        }
        else{
            bucketno = 0;
        }
        double tempbucket;
        int left = min + bucketno*width;
        int right = (left + width - 1) <= max ? left+width-1 : max;
        int height_b = histogram[bucketno];

        switch(op){
            case EQUALS:
                if(v>max || v<min) return 0.0;
               double interest = (double)height_b/(right-left+1);
               return (double)interest/ntups;

            case GREATER_THAN:
               if(v>max) return 0.0;
               if(v<min) return 1.0;
               tempbucket = (double)height_b*(right-v)/(right-left+1);

               //System.out.println(left+","+right+","+height_b+","+tempbucket);

               if (bucketno == buckets-1) return (double)tempbucket/ntups;

               for(int i = bucketno+1; i<buckets; i++) {
                   tempbucket += (double)histogram[i];
               }
               return (double)tempbucket/ntups;

            case LESS_THAN:
               if(v>max) return 1.0;
               if(v<min) return 0.0;

               tempbucket = (double)height_b*(v-left)/(right-left+1);
               if (bucketno == 0) return (double)tempbucket/ntups;

               for(int i = bucketno-1; i>=0; i--){
                   tempbucket += (double)histogram[i];
               }
               return (double)tempbucket/ntups;
            case LESS_THAN_OR_EQ:
               return estimateSelectivity(Predicate.Op.LESS_THAN,v)
                   +estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
               return estimateSelectivity(Predicate.Op.GREATER_THAN, v)
                   +estimateSelectivity(Predicate.Op.EQUALS, v);

            case LIKE:
               return 1.0;
            case NOT_EQUALS:
               return (double)1-estimateSelectivity(Predicate.Op.EQUALS, v);

            default:
               throw new RuntimeException("should not reach here");

        }

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        String ans= "";
        for(int i = 0; i<buckets; i++){
            ans += histogram[i];
            ans += ",";
        }
        return ans;
    }
}
