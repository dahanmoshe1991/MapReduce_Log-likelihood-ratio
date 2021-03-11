import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//********************************************************************
//                      phaseTwoValue
//********************************************************************


public class phaseValue implements WritableComparable<phaseValue>{

    private LongWritable cw1;
    private LongWritable cw2;
    private LongWritable cw1w2;
    private IntWritable N;


    public phaseValue() {
        cw1w2 = new LongWritable();
        cw1 = new LongWritable();
        cw2 = new LongWritable();
        N = new IntWritable();
    }

    public phaseValue(LongWritable cw1, LongWritable cw2 , LongWritable cw1w2, IntWritable N) {
        this.cw1 =cw1;
        this.cw2 = cw2;
        this.cw1w2 = cw1w2;
        this.N = N;
    }


    @Override
    public void write(DataOutput data) throws IOException {
        cw1w2.write(data);
        cw1.write(data);
        cw2.write(data);
        N.write(data);

    }

    @Override
    public void readFields(DataInput data) throws IOException {
        cw1w2.readFields(data);
        cw1.readFields(data);
        cw2.readFields(data);
        N.readFields(data);

    }

    @Override
    public int compareTo(phaseValue otherValue) {
        //first compare value of coupleCount then sort by number of occurrences
        int isCw1w2Same = this.cw1w2.compareTo(otherValue.getCw1w2());
        if( isCw1w2Same != 0) {
            return isCw1w2Same;
        }
        return this.cw1.compareTo(otherValue.getCw1());
    }

    public LongWritable getCw1w2() {
        return cw1w2;
    }

    public LongWritable getCw1() {
        return cw1;
    }

    public LongWritable getCw2() {
        return cw2;
    }

    public IntWritable getN() {
        return N;
    }

    public void setCw1(LongWritable cw1) {
        this.cw1 = cw1;
    }

    public void setCw1w2(LongWritable cw1w2) {
        this.cw1w2 = cw1w2;
    }

    public void setCw2(LongWritable cw2) {
        this.cw2 = cw2;
    }

    public void setN(IntWritable n) {
        N = n;
    }

    public String toString() {
        return  this.cw1.toString()+" " + this.cw2.toString()+" " +this.cw1w2.toString() + " "+ this.N.toString();
    }

}
