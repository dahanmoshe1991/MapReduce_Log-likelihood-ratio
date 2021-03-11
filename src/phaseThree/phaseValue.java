import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//********************************************************************
//                      phaseTwoValue
//********************************************************************


public class phaseValue implements WritableComparable<phaseValue>{

    private DoubleWritable cw1;
    private DoubleWritable cw2;
    private DoubleWritable cw1w2;
    private DoubleWritable N;
    private DoubleWritable logLikelihood;


    public phaseValue() {
        cw1w2 = new DoubleWritable();
        cw1 = new DoubleWritable();
        cw2 = new DoubleWritable();
        N = new DoubleWritable();
        logLikelihood = new DoubleWritable();
    }

    public phaseValue(DoubleWritable cw1, DoubleWritable cw2 , DoubleWritable cw1w2, DoubleWritable N) {
        this.cw1 =cw1;
        this.cw2 = cw2;
        this.cw1w2 = cw1w2;
        this.N = N;
        this.logLikelihood = new DoubleWritable();
    }

    public phaseValue(double cw1, double cw2 , double cw1w2, double N) {
        this.cw1 =new DoubleWritable(cw1);
        this.cw2 = new DoubleWritable(cw2);
        this.cw1w2 = new DoubleWritable(cw1w2);
        this.N = new DoubleWritable(N);
        this.logLikelihood = new DoubleWritable();
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

    public DoubleWritable getCw1w2() {
        return cw1w2;
    }

    public DoubleWritable getCw1() {
        return cw1;
    }

    public DoubleWritable getCw2() {
        return cw2;
    }

    public DoubleWritable getN() {
        return N;
    }

    public void setCw1(DoubleWritable cw1) {
        this.cw1 = cw1;
    }

    public void setCw1w2(DoubleWritable cw1w2) {
        this.cw1w2 = cw1w2;
    }

    public void setCw2(DoubleWritable cw2) {
        this.cw2 = cw2;
    }

    public void setN(DoubleWritable n) {
        N = n;
    }

    public DoubleWritable getLogLikelihood() {
        return logLikelihood;
    }

    public void setLogLikelihood(double logLikelihood) {
        this.logLikelihood = new DoubleWritable(logLikelihood);
    }

    public String toString() {
        return  this.cw1.toString()+" " + this.cw2.toString()+" " +this.cw1w2.toString() + " "+ this.N.toString() + " " +this.logLikelihood.toString();
    }

    public String LogOutputString(){
        return "LogLikelihood is: "+ logLikelihood.toString();
    }

}
