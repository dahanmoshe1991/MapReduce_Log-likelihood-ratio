import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class phaseKey  implements WritableComparable<phaseKey> {

    private Text w1;
    private Text w2;
    private IntWritable decade;

    public phaseKey() {
        w1 = new Text();
        w2 = new Text();
        decade = new IntWritable();
    }

    public phaseKey(Text w1, Text w2,IntWritable decade) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = decade;
    }

    public int gethashCode() {
        return decade.hashCode() + w1.hashCode();
    }//decade.hashCode();

    public Text getw1() {
        return w1;
    }

    public Text getw2() {
        return w2;
    }

    public IntWritable getDecade() {
        return decade;
    }

    @Override
    public String toString() {
        return this.w1.toString() + " " + this.w2.toString() + " " + this.decade.toString();
    }

    @Override
    public int compareTo(phaseKey otherKey) {
        int CompDecades = this.decade.compareTo(otherKey.getDecade());
        //first we sort all kes by decade then by key w1 then by key w2.
        // in both keys:  * is before all other words

        if(CompDecades == 0) {
            boolean isOtherW1Asterisk = otherKey.getw1().toString().equals("*");
            boolean isW1Asterisk = this.w1.toString().equals("*");
            if(isW1Asterisk && !isOtherW1Asterisk) { //<*==w1>
                return -1;
            } else if(isOtherW1Asterisk && !isW1Asterisk){//<w1==*>
                return 1;
            } else {//<*==*> or //<w1=?=w1>
                int compW1 = this.w1.compareTo(otherKey.getw1());
                if( compW1 == 0) {//<w1==w1>
                    boolean isOtherW2Asterisk = otherKey.getw2().toString().equals("*");
                    boolean isW2Asterisk = this.w2.toString().equals("*");

                    if(isW2Asterisk && !isOtherW2Asterisk) {//<*==w2>
                        return -1;
                    } else if(isOtherW2Asterisk && !isW2Asterisk){//<w2==*>
                        return 1;
                    } else {//<*==*> or //<w2=?=w2>
                        return this.w2.compareTo(otherKey.getw2());
                    }
                }
                return compW1;
            }
        }

        else
            return CompDecades;
    }

    @Override
    public void readFields(DataInput data) throws IOException {
        w1.readFields(data);
        w2.readFields(data);
        decade.readFields(data);
    }

    @Override
    public void write(DataOutput data) throws IOException {
        w1.write(data);
        w2.write(data);
        decade.write(data);
    }

}
