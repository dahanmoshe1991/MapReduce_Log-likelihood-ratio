import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class phaseThree {


    public static void main(String[] args) throws Exception {
        System.out.println("********PhaseThree started Running**********\n");

        String appId = args[0];
        String lang = args[1];
//       input
        String inputPhaseOnePath = "input_1_gram";//"s3n://dsps202assignment2/" + appId + "/results/outputPhase1";//"input_1_gram";
        String inputPhaseTwoPath = "input_2_gram";//"s3n://dsps202assignment2/" + appId + "/results/outputPhase2"; //"input_2_gram";
//       output
        String outputPhaseThree = "out3";//s3n://dsps202assignment2/" + appId + "/results/outputPhase3";//UUID.randomUUID().toString().replace('\\', '_').replace('/', '_').replace(':', '_');//" s3n://dsps202assignment2/" + appId + "/results/outputPhase3";

        Configuration conf = new Configuration();
        conf.set("lang", lang);
        Job job = Job.getInstance(conf, "thirdJob");
        job.setJarByClass(phaseThree.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(phaseKey.class);
        job.setMapOutputValueClass(phaseValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPhaseOnePath));
        FileInputFormat.addInputPath(job, new Path(inputPhaseTwoPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPhaseThree));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        while (true) ;
    }


    public static class MapperClass extends Mapper<LongWritable, Text, phaseKey, phaseValue> {
        public Text ASTRESIK = new Text("*");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] keyFields = keyValue[0].split(" ");
            String[] valueFields = keyValue[1].split(" ");
            phaseValue newValue = new phaseValue(new DoubleWritable(Long.parseLong(valueFields[0]))
                    ,new DoubleWritable(Long.parseLong(valueFields[1]))
                    ,new DoubleWritable(Long.parseLong(valueFields[2]))
                    ,new DoubleWritable(Long.parseLong(valueFields[3])));

            if (!keyFields[1].equals(ASTRESIK.toString())) {//<w1,w2>
                phaseKey reversedKey = new phaseKey(keyFields[1], keyFields[0], keyFields[2]);
                context.write(reversedKey, newValue);
            } else {

                phaseKey newKey = new phaseKey(keyFields[0], keyFields[1], keyFields[2]);
                context.write(newKey, newValue);//<w1,*>
            }
        }
    }


    public static class PartitionerClass extends Partitioner<phaseKey, phaseValue> {

        @Override
        public int getPartition(phaseKey key, phaseValue value, int numPartitions) {
            return Math.abs(key.getDecade().hashCode()) % numPartitions;
        }

    }


    public static class CombinerClass extends Reducer<phaseKey, phaseValue, phaseKey, phaseValue> {

        @Override
        public void reduce(phaseKey key, Iterable<phaseValue> values, Context context) throws IOException, InterruptedException {
            double cw1 = 0;
            double cw2 = 0;
            double cw1w2 = 0;
            double N = 0;
            for (phaseValue value : values) {
                cw1 += value.getCw1().get();
                cw2 += value.getCw2().get();
                cw1w2 += value.getCw1w2().get();
                N += value.getN().get();
            }
            context.write(key, new phaseValue(cw1, cw2, cw1w2, N));
        }
    }

    public static class ReducerClass extends Reducer<phaseKey, phaseValue, Text, Text> {
        public Text ASTRESIK = new Text("*");
        private DoubleWritable cw2 = new DoubleWritable();


        @Override
        public void reduce(phaseKey key, Iterable<phaseValue> values, Context context) throws IOException, InterruptedException {


            long lCw1 = 0;
            long lCw2 = 0;
            long lCw1w2 = 0;
            long lN = 0;
            for (phaseValue value : values) {
                lCw1 += value.getCw1().get();
                lCw2 += value.getCw2().get();
                lCw1w2 += value.getCw1w2().get();
                lN += value.getN().get();
            }


            if (key.getw2().equals(ASTRESIK) && !key.getw1().equals(ASTRESIK)) {//<w2,*>
                cw2 = new DoubleWritable(lCw1);
            } else {//<w2,w1>
                phaseKey originalKey = new phaseKey(key.getw2(), key.getw1(), key.getDecade());
                phaseValue newValue = new phaseValue(new DoubleWritable(lCw1)
                        ,new DoubleWritable(cw2.get())
                        ,new DoubleWritable(lCw1w2)
                        ,new DoubleWritable(lN));

                double logLikelihood = likelihoodRatio(newValue.getCw1().get(),
                        newValue.getCw2().get(),
                        newValue.getCw1w2().get(),
                        newValue.getN().get());
                newValue.setLogLikelihood(logLikelihood);
                Text keyText = new Text(originalKey.toString());
                Text valueText = new Text(newValue.LogOutputString());


                context.write(keyText, valueText);
            }

        }

        private double likelihoodRatio(double cw1, double cw2, double cw1w2, double N) {
            double p = adjustProbValues(cw2 / N);
            double p1 = adjustProbValues(cw1w2 / cw1);
            double p2 = adjustProbValues((cw2 - cw1w2) / (N - cw1));
            double logLamda = Math.log(L_Operator(cw1w2, cw1, p)) +
                    Math.log(L_Operator(cw2 - cw1w2, N - cw1, p)) -
                    Math.log(L_Operator(cw1w2, cw1, p1)) -
                    Math.log(L_Operator(cw2 - cw1w2, N - cw1, p2));

            return logLamda;

        }

        private double adjustProbValues(double P) {
            if (P == 0.0)
                return 0.01;
            if (P == 1.0)
                return 0.99;
            return  P;
        }

        private double L_Operator(double k, double n, double x) {
            return Math.pow(x, k) * Math.pow(1 - x, n - k);
        }




    }


}
