import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class phaseOne {

    public static class MapperClass extends Mapper<LongWritable, Text, phaseKey, LongWritable> {
        public static ArrayList<String> StopWords;
        public ArrayList<K_L> K_L_toWrite;
        public Text ASTERISK = new Text("*");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String lang = context.getConfiguration().get("lang");
            StopWords = createStopWords(lang);
            K_L_toWrite = new ArrayList<K_L>();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] attributesArray = value.toString().split("\t");
            String[] oneGram = attributesArray[0].split(" ");
            if (oneGram.length != 1) return;
            String lang = context.getConfiguration().get("lang");
            String cleanedWord = null;
            if (lang.equals("eng")) {
                cleanedWord = oneGram[0].replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                if (cleanedWord == null || cleanedWord.equals("") || StopWords.contains(cleanedWord))
                    return;
            } else if (lang.equals("heb")) {
                cleanedWord = oneGram[0].replaceAll("[^א-ת]", "");
                if (cleanedWord == null || cleanedWord.equals("") || StopWords.contains(cleanedWord))
                    return;
            }

            Text w1 = new Text(cleanedWord);
            LongWritable occurrences = new LongWritable(Long.parseLong(attributesArray[2]));
            IntWritable decade = new IntWritable(((int) (Integer.parseInt(attributesArray[1]) / 10)) * 10);

            if (K_L_toWrite.size() < 5000) {
                K_L_toWrite.add(new K_L(new phaseKey(w1,ASTERISK ,decade), occurrences));
            } else {
                //write and clean
                Text ASTERISK = new Text("*");
                for (K_L k_l : K_L_toWrite) {
                    context.write(k_l.getPhaseKey(), k_l.getPhaseLong());
                    context.write(new phaseKey(ASTERISK,ASTERISK, k_l.getPhaseKey().getDecade()), k_l.getPhaseLong());
                }

                K_L_toWrite.clear();
//                write current K_L
                K_L_toWrite.add(new K_L(new phaseKey(w1, ASTERISK,decade), occurrences));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!K_L_toWrite.isEmpty()) {
                Text ASTERISK = new Text("*");
                for (K_L k_l : K_L_toWrite) {
                    context.write(k_l.getPhaseKey(), k_l.getPhaseLong());
                    context.write(new phaseKey(ASTERISK,ASTERISK, k_l.getPhaseKey().getDecade()), k_l.getPhaseLong());
                }

                K_L_toWrite.clear();
            }

            super.cleanup(context);
        }

        public static ArrayList<String> createStopWords(String lang) {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

            S3Object fullObject;
            if (lang.equals("eng")) {
                fullObject = s3Client.getObject(new GetObjectRequest("dsps202assignment2/StopWords", "eng-stopWords.txt"));
            } else {
                fullObject = s3Client.getObject(new GetObjectRequest("dsps202assignment2/StopWords", "heb-stopWords.txt"));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
            String line = null;
            ArrayList<String> list = new ArrayList<String>();
            while (true) {
                try {
                    if (!((line = reader.readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                list.add(line);
            }

            return list;
        }
    }


    public static class K_L { //for mapper side aggregation before writing to context
        private phaseKey phaseKey;
        private LongWritable phaseLong;


        public K_L(phaseKey key, LongWritable value) {
            this.phaseKey = key;
            this.phaseLong = value;
        }

        public phaseKey getPhaseKey() {
            return phaseKey;
        }

        public LongWritable getPhaseLong() {
            return phaseLong;
        }
    }


    public static class K_V { //for reducer side aggregation before writing to context
        private phaseKey phaseKey;
        private phaseValue phaseValue;


        public K_V(phaseKey key, phaseValue value) {
            this.phaseKey = key;
            this.phaseValue = value;
        }

        public phaseKey getPhaseKey() {
            return phaseKey;
        }

        public phaseValue getPhaseValue() {
            return phaseValue;
        }
    }

    public static class CombinerClass extends Reducer<phaseKey, LongWritable, phaseKey, LongWritable> {
        private LongWritable occurrences = new LongWritable();

        @Override
        public void reduce(phaseKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }
            occurrences.set(counter);
            context.write(key, occurrences);
        }
    }

    public static class PartitionerClass extends Partitioner<phaseKey, LongWritable> {

        @Override
        public int getPartition(phaseKey key, LongWritable value, int numPartitions) {
            return Math.abs(key.gethashCode()) % numPartitions;
        }

    }


    public static class ReducerClass extends Reducer<phaseKey, LongWritable, phaseKey, phaseValue> {
        public ArrayList<K_V> K_V_toWrite;
        public Text ASTERISK = new Text("*");
        public HashMap<IntWritable, LongWritable> N_ToWrite;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            K_V_toWrite = new ArrayList<K_V>();
            N_ToWrite = new HashMap<IntWritable, LongWritable>();
        }

        @Override
        public void reduce(phaseKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }
//            occurrences.set(counter);

            Text w1 = new Text(key.getw1().toString());
            LongWritable cw1 = new LongWritable(counter);
            IntWritable decade = new IntWritable(key.getDecade().get());
            phaseKey newKey = new phaseKey(w1,ASTERISK ,decade);
            phaseValue stpVal = new phaseValue(); //start a default Phase value

            if (key.getw1().equals(ASTERISK)) { // calc N for a decade
                N_ToWrite.put(decade, cw1);
            } else {

                stpVal.setCw1(cw1);//update cw1 when <w1>

                if (K_V_toWrite.size() >= 5000) {
                    //write and clean
                    for (K_V k_v : K_V_toWrite) {
                        context.write(k_v.getPhaseKey(), k_v.getPhaseValue());
                    }
                    K_V_toWrite.clear();
                    //add current K-V
                }
                K_V_toWrite.add(new K_V(newKey, stpVal));
            }
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!N_ToWrite.isEmpty()) {//create NFile listing all W1 for decades and upload to S3
                String NPath = context.getConfiguration().get("NPath");
                String Name = UUID.randomUUID().toString().replace('\\', '_').replace('/', '_').replace(':', '_');
                File NFile = new File(Name);
                try {
                    FileWriter myWriter = new FileWriter(NFile);
                    for (IntWritable dec : N_ToWrite.keySet()) {
                        myWriter.write(dec.toString() + " " + N_ToWrite.get(dec).toString() +"\n");
                    }

                    myWriter.close();
                    AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
                    InputStream is = new FileInputStream(NFile);
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(NFile.length());
                    PutObjectRequest req = new PutObjectRequest(NPath, Name, is, metadata);
                    s3Client.putObject(req);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (SdkClientException e) {
                    e.printStackTrace();
                }
            }

            if (!K_V_toWrite.isEmpty()) {//write leftover K-V
                for (K_V k_v : K_V_toWrite) {
                    context.write(k_v.getPhaseKey(), k_v.getPhaseValue());
                }
                K_V_toWrite.clear();
            }

            super.cleanup(context);

        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("********PhaseOne started Running**********\n");
        String inputFilePath = args[0];
        String appId = args[1];//UUID.randomUUID().toString().replace('\\', '_').replace('/', '_').replace(':', '_');
        String lang = args[2];
        System.out.println(inputFilePath);
        System.out.println(appId);
        String output ="s3n://dsps202assignment2/" + appId + "/results/outputPhase1";
        String NPath = "dsps202assignment2/" + appId + "/N";
        Configuration conf = new Configuration();
        conf.set("NPath", NPath);
        conf.set("lang", lang);
        Job job = Job.getInstance(conf, "phaseOne");
        job.setJarByClass(phaseOne.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(phaseKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(phaseKey.class);
        job.setOutputValueClass(phaseKey.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);//SequenceFileInputFormat
        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.out.println("********PhaseOne wait for completion**********\n");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("********PhaseOne exit**********\n");
        while (true) ;
    }


}




