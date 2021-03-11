import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;


public class phaseTwo {


    public static class MapperClass extends Mapper<LongWritable, Text, phaseKey, LongWritable> {
        private Text ASTERISK = new Text("*");
        public ArrayList<K_L2> K_L_toWrite;
        public static ArrayList<String> StopWords = new ArrayList<String>();
        public Text w1;
        public Text w2;
        public IntWritable decade;
        public LongWritable occurrences;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String lang = context.getConfiguration().get("lang");
            if (StopWords.isEmpty())
                StopWords = createStopWords(lang);
            K_L_toWrite = new ArrayList<K_L2>();

        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] attributesArray = value.toString().split("\t");
            String[] Gram = attributesArray[0].split(" ");
            if (Gram.length == 2) {//input from bigram

                //check and set w1
                String lang = context.getConfiguration().get("lang");
                String cleanedWord = null;
             //   if (!Gram[0].equals("*")) {
                    if (lang.equals("eng")) {
                        cleanedWord = Gram[0].replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (cleanedWord.equals("") || StopWords.contains(cleanedWord))
                            return;
                    } else if (lang.equals("heb")) {
                        cleanedWord = Gram[0].replaceAll("[^א-ת]", "");
                        if (cleanedWord.equals("") || StopWords.contains(cleanedWord))
                            return;
                    }
//                } else {
//                    cleanedWord = ASTERISK.toString();
//                }
//            w1.set(cleanedWord);
                w1 = new Text(cleanedWord);

                //check and set w2
               // if (!Gram[1].equals("*")) {
                    if (lang.equals("eng")) {
                        cleanedWord = Gram[1].replaceAll("[^A-Za-z0-9]", "").toLowerCase();
                        if (cleanedWord.equals("") || StopWords.contains(cleanedWord))
                            return;
                    } else if (lang.equals("heb")) {
                        cleanedWord = Gram[1].replaceAll("[^א-ת]", "");
                        if (cleanedWord.equals("") || StopWords.contains(cleanedWord))
                            return;
                    }
//                } else {
//                    cleanedWord = ASTERISK.toString();
//                }

                w2 = new Text(cleanedWord);
                decade = new IntWritable(((int) (Integer.parseInt(attributesArray[1]) / 10)) * 10);
                occurrences = new LongWritable(Long.parseLong(attributesArray[2]));


            } else if (Gram.length == 3) {//input from unigram

                String[] uniValues = attributesArray[1].split(" ");
                w1 = new Text(Gram[0]);
                w2 = new Text(Gram[1]);
                decade = new IntWritable(((int) (Integer.parseInt(Gram[2]) / 10)) * 10);
                occurrences = new LongWritable(Long.parseLong(uniValues[0]));

            }



            if (K_L_toWrite.size() < 5000) {
                K_L_toWrite.add(new K_L2(new phaseKey(w1, w2, decade), occurrences));
            } else {
                //write and clean
                for (K_L2 k_l : K_L_toWrite) {
                    if (!k_l.isNull())
                        context.write(k_l.getPhaseKey(), k_l.getPhaseLong());//<w1,w2,decade>
                }

                K_L_toWrite.clear();
//                write current K_L
                K_L_toWrite.add(new K_L2(new phaseKey(w1, w2, decade), occurrences));
            }

        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!K_L_toWrite.isEmpty()) {
                for (K_L2 k_l : K_L_toWrite) {
                    if (!k_l.isNull())
                        context.write(k_l.getPhaseKey(), k_l.getPhaseLong());//<w1,w2,decade>
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


    public static class K_L2 { //for mapper side aggregation before writing to context
        private phaseKey phaseKey;
        private LongWritable phaseLong;


        public K_L2(phaseKey key, LongWritable value) {
            this.phaseKey = key;
            this.phaseLong = value;
        }


        public phaseKey getPhaseKey() {
            return phaseKey;
        }

        public LongWritable getPhaseLong() {
            return phaseLong;
        }

        public boolean isNull(){
            return phaseKey.checkNull();
        }
    }

    public static class PartitionerClass extends Partitioner<phaseKey, LongWritable> {

        @Override
        public int getPartition(phaseKey key, LongWritable value, int numPartitions) {
            int val = 1;
            try {
                val = Math.abs(key.gethashCode()) % numPartitions;
            } catch (Exception e) {
                System.out.println("***Error in key: " + key.toString());
            }

            return val;
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
            this.occurrences.set(counter);
            context.write(key, this.occurrences);
        }
    }


    public static class K_V2 { //for reducer side aggregation before writing to context
        private phaseKey phaseKey;
        private phaseValue phaseValue;


        public K_V2(phaseKey key, phaseValue value) {
            this.phaseKey = key;
            this.phaseValue = value;
        }

        public phaseKey getPhaseTwoKey() {
            return phaseKey;
        }

        public phaseValue getPhaseTwoValue() {
            return phaseValue;
        }
    }

    public static class ReducerClass extends Reducer<phaseKey, LongWritable, phaseKey, phaseValue> {

        private LongWritable cw1 = new LongWritable();
        private LongWritable N = new LongWritable();
        private HashMap<Integer, LongWritable> DecadeN_Map = new HashMap<Integer, LongWritable>();
        private Text ASTERISK = new Text("*");
        public ArrayList<K_V2> K_V_toWrite;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            K_V_toWrite = new ArrayList<K_V2>();
            //reading N content and add it to HashMap
            String NPath = context.getConfiguration().get("NPath");
            getNMap(NPath);


        }


        public void getNMap(String NPath) {

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
            ListObjectsRequest req = new ListObjectsRequest().withBucketName("dsps202assignment2").withPrefix(NPath);
            ObjectListing listing = s3Client.listObjects(req);
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();
            while (listing.isTruncated()) {
                listing = s3Client.listNextBatchOfObjects(listing);
                summaries.addAll(listing.getObjectSummaries());
            }
//            do {
            for (S3ObjectSummary summary : summaries) {
                S3Object fullObject = s3Client.getObject(new GetObjectRequest("dsps202assignment2", summary.getKey()));
                BufferedReader myReader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
                String line = null;
                while (true) {
                    try {
                        if (!((line = myReader.readLine()) != null)) break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    String[] decN = line.split(" ");
                    if (decN.length == 2)
                        if (!DecadeN_Map.containsKey(Integer.parseInt(decN[0])))
                            DecadeN_Map.put(Integer.parseInt(decN[0]), new LongWritable(Long.parseLong(decN[1])));//new Decade
                        else {//updating value of N for decade
                            LongWritable previousVal = DecadeN_Map.get(Long.parseLong(decN[0]));
                            long newVal = Long.parseLong(decN[1]) + previousVal.get();
                            DecadeN_Map.put(Integer.parseInt(decN[0]), new LongWritable(newVal));
                        }
                }
            }

        }


        @Override
        public void reduce(phaseKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }

            Text w1 = new Text(key.getw1().toString());
            Text w2 = new Text(key.getw2().toString());
            IntWritable decade = new IntWritable(key.getDecade().get());


            if (key.getw2().equals(ASTERISK) && !key.getw1().equals(ASTERISK)) {//<w1,*>
                cw1 = new LongWritable(counter);
            } else {//<w1,w2>

                LongWritable currentN;
                //retrive N for decade from Map
                if (DecadeN_Map.containsKey(decade.get())) {
                    currentN = DecadeN_Map.get(decade.get());
                } else
                    currentN = new LongWritable(1);

                phaseKey newPhaseTwoKey = new phaseKey(w1, w2, decade);
                phaseValue newPhaseTwoValue = new phaseValue(
                        new LongWritable(cw1.get()),
                        new LongWritable(0),//cw2
                        new LongWritable(counter),//cw1w2
                        new LongWritable(currentN.get())//N
                );
                // context.write(newPhaseTwoKey, newPhaseTwoValue);

                if (K_V_toWrite.size() < 5000) {
                    K_V_toWrite.add(new K_V2(newPhaseTwoKey, newPhaseTwoValue));
                } else {
                    //write and clean
                    for (K_V2 k_v : K_V_toWrite) {
                        context.write(k_v.getPhaseTwoKey(), k_v.getPhaseTwoValue());
                    }
                    K_V_toWrite.clear();
                    //add current K-V
                    K_V_toWrite.add(new K_V2(newPhaseTwoKey, newPhaseTwoValue));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!K_V_toWrite.isEmpty()) {
                for (K_V2 k_v : K_V_toWrite) {
                    context.write(k_v.getPhaseTwoKey(), k_v.getPhaseTwoValue());
                }
                K_V_toWrite.clear();
            }
            super.cleanup(context);
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("********PhaseTwo started Running**********\n");

        String TwoGramInput =  args[0];
        String appId = args[1];
        String lang = args[2];
        String oneGramInput =  "s3n://dsps202assignment2/" + appId + "/results/outputPhase1";
        System.out.println(appId);
        String output = "s3n://dsps202assignment2/" + appId + "/results/outputPhase2"; //UUID.randomUUID().toString().replace('\\', '_').replace('/', '_').replace(':', '_')
        String NPath =  appId + "/N";//"/" +
        Configuration conf = new Configuration();
        conf.set("NPath", NPath);
        conf.set("lang", lang);
        Job job = Job.getInstance(conf, "phaseTwo");
        job.setJarByClass(phaseTwo.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(phaseKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(phaseKey.class);
        job.setOutputValueClass(phaseValue.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);//SequenceFileInputFormat
        //FileInputFormat.addInputPath(job, new Path(inputFilePath2));
        //FileInputFormat.addInputPath(job, new Path(inputFilePath));
        MultipleInputs.addInputPath(job, new Path(oneGramInput), TextInputFormat.class);//oneGramInput
        MultipleInputs.addInputPath(job, new Path(TwoGramInput), SequenceFileInputFormat.class);//SequenceFileInputFormat

        FileOutputFormat.setOutputPath(job, new Path(output));
        System.out.println("********PhaseTwo wait for completion**********\n");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("********PhaseTwo exit**********\n");
        while (true) ;
    }

}
