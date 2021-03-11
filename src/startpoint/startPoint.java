/**
 *
 */

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.UUID;


public class startPoint {
    //General variables
    private static final String APP_ID = UUID.randomUUID().toString().replace('-', '_').replace('\\', '_').replace('/', '_').replace(':', '_');


    //ENG
    private static final String twoGramENGUrl = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
    private static final String oneGramENGUrl = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
    //HEB
    private static final String twoGramHEBUrl = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    private static final String oneGramHEBUrl = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";

    public static void main(String [ ] args)
    {
    System.out.println("started startPoint\n");
//        ******Initialize*************
    String lang_to_process = args[0];
    String twoGramUrl;
    String oneGramUrl;

    if (lang_to_process.equals("eng")) {
        twoGramUrl = twoGramENGUrl;
        oneGramUrl = oneGramENGUrl;
    } else if (lang_to_process.equals("heb")) {
        twoGramUrl = twoGramHEBUrl;
        oneGramUrl = oneGramHEBUrl;
    } else { //bug in run command
        System.out.println("Invalid Run Command, please run again.\n");
        return;
    }


    AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
    AmazonElasticMapReduce EMR = AmazonElasticMapReduceClientBuilder
            .standard()
            .withRegion(Regions.US_EAST_1)
            .withCredentials(credentialsProvider)
            .build();


//        ******Define steps*************


    HadoopJarStepConfig jarPhase1 = new HadoopJarStepConfig()
            .withJar("s3n://dsps202assignment2/jars/phaseOne.jar")
            .withArgs(oneGramUrl, APP_ID, lang_to_process);

    StepConfig hadoopPhase1 = new StepConfig()
            .withName("hadoopPhase1")
            .withHadoopJarStep(jarPhase1)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

    HadoopJarStepConfig jarStep2 = new HadoopJarStepConfig()
            .withJar("s3n://dsps202assignment2/jars/phaseTwo.jar")
            .withArgs(twoGramUrl, APP_ID, lang_to_process);

    StepConfig hadoopPhase2 = new StepConfig()
            .withName("hadoopPhase2")
            .withHadoopJarStep(jarStep2)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://dsps202assignment2/jars/phaseThree.jar")
                .withArgs(APP_ID, lang_to_process);

        StepConfig hadoopPhase3 = new StepConfig()
                .withName("hadoopPhase3")
                .withHadoopJarStep(jarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


    //        ******Define Instances*************
    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
            .withInstanceCount(5)
            .withMasterInstanceType(InstanceType.M4Large.toString())
            .withSlaveInstanceType(InstanceType.M4Large.toString())
            .withHadoopVersion("3.1.2")
            .withEc2KeyName("EC2_KeyPair")
            .withKeepJobFlowAliveWhenNoSteps(false)
            .withPlacement(new PlacementType("us-east-1a")
            );


    //        ******create runFlowRequest*************

    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
            .withName("dsps202assignment2")
            .withInstances(instances)
            .withSteps( hadoopPhase1,hadoopPhase2,hadoopPhase3)
            .withLogUri("s3n://dsps202assignment2/" + APP_ID + "/logs")
            .withJobFlowRole("EMR_EC2_DefaultRole")
            .withServiceRole("EMR_DefaultRole")
            .withReleaseLabel("emr-6.0.0");

    RunJobFlowResult runJobFlowResult = EMR.runJobFlow(runFlowRequest);
    String jobFlowId = runJobFlowResult.getJobFlowId();
    System.out.println("Ran job flow with id: " + jobFlowId);


    }
}

