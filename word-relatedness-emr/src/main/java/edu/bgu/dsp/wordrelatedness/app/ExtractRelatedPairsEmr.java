package edu.bgu.dsp.wordrelatedness.app;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

/**
 * Created by lidanh on 11/06/2016.
 */
public class ExtractRelatedPairsEmr {
    static final String S3Jar = "s3n://dsp-lidan-emr/WordRelatedness.jar";
    static final String CorpusPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/5gram/data";
    static final String IntermediateOutput = "hdfs:///intermediate_output/";
    static final String FinalOutput = "s3n://dsp-lidan-emr/output_eng_5gram";
    static final ActionOnFailure DefaultActionOnFailure = ActionOnFailure.TERMINATE_JOB_FLOW;
    static final Integer InstancesCount = 10;
    static final InstanceType DefaultInstanceType = InstanceType.M1Large;
    static final String HadoopVersion = "2.7.2";
    static final String LogsPath = "s3n://dsp-lidan-emr/logs/";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage:\njava -jar ExtractRelatedPairs.jar <k>");
            return;
        }

        try {
            Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.err.println("Usage:\njava -jar ExtractRelatedPairs.jar <k>\nk argument must be a valid number!");
            return;
        }

        try {
            runEmr(args[0]);
        } catch (Exception e) {
            System.err.println("Error during running the job: \n" + e.getMessage());
        }
    }

    private static void runEmr(String k) {
        AmazonElasticMapReduce emrClient = new AmazonElasticMapReduceClient(new DefaultAWSCredentialsProviderChain());

        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar(S3Jar)
                .withMainClass("edu.bgu.dsp.wordrelatedness.jobs.NGramsToWordPairs")
                .withArgs(CorpusPath, IntermediateOutput);

        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar(S3Jar)
                .withMainClass("edu.bgu.dsp.wordrelatedness.jobs.WordPairsPMICalc")
                .withArgs(IntermediateOutput, FinalOutput, k);

        StepConfig step1Conf = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure(DefaultActionOnFailure);

        StepConfig step2Conf = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure(DefaultActionOnFailure);

        JobFlowInstancesConfig instancesConfig = new JobFlowInstancesConfig()
                .withInstanceCount(InstancesCount)
                .withMasterInstanceType(DefaultInstanceType.toString())
                .withSlaveInstanceType(DefaultInstanceType.toString())
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"))
                .withHadoopVersion(HadoopVersion);

        RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest()
                .withName("word-relatedness")
                .withInstances(instancesConfig)
                .withSteps(step1Conf, step2Conf)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRolw")
                .withLogUri(LogsPath);

        RunJobFlowResult runJobFlowResult = emrClient.runJobFlow(runJobFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println(String.format("Job %s was submitted successfully!", jobFlowId));

    }
}
