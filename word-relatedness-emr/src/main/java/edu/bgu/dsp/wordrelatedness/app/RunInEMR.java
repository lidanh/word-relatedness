package edu.bgu.dsp.wordrelatedness.app;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * Created by lidanh on 11/06/2016.
 */
public class RunInEMR {
    private static final String S3Jar1 = "s3n://malachi-bucket/jars/word-relatedness-job1.jar";
    private static final String S3Jar2 = "s3n://malachi-bucket/jars/word-relatedness-job2.jar";
    private static final String S3Jar3 = "s3n://malachi-bucket/jars/word-relatedness-job3.jar";
//    private static final String CorpusPath = "s3n://malachi-bucket/input";
    static final String CorpusPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/5gram/data";
    private static final String IntermediateOutput1 = "hdfs:///intermediate_output1/";
    private static final String IntermediateOutput2 = "hdfs:///intermediate_output2/";
    private static final String FinalOutput = "s3n://malachi-bucket/output";
    private static final ActionOnFailure DefaultActionOnFailure = ActionOnFailure.TERMINATE_JOB_FLOW;
    private static final Integer InstancesCount = 2;
    private static final InstanceType DefaultInstanceType = InstanceType.M1Large;
    private static final String HadoopVersion = "2.4.0";
    private static final String LogsPath = "s3n://malachi-bucket/logs/";

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

    private static void runEmr(String k) throws IOException, InterruptedException {

        AmazonElasticMapReduceClient emrClient = new AmazonElasticMapReduceClient(new DefaultAWSCredentialsProviderChain());

        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar(S3Jar1)
                .withArgs(CorpusPath, IntermediateOutput1);

        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar(S3Jar2)
                .withArgs(IntermediateOutput1, IntermediateOutput2);

        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                    .withJar(S3Jar3)
                    .withArgs(IntermediateOutput2, FinalOutput);

        StepConfig step1Conf = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure(DefaultActionOnFailure);

        StepConfig step2Conf = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure(DefaultActionOnFailure);

        StepConfig step3Conf = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(step3)
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
                .withSteps(step1Conf, step2Conf, step3Conf)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withLogUri(LogsPath);
//
        RunJobFlowResult runJobFlowResult = emrClient.runJobFlow(runJobFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println(String.format("Job %s was submitted successfully!", jobFlowId));

        DescribeClusterResult result = waitForCompletion(emrClient, jobFlowId, 10, TimeUnit.SECONDS);
        diagnoseClusterResult(result, jobFlowId);


    }

    private static void displayTextInputStream(InputStream input)
            throws IOException {
        // Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new
                InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    //////////////////// DEBUG //////////////////////

    private static DescribeClusterResult waitForCompletion(
            AmazonElasticMapReduceClient emr, String jobFlowId,
            long sleepTime, TimeUnit timeUnit)
            throws InterruptedException {
        String state = "STARTING";
        while (true) {
            DescribeClusterResult result = emr.describeCluster(
                    new DescribeClusterRequest().withClusterId(jobFlowId)
            );
            ClusterStatus status = result.getCluster().getStatus();
            String newState = status.getState();
            if (!state.equals(newState)) {
                System.out.println("Cluster id "+jobFlowId+" switched from "+state+" to "+newState+".  Reason: "+status.getStateChangeReason()+".");
                state = newState;
            }
            System.out.println(state);

            switch (state) {
                case "TERMINATED":
                case "TERMINATED_WITH_ERRORS":
                case "WAITING":
                    return result;
            }

            timeUnit.sleep(sleepTime);
        }
    }

    private static void diagnoseClusterResult(DescribeClusterResult result, String jobFlowId) {
        ClusterStatus status = result.getCluster().getStatus();
        ClusterStateChangeReason reason = status.getStateChangeReason();
        ClusterStateChangeReasonCode code =
                ClusterStateChangeReasonCode.fromValue(reason.getCode());
        switch (code) {
            case ALL_STEPS_COMPLETED:
                System.out.println("Completed EMR job "+ jobFlowId);
                break;
            default:
                failEMR(jobFlowId, status);
        }
    }

    private static void failEMR(String jobFlowId, ClusterStatus status) {
        String msg = "EMR cluster run %s terminated with errors.  ClusterStatus = %s";
        throw new RuntimeException(String.format(msg, jobFlowId, status));
    }

}
