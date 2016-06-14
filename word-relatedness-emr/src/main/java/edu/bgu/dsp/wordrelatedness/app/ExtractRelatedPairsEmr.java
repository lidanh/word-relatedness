package edu.bgu.dsp.wordrelatedness.app;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by lidanh on 11/06/2016.
 */
public class ExtractRelatedPairsEmr {
    static final String S3Jar = "s3n://malachi-bucket/jars/ExtractRelatedPairs.jar";
    static final String CorpusPath = "s3n://malachi-bucket/input";
//    static final String CorpusPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/5gram/data";
    static final String IntermediateOutput = "hdfs:///intermediate_output/";
    static final String FinalOutput = "s3n://malachi-bucket/output";
    static final ActionOnFailure DefaultActionOnFailure = ActionOnFailure.TERMINATE_JOB_FLOW;
    static final Integer InstancesCount = 10;
    static final InstanceType DefaultInstanceType = InstanceType.M1Large;
    static final String HadoopVersion = "2.4.0";
    static final String LogsPath = "s3n://malachi-bucket/logs/";

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
        AWSCredentials Credentials = new PropertiesCredentials(new FileInputStream(
                "AwsCredentials.properties"));
        AmazonElasticMapReduceClient emrClient = new AmazonElasticMapReduceClient(Credentials);


        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar(S3Jar)
                .withMainClass("edu.bgu.dsp.wordrelatedness.jobs.ExtractRelatedPairs")
                .withArgs(CorpusPath, FinalOutput);
//                .withArgs(CorpusPath, IntermediateOutput);

//        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
//                .withJar(S3Jar)
//                .withMainClass("edu.bgu.dsp.wordrelatedness.jobs.AddStarToWord")
//                .withArgs(IntermediateOutput, FinalOutput, k);

        StepConfig step1Conf = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure(DefaultActionOnFailure);

//        StepConfig step2Conf = new StepConfig()
//                .withName("step2")
//                .withHadoopJarStep(step2)
//                .withActionOnFailure(DefaultActionOnFailure);

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
//                .withSteps(step1Conf, step2Conf)
                .withSteps(step1Conf)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withLogUri(LogsPath);

        RunJobFlowResult runJobFlowResult = emrClient.runJobFlow(runJobFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println(String.format("Job %s was submitted successfully!", jobFlowId));

        DescribeClusterResult result = waitForCompletion(emrClient, jobFlowId, 10, TimeUnit.SECONDS);
        diagnoseClusterResult(result, jobFlowId);

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
