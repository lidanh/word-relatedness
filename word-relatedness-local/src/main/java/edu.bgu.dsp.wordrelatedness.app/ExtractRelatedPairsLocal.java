package edu.bgu.dsp.wordrelatedness.app;

import edu.bgu.dsp.wordrelatedness.domain.WordsPair;
import edu.bgu.dsp.wordrelatedness.jobs.AddStarToWord;
import edu.bgu.dsp.wordrelatedness.jobs.CalcPMI;
import edu.bgu.dsp.wordrelatedness.jobs.ExtractRelatedPairs;
import edu.bgu.dsp.wordrelatedness.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExtractRelatedPairsLocal {
    public static void main(String[] args) throws IOException {
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

        Utils.deleteDirectory(new File("output1"));
        Utils.deleteDirectory(new File("output2"));
        Utils.deleteDirectory(new File("output3"));
        Utils.deleteDirectory(new File("output4"));

        //////////////////////// JOB1 /////////////////////////////////
        Configuration jobConfig = new Configuration();
        Job job1 = ExtractRelatedPairs.getJobWiring("/home/malachi/IdeaProjects/word-relatedness/example", "/home/malachi/IdeaProjects/word-relatedness/output1");

        ControlledJob cJob1 = new ControlledJob(jobConfig);
        cJob1.setJob(job1);

        //////////////////////// JOB2 /////////////////////////////////
        Job job2 = AddStarToWord.getJobWiring("/home/malachi/IdeaProjects/word-relatedness/output1/part-r-00000", "/home/malachi/IdeaProjects/word-relatedness/output2");

        ControlledJob cJob2 = new ControlledJob(jobConfig);
        cJob2.setJob(job2);

        //////////////////////// JOB3 /////////////////////////////////
        Job job3 = CalcPMI.getJobWiring("/home/malachi/IdeaProjects/word-relatedness/output2/part-r-00000", "/home/malachi/IdeaProjects/word-relatedness/output3");

        ControlledJob cJob3 = new ControlledJob(jobConfig);
        cJob3.setJob(job3);

        //////////////////////// JOB4 /////////////////////////////////
//        Job job4 = SortDescendingPMI.getJobWiring("/home/malachi/IdeaProjects/word-relatedness/output3/part-r-00000", "/home/malachi/IdeaProjects/word-relatedness/output4");
//
//        ControlledJob cJob4 = new ControlledJob(jobConfig);
//        cJob4.setJob(job4);

        ////////////////////// RUN /////////////////////////////////
        JobControl jobController = new JobControl("jobctrl");
        jobController.addJob(cJob1);
        jobController.addJob(cJob2);
        jobController.addJob(cJob3);
//        jobController.addJob(cJob4);
        cJob2.addDependingJob(cJob1);
        cJob3.addDependingJob(cJob2);
//        cJob4.addDependingJob(cJob3);

        jobController.run();

        String resultFilePath = "/home/malachi/IdeaProjects/word-relatedness/output3/part-r-00000";
        Map scores = Utils.calcFMeasure(resultFilePath);
        Utils.scoresToFile(scores);

        List<WordsPair> Ks = Utils.GetK(resultFilePath, 5);
        Utils.KsToFile(Ks);
    }
}
