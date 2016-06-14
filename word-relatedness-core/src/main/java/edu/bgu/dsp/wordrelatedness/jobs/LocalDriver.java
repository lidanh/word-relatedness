package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import edu.bgu.dsp.wordrelatedness.domain.WordPairMapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.util.List;
import java.util.Map;

public class LocalDriver {

    public static void main(String[] args) throws Exception {

        Utils.deleteDirectory(new File("output1"));
        Utils.deleteDirectory(new File("output2"));
        Utils.deleteDirectory(new File("output3"));
        Utils.deleteDirectory(new File("output4"));

        //////////////////////// JOB1 /////////////////////////////////
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "job1");
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setJarByClass(ExtractRelatedPairs.class);
        job1.setMapperClass(ExtractRelatedPairs.JobMapper.class);
        job1.setReducerClass(ExtractRelatedPairs.JobReducer.class);
        job1.setPartitionerClass(ExtractRelatedPairs.JobPartitioner.class);
        job1.setOutputKeyClass(WordPair.class);
        job1.setOutputValueClass(LongWritable.class);

        ControlledJob cJob1 = new ControlledJob(conf1);
        cJob1.setJob(job1);

        FileInputFormat.addInputPath(job1, new Path("/home/malachi/IdeaProjects/word-relatedness/example"));
        FileOutputFormat.setOutputPath(job1, new Path("/home/malachi/IdeaProjects/word-relatedness/output1"));



        //////////////////////// JOB2 /////////////////////////////////
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "job2");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setJarByClass(AddStarToWord.class);
        job2.setMapperClass(AddStarToWord.JobMapper.class);
        job2.setReducerClass(AddStarToWord.JobReducer.class);
        job2.setOutputKeyClass(WordPair.class);
        job2.setOutputValueClass(WordPairMapWritable.class);

//        job2.setSortComparatorClass(StarComparator.class);

        ControlledJob cJob2 = new ControlledJob(conf1);
        cJob2.setJob(job2);
        FileInputFormat.addInputPath(job2, new Path("/home/malachi/IdeaProjects/word-relatedness/output1/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("/home/malachi/IdeaProjects/word-relatedness/output2"));



        //////////////////////// JOB3 /////////////////////////////////
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "job3");
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        job3.setOutputKeyClass(WordPair.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setMapOutputValueClass(WordPairMapWritable.class);

        job3.setJarByClass(CalcPMI.class);
        job3.setMapperClass(CalcPMI.Map.class);
        job3.setReducerClass(CalcPMI.Reduce.class);

//        job3.setSortComparatorClass(StarComparator.class);

        ControlledJob cJob3 = new ControlledJob(conf1);
        cJob3.setJob(job3);
        FileInputFormat.addInputPath(job3, new Path("/home/malachi/IdeaProjects/word-relatedness/output2/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path("/home/malachi/IdeaProjects/word-relatedness/output3"));

        //////////////////////// JOB4 /////////////////////////////////
        Configuration conf4 = new Configuration();
        Job job4 = new Job(conf4, "job4");
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        job4.setOutputKeyClass(DoubleWritable.class);
        job4.setOutputValueClass(WordPair.class);

        job4.setSortComparatorClass(DoubleReverseComparator.class);

        job4.setJarByClass(SortDecendingPMI.class);
        job4.setMapperClass(SortDecendingPMI.Map.class);
        job4.setReducerClass(SortDecendingPMI.Reduce.class);


        ControlledJob cJob4 = new ControlledJob(conf1);
        cJob4.setJob(job4);
        FileInputFormat.addInputPath(job4, new Path("/home/malachi/IdeaProjects/word-relatedness/output3/part-r-00000"));
        FileOutputFormat.setOutputPath(job4, new Path("/home/malachi/IdeaProjects/word-relatedness/output4"));


        ////////////////////// RUN /////////////////////////////////
        JobControl jobctrl = new JobControl("jobctrl");
        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        cJob2.addDependingJob(cJob1);
        cJob3.addDependingJob(cJob2);
        cJob4.addDependingJob(cJob3);

        jobctrl.run();

        Map scores = Utils.calcFMeasure("/home/malachi/IdeaProjects/word-relatedness/output4/part-r-00000");
        Utils.scoresToFile(scores);

        List<WordsPair> Ks = Utils.GetK("/home/malachi/IdeaProjects/word-relatedness/output4/part-r-00000", 5);
        Utils.KsToFile(Ks);

    }
}