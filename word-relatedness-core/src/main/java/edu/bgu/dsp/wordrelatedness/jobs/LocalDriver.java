package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;
import java.util.Map;

public class LocalDriver {

    public static void main(String[] args) throws Exception {

        //////////////////////// JOB1 /////////////////////////////////
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "job1");
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setJarByClass(NGramsToWordPairsOld.class);
        job1.setMapperClass(NGramsToWordPairsOld.JobMapper.class);
        job1.setReducerClass(NGramsToWordPairsOld.JobReducer.class);
        job1.setPartitionerClass(NGramsToWordPairsOld.JobPartitioner.class);
        job1.setOutputKeyClass(WordPair.class);
        job1.setOutputValueClass(LongWritable.class);

        ControlledJob cJob1 = new ControlledJob(conf1);
        cJob1.setJob(job1);

        FileInputFormat.addInputPath(job1, new Path("/home/malachi/IdeaProjects/dist2/input"));
        FileOutputFormat.setOutputPath(job1, new Path("/home/malachi/IdeaProjects/dist2/output1"));



        //////////////////////// JOB2 /////////////////////////////////
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "job2");
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setJarByClass(Job2.class);
        job2.setMapperClass(Job2.Map.class);
        job2.setReducerClass(Job2.Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(MapWritable.class);

        job2.setSortComparatorClass(StarComparator.class);

        ControlledJob cJob2 = new ControlledJob(conf1);
        cJob2.setJob(job2);
        FileInputFormat.addInputPath(job2, new Path("/home/malachi/IdeaProjects/dist2/output1/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("/home/malachi/IdeaProjects/dist2/output2"));



        //////////////////////// JOB3 /////////////////////////////////
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "job3");
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setMapOutputValueClass(MapWritable.class);

        job3.setJarByClass(Job3.class);
        job3.setMapperClass(Job3.Map.class);
        job3.setReducerClass(Job3.Reduce.class);

        job3.setSortComparatorClass(StarComparator.class);

        ControlledJob cJob3 = new ControlledJob(conf1);
        cJob3.setJob(job3);
        FileInputFormat.addInputPath(job3, new Path("/home/malachi/IdeaProjects/dist2/output2/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path("/home/malachi/IdeaProjects/dist2/output3"));

        //////////////////////// JOB4 /////////////////////////////////
        Configuration conf4 = new Configuration();
        Job job4 = new Job(conf4, "job4");
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        job4.setOutputKeyClass(DoubleWritable.class);
        job4.setOutputValueClass(Text.class);

        job4.setSortComparatorClass(OppositeComparator.class);

        job4.setJarByClass(Job4.class);
        job4.setMapperClass(Job4.Map.class);
        job4.setReducerClass(Job4.Reduce.class);


        ControlledJob cJob4 = new ControlledJob(conf1);
        cJob4.setJob(job4);
        FileInputFormat.addInputPath(job4, new Path("/home/malachi/IdeaProjects/dist2/output3/part-r-00000"));
        FileOutputFormat.setOutputPath(job4, new Path("/home/malachi/IdeaProjects/dist2/output4"));


        //////////////////////// RUN /////////////////////////////////
        JobControl jobctrl = new JobControl("jobctrl");
        jobctrl.addJob(cJob1);
        jobctrl.addJob(cJob2);
        jobctrl.addJob(cJob3);
        jobctrl.addJob(cJob4);
        cJob2.addDependingJob(cJob1);
        cJob3.addDependingJob(cJob2);
        cJob4.addDependingJob(cJob3);

        jobctrl.run();

        Map scores = Utils.calcFMeasure("resources/Job4/part-r-00000");
        Utils.scoresToFile(scores);

        List<WordsPair> Ks = Utils.GetK("resources/Job4/part-r-00000", 5);
        Utils.KsToFile(Ks);

    }
}