package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.DoubleReverseComparator;
import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import edu.bgu.dsp.wordrelatedness.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;


public class SortDescendingPMI extends Configured implements Tool {

    static class JobMapper extends Mapper<WordPair, DoubleWritable, DoubleWritable, WordPair> {
        public void map(WordPair key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }


    static class JobReducer extends Reducer<DoubleWritable, WordPair, DoubleWritable, WordPair> {
        public void reduce(DoubleWritable key, Iterable<WordPair> values, Context context) throws IOException, InterruptedException {
            for (WordPair value : values) {
                context.write(key, value);
            }
        }
    }

    public static Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = new Job(new Configuration(), SortDescendingPMI.class.getSimpleName());

        job.setJarByClass(SortDescendingPMI.class);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);

        job.setSortComparatorClass(DoubleReverseComparator.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(WordPair.class);

        job.setInputFormatClass(SequenceFileInputFormat.class); // LZO Compressed files
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobWiring(args[0], args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("Args error");

        try {
            // If out dir is already exists - delete it
            Utils.deleteDirectory(new File(args[1]));
            ToolRunner.run(new SortDescendingPMI(), args);


            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}

