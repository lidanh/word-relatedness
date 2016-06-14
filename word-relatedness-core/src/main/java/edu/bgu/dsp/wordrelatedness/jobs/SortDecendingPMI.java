package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class SortDecendingPMI {

    public static class Map<S, I> extends Mapper<WordPair, DoubleWritable, DoubleWritable, WordPair> {
        public void map(WordPair key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }


    public static class Reduce extends Reducer<DoubleWritable, WordPair, DoubleWritable, WordPair> {

        public void reduce(DoubleWritable key, Iterable<WordPair> values, Context context) throws IOException, InterruptedException {
            for (WordPair value : values) {
                context.write(key, value);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "SortDecendingPMI");

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(WordPair.class);

        job.setSortComparatorClass(DoubleReverseComparator.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // If out dir is already exists - delete it
        Utils.deleteDirectory(new File(args[1]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        List<WordsPair> Ks = Utils.GetK("resources/SortDecendingPMI/part-r-00000", 5);
        Utils.KsToFile(Ks);

    }

}

