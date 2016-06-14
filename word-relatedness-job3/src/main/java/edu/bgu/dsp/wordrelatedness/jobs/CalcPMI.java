package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import edu.bgu.dsp.wordrelatedness.domain.WordPairMapWritable;
import edu.bgu.dsp.wordrelatedness.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;


public class CalcPMI extends Configured implements Tool {

    static class JobMapper extends Mapper<WordPair, MapWritable, WordPair, MapWritable> {
        Text currentStarWord = null;
        LongWritable currentStarCount = null;

        public void map(WordPair key, MapWritable value, Context context) throws IOException, InterruptedException {
            // if key == *-* : emit (*-*| <*-*, count>)
            if(key.isTotalForDecade()){
                context.write(key, value);
                return;
            }
            // else if key == word-* : write value in memory
            if(key.isStar()){
                this.currentStarWord = key.getW1();
                this.currentStarCount = (LongWritable) value.get(key);
                return;
            }
            // else if word1,word2 : emit(word2==> <word1-word2, count>, <word1-*, count>, <word-*, count>)
            WordPair W1W2 = new WordPair(key.getW2(), key.getW1(), key.getDecade());
            value.put(currentStarWord, currentStarCount);
            context.write(W1W2, value);
        }
    }


    static class JobReducer extends Reducer<WordPair, WordPairMapWritable, WordPair, DoubleWritable> {
        long stars = 0;
        long both = 0;
        double quotient = 0;
        double logVal = 0;
        long N = 0;

        public void reduce(WordPair key, Iterable<WordPairMapWritable> values, Context context) throws IOException, InterruptedException {
            // calc PMI
            stars = 0;
            both = 0;
            quotient = 0;
            logVal = 0;

            if (key.isTotalForDecade()) {
                N = Long.parseLong(values.iterator().next().get(key).toString());
                return;
            }
            for (MapWritable value : values) {

                for (Writable wordsCount : value.keySet()) {
                    if (wordsCount.toString().contains(",")) {
                        both += Long.parseLong(value.get(wordsCount).toString());
                    } else {
                        stars += Math.log(Long.parseLong(value.get(wordsCount).toString()));
                    }
                }

                logVal = Math.log(both) + Math.log(N) - stars;

                DoubleWritable PMI = new DoubleWritable(logVal);
                context.write(key, PMI);
            }
        }
    }

    public static Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = new Job(new Configuration(), CalcPMI.class.getSimpleName());

        job.setJarByClass(CalcPMI.class);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputValueClass(WordPairMapWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class); // LZO Compressed files
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

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
            ToolRunner.run(new CalcPMI(), args);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}
