package edu.bgu.dsp.wordrelatedness.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;


public class Job3 {

    public static class Map extends Mapper<Text, MapWritable, Text, MapWritable> {
        String currentStarWord = null;
        LongWritable currentStarCount = null;

        public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            /// if word * : keep th value in memory
            if (key.toString().contains("*,*")) {
                context.write(key, value);
            } if (Utils.isStar(key.toString())) {
                // Parse and get star word
                String starWord = Utils.getStarWord(key);
                LongWritable starWordCount = Utils.stringToLongWritable(value.get(key).toString());
                this.currentStarWord = starWord;
                this.currentStarCount = starWordCount;
                return;
            }
            // Else if word1 : <word2,count> : emit(word2, <word1,word2|count>, <word1,*|star_count>)
            value.put(new Text(key.toString() + ",*"), this.currentStarCount);
            key = Utils.getKeyFromValue(value);
            context.write(key, value);
        }
    }


    public static class Reduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        long stars = 0;
        long both = 0;
        double quotient = 0;
        double logVal = 0;
        long N = 0;

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {



            // calc PMI
            stars = 0;
            both = 0;
            quotient = 0;
            logVal = 0;
            if (key.toString().contains("*,*")) {
                N = Long.parseLong(values.iterator().next().get(key).toString());
                return;
            }
            for (MapWritable value : values) {
                for (Writable wordsCount : value.keySet()) {
                    if (wordsCount.toString().contains("*")) {
                        both += Long.parseLong(value.get(wordsCount).toString());
                    } else {
                        stars += Math.log(Long.parseLong(value.get(wordsCount).toString()));
                    }
                }

                logVal = Math.log(both) + Math.log(N) - stars;

                DoubleWritable PMI = new DoubleWritable(logVal);
                context.write(key, PMI);
//                System.out.print(key + "\t");
//                System.out.println(PMI);
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Job3");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

//        job.setSortComparatorClass(StarComparator.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // If out dir is already exists - delete it
        Utils.deleteDirectory(new File(args[1]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
//        System.out.println(job.getCounters());

    }

}
