package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import edu.bgu.dsp.wordrelatedness.domain.WordPairMapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;


public class Job3 {

    public static class Map extends Mapper<WordPair, MapWritable, WordPair, MapWritable> {
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


    public static class Reduce extends Reducer<WordPair, WordPairMapWritable, WordPair, DoubleWritable> {
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
            if (key.toString().contains("*,*")) {
                N = Long.parseLong(values.iterator().next().get(key).toString());
                return;
            }
            for (MapWritable value : values) {
                System.out.print(key+"\t");
                System.out.println(value);

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

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputValueClass(WordPairMapWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

//        job.setSortComparatorClass(StarComparator.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // If out dir is already exists - delete it
        Utils.deleteDirectory(new File(args[1]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
