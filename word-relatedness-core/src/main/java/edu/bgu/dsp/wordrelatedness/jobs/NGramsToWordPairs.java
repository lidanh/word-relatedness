package edu.bgu.dsp.wordrelatedness.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NGramsToWordPairs extends Configured implements Tool {
    private static final String WordRegex = "[a-zA-Z]*";

    public static class JobMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");

            /*
            splitted[0] - n-gram
            splitted[1] - year
            splitted[2] - occurrences
            splitted[3] - pages
            splitted[4] - books
			*/

            if (splitted.length < 5)
                return;

            /* Year -> Decade */
            int year = Integer.parseInt(splitted[1]);
            if (year < 1900)
                return;

            int decade = year - (year % 10);
            if (decade < 0) {
                return;
            }

            /* Occurrences */
            long occurrences = Long.parseLong(splitted[2]);


            /* n-grams */
            String[] ngrams = splitted[0]
                    .toLowerCase()
                    .replaceAll("[^a-z ]", "")
                    .split("\\s+");

            if (ngrams.length < 2)
                return;

            // Split the ngram to words
            List<String> words = new ArrayList<>();
            for (String s : ngrams) {
                words.add(s);
            }

            // Remove stopwords
            words.removeAll(Utils.stopWords);

            // The word we will compare to
            int middle_word_index;

            // Remove all empty strings
            words.removeAll(Collections.singleton(""));

            // Ignore 1 gram or empty
            if (words.size() < 2) {
                return;
            }

            // decide which word is middle
            middle_word_index = getMiddleWordIndex(words);

            // middle word
            String middle_word = words.get(middle_word_index);

            // Remove it from the rest
            words.remove(middle_word);

            // For each couple middle,word, insert:
            // middle,word
            // middle,*
            // word,*
            for (String word : words) {
                Text toWriteMiddleWord;
                // Lexicography order
                if (middle_word.compareTo(word) < 0) {
                    toWriteMiddleWord = new Text(middle_word + "," + word);
                } else {
                    toWriteMiddleWord = new Text(word + "," + middle_word);
                }
                Text toWritemiddleStar = new Text(middle_word + ",*");
                Text toWriteWordStar = new Text(word + ",*");
                Text twoStars = new Text("*,*");

                writeToContext(context, new LongWritable(decade), new LongWritable(occurrences), twoStars);
                writeToContext(context, new LongWritable(decade), new LongWritable(occurrences), toWriteMiddleWord);
                writeToContext(context, new LongWritable(decade), new LongWritable(occurrences), toWritemiddleStar);
                writeToContext(context, new LongWritable(decade), new LongWritable(occurrences), toWriteWordStar);
            }
        }
    }

    private static int getMiddleWordIndex(List<String> words) {
        int middle_word_index;
        if (words.size() == 2) {
            middle_word_index = 0;
        } else if (words.size() == 3 || words.size() == 4) {
            middle_word_index = 1;
        } else {
            middle_word_index = 2;
        }
        return middle_word_index;
    }

    private static void writeToContext(Mapper<LongWritable, Text, Text, LongWritable>.Context context, LongWritable decade, LongWritable times, Text toWrite) throws IOException, InterruptedException {
        context.write(new Text(decade.toString()+","+toWrite), times);
    }

    public static class JobCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // For each word, sum all its maps values, merge it to "reduced" map
            LongWritable allTimes = new LongWritable();
            for (LongWritable val : values) {
                allTimes.set(allTimes.get() + val.get());
            }
            context.write(key, allTimes);
        }
    }

    public static class JobReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // For each word, sum all its maps values, merge it to "reduced" map
            LongWritable allTimes = new LongWritable();
            for (LongWritable val: values) {
                allTimes.set(allTimes.get()+val.get());
            }
//            Utils.updateCounter(key, context);
            context.write(key, allTimes);
        }
    }

    public Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = new Job(new Configuration(), NGramsToWordPairs.class.getSimpleName());

        job.setJarByClass(NGramsToWordPairs.class);

        job.setMapperClass(JobMapper.class);
        job.setCombinerClass(JobCombiner.class);
        job.setReducerClass(JobReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

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
            ToolRunner.run(new NGramsToWordPairs(), args);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }
    }

}