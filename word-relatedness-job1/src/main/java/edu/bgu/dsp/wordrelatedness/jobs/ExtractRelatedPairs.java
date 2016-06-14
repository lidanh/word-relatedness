package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import edu.bgu.dsp.wordrelatedness.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExtractRelatedPairs extends Configured implements Tool {
    private static final String WordRegex = "[a-zA-Z]*";

    static class JobMapper extends Mapper<LongWritable, Text, WordPair, LongWritable> {
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

            // Remove all empty strings
            words.removeAll(Collections.singleton(""));

            // Ignore 1 gram or empty
            if (words.size() < 2) {
                return;
            }

            // decide which word is middle
            int middle_word_index = getMiddleWordIndex(words);

            // middle word
            String middle_word = words.get(middle_word_index);

//            if (Utils.stopWords.contains(middle_word) || !middle_word.matches(WordRegex))
//                return;

            // Remove it from the rest
            words.remove(middle_word);

            // For each couple middle,word, insert:
            // middle,word
            // middle,*
            // word,*
            for (String word : words) {
//                if (word.equals(middle_word))
//                    continue;

//                if (Utils.stopWords.contains(word) || !word.matches(WordRegex))
//                    continue;

                // emit <<*, *>, count> for counting n (total number of words per decade)
                context.write(new WordPair(WordPair.WildCard, WordPair.WildCard, decade), new LongWritable(occurrences));
                // emit <<*, w1>, count> for counting c(w2)
                context.write(new WordPair(middle_word, WordPair.WildCard, decade), new LongWritable(occurrences));
                // emit <<*, w2>, count> for counting c(w1)
                context.write(new WordPair(word, WordPair.WildCard, decade), new LongWritable(occurrences));
                // emit <<w1, w2>, count>
                context.write(new WordPair(middle_word.compareTo(word) < 0 ? middle_word : word, middle_word.compareTo(word) < 0 ? word : middle_word, decade), new LongWritable(occurrences));
            }
        }
    }

    static int getMiddleWordIndex(List<String> words) {
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

    static class JobCombiner extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
        public void reduce(WordPair key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // For each word, sum all its maps values, merge it to "reduced" map
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            context.write(key, new LongWritable(sum));
        }
    }

    static class JobPartitioner extends Partitioner<WordPair, LongWritable> {
        public int getPartition(WordPair wordPair, LongWritable longWritable, int numPartitions) {
            return wordPair.getDecade().get() % numPartitions;
        }
    }

    static class JobReducer extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
        public void reduce(WordPair key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // For each word, sum all its maps values, merge it to "reduced" map
            long total = 0;

            for (LongWritable value : values) {
                total += value.get();
            }

            context.write(key, new LongWritable(total));
        }
    }

    public static Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = new Job(new Configuration(), ExtractRelatedPairs.class.getSimpleName());

        job.setJarByClass(ExtractRelatedPairs.class);

        job.setMapperClass(JobMapper.class);
        job.setCombinerClass(JobCombiner.class);
        job.setPartitionerClass(JobPartitioner.class);
        job.setReducerClass(JobReducer.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(LongWritable.class);

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
        System.out.println("!!!!!");
        if (args.length != 2)
            throw new IllegalArgumentException("Args error");

        try {
            // If out dir is already exists - delete it
            Utils.deleteDirectory(new File(args[1]));
            ToolRunner.run(new ExtractRelatedPairs(), args);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}