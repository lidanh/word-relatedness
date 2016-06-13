package edu.bgu.dsp.wordrelatedness.old.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import org.apache.commons.io.IOUtils;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by lidanh on 10/06/2016.
 */
public class NGramsToWordPairsOld extends Configured implements Tool {
    private static final String WordRegex = "[a-zA-Z]*";

    static class JobMapper extends Mapper<Object, Text, WordPair, LongWritable> {
        private static Set<String> stopWords;
        static {
            ClassLoader classLoader = NGramsToWordPairsOld.class.getClassLoader();
            try {
                List<String> stopWordLines = IOUtils.readLines(classLoader.getResourceAsStream("stopwords.txt"));
                stopWords = new HashSet<String>(stopWordLines);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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

            WordPair pair;

            /* Year */
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
            String[] ngrams = splitted[0].split("\\s+");
            if (ngrams.length == 1) {
                return;
            }

            int midIndex = ngrams.length / 2;
            String midWord = ngrams[midIndex].toLowerCase();
            if (stopWords.contains(midWord) || !midWord.matches(WordRegex))
                return;

            for (int i = 0; i < ngrams.length; i++) {
                if (i == midIndex)
                    continue;

                String currentWord = ngrams[i].toLowerCase();
                if (stopWords.contains(currentWord) || !currentWord.matches(WordRegex))
                    continue;

                // emit <<w1, *>, count> for counting c(w1)
                context.write(new WordPair(midWord, WordPair.WildCard, decade), new LongWritable(occurrences));
                // emit <<*, w2>, count> for counting c(w2)
                context.write(new WordPair(WordPair.WildCard, currentWord, decade), new LongWritable(occurrences));

                context.write(new WordPair(WordPair.WildCard, WordPair.WildCard, decade), new LongWritable(occurrences));
                // emit <<w1, w2>, count>
                context.write(new WordPair(midWord, currentWord, decade), new LongWritable(occurrences));
            }
        }
    }

    static class JobCombiner extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
        private LongWritable newSum = new LongWritable();

        @Override
        protected void reduce(WordPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            newSum.set(sum);
            context.write(key, newSum);
        }
    }

    static class JobPartitioner extends Partitioner<WordPair, LongWritable> {
        public int getPartition(WordPair wordPair, LongWritable longWritable, int numPartitions) {
            return wordPair.getDecade().get() % numPartitions;
        }
    }

    static class JobReducer extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
        private LongWritable result = new LongWritable();
        private LongWritable w1Sum = new LongWritable();
        private LongWritable decadeSum = new LongWritable();

        private long n = 0;
        private long c = 0;

        @Override
        protected void reduce(WordPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            for (LongWritable value : values) {
                sum += value.get();
            }
            result.set(sum);

//            context.write(key, result);

            if (key.getW1().toString().equals(WordPair.WildCard) && key.getW2().toString().equals(WordPair.WildCard)) {
                // <*,*>
                n = sum;
            } else if (key.getW1().toString().equals(WordPair.WildCard)) {
                // <*, w>
                context.write(key, result);
            } else if (key.getW2().toString().equals(WordPair.WildCard)) {
                // <w, *>
                c = sum;
            } else {
                // <w, w>
                key.setN(new LongWritable(n));
                key.setC(new LongWritable(c));
                context.write(key, result);
            }
        }
    }

    public Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(getConf(), NGramsToWordPairsOld.class.getSimpleName());
        job.setJarByClass(NGramsToWordPairsOld.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(JobPartitioner.class);
        job.setCombinerClass(JobCombiner.class);
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class); // LZO Compressed files

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public int run(String[] args) throws Exception {
        System.out.println("Input directory: " + args[0]);
        System.out.println("Output directory: " + args[1]);

        Job job = getJobWiring(args[0], args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 2)
            throw new IllegalArgumentException("Args error");

        try {
            ToolRunner.run(new NGramsToWordPairsOld(), args);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }
    }

}
