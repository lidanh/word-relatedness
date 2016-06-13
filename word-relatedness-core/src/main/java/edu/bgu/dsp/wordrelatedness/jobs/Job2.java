package edu.bgu.dsp.wordrelatedness.jobs;

import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import edu.bgu.dsp.wordrelatedness.domain.WordPairMapWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;


public class Job2 extends Configured implements Tool {

    private static class JobMapper extends Mapper<Text, LongWritable, Text, WordPairMapWritable> {
        // Map for writing to context
        WordPairMapWritable toWrite = new WordPairMapWritable();

        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            String[] words = key.toString().split(",");
            // key : <year,word1,word2>
            // value : <count>
            String year = words[0];
            String first_word = words[1];
            String second_word = words[2];

            // if word,* : emit(word*, count)
            if (first_word.equals("*") && second_word.equals("*")) {
                toWrite.put(key, value);
                context.write(key, toWrite);
                toWrite.clear();
            } else if (Utils.isStar(second_word)) {
                toWrite.put(key, new Text(value.toString()));
                context.write(key, toWrite);
                toWrite.clear();
                // else if word1,word2 : emit(word1, <word2, count>)
            } else {
                toWrite.put(new Text(year + "," + first_word), new Text(year + "," + second_word + "," + value.toString()));
                context.write(new Text(year + "," + first_word), toWrite);
                toWrite.clear();
            }
        }
    }

    private static class JobReducer extends Reducer<Text, WordPairMapWritable, Text, WordPairMapWritable> {
        String currentStarWord = null;
        LongWritable currentStarCount = null;



        public void reduce(Text key, Iterable<WordPairMapWritable> values, Context context) throws IOException, InterruptedException {
            // Map for writing to context
            WordPairMapWritable toWrite = new WordPairMapWritable();
            // if word * : keep th value in memory
            if (key.toString().contains("*,*")) {
                for (WordPairMapWritable value : values) {
                    context.write(key, value);
                }
                return;
            } else if (Utils.isStar(key.toString())) {
                // Parse and get star word
                String starWord = Utils.getStarWord(key);

                for (WordPairMapWritable value : values) {
                    // Get value(=count) of star word
                    LongWritable starWordCount = Utils.stringToLongWritable(value.get(key).toString());

                    // Keep in map
                    this.currentStarWord = starWord;
                    this.currentStarCount = starWordCount;

                    // emit <word,*|count>
                    toWrite.put(key, starWordCount);
                    context.write(key, toWrite);
                    toWrite.clear();
                }
                return;
            }

            // Else if word1 : <word2,count> : emit(word2, <word1,word2|count>, <word1,*|star_count>)
            for (WordPairMapWritable value : values) {
                String[] year_word_count = value.get(key).toString().split(",");
                // value : <word,count>
                String year = year_word_count[0];
                String word1 = year_word_count[1];
                String word2 = key.toString().substring(5);

                LongWritable count = Utils.stringToLongWritable(year_word_count[2]);

                toWrite.put(new Text(year+","+word2 + "," + word1), count);
                toWrite.put(new Text(year+","+word2 + ",*"), currentStarCount);

                context.write(new Text(year + "," + word1), toWrite);
                System.out.print(year + "," + word1 + "\t");
                System.out.println(Utils.Map_to_string(toWrite));
                toWrite.clear();
            }
        }
    }

    private Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = new Job(new Configuration(), Job2.class.getSimpleName());

        job.setJarByClass(Job2.class);

        job.setMapperClass(JobMapper.class);
//        job.setCombinerClass(NGramsToWordPairs.JobCombiner.class);
//        job.setPartitionerClass(NGramsToWordPairs.JobPartitioner.class);
        job.setReducerClass(JobReducer.class);

        job.setSortComparatorClass(StarComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordPairMapWritable.class);

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
            ToolRunner.run(new Job2(), args);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}