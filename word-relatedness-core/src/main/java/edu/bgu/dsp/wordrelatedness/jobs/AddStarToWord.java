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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;


public class AddStarToWord extends Configured implements Tool {

    static class JobMapper extends Mapper<WordPair, LongWritable, WordPair, WordPairMapWritable> {
        // Map for writing to context
        WordPairMapWritable toWrite = new WordPairMapWritable();

        public void map(WordPair key, LongWritable value, Context context) throws IOException, InterruptedException {
            // if key == *-* :  emit (*-*| <*-*, count>)
            if (key.isTotalForDecade()) {
                toWrite.put(key, value);
                context.write(key, toWrite);
            } else if (key.getW2().toString().equals(WordPair.WildCard)) {
                // else if key == word-* : emit(word-*| <word-*, count>)
                toWrite.put(key, value);
                context.write(key, toWrite);
            } else {
                // else if key == word1,word2 : emit(word1,word1| <word1-word2, count>)
                toWrite.put(key, value);
                context.write(key, toWrite);
            }
            toWrite.clear();
        }
    }

    /**
     *
     *  if key == *-*
             emit (*-*| <*-*, count>)
        else if key == word-*
             emit(word-*| <word-*, count>)
        else if key == word1-word2
             emit(word2| <word1-word2, count>, <word1-*, count>)
     */
    static class JobReducer extends Reducer<WordPair, WordPairMapWritable, WordPair, WordPairMapWritable> {
        Text currentStarWord = null;
        LongWritable currentStarCount = null;

        public void reduce(WordPair key, Iterable<WordPairMapWritable> values, Context context) throws IOException, InterruptedException {
            for (WordPairMapWritable value : values) {
                // if key == *-* : emit (*-*| <*-*, count>)
                if (key.isTotalForDecade()) {
                    context.write(key, value);
                    return;
                }
                // else if key == word-* : emit(word-*| <word-*, count>)
                if (key.isStar()){
                    this.currentStarWord = key.getW1();
                    this.currentStarCount = (LongWritable) value.get(key);
                    context.write(key, value);
                    return;
                }
                // else if key == word1-word2 : emit(word2,word2| <word1-word2, count>, <word1-*, count>)
                WordPair W1W2 = new WordPair(key.getW2(), key.getW1(), key.getDecade());
                value.put(currentStarWord, currentStarCount);
                context.write(W1W2, value);
            }
        }
    }

    private Job getJobWiring(String inputPath, String outputPath) throws IOException {
        Job job = new Job(new Configuration(), AddStarToWord.class.getSimpleName());

        job.setJarByClass(AddStarToWord.class);

        job.setMapperClass(JobMapper.class);
//        job.setCombinerClass(ExtractRelatedPairs.JobCombiner.class);
//        job.setPartitionerClass(ExtractRelatedPairs.JobPartitioner.class);
        job.setReducerClass(JobReducer.class);


        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(WordPairMapWritable.class);

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
            ToolRunner.run(new AddStarToWord(), args);
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}