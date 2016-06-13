package edu.bgu.dsp.wordrelatedness.old.jobs;

import edu.bgu.dsp.wordrelatedness.domain.KTree;
import edu.bgu.dsp.wordrelatedness.domain.Node;
import edu.bgu.dsp.wordrelatedness.domain.WordPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by lidanh on 11/06/2016.
 */
public class WordPairsPMICalc extends Configured implements Tool {
    static class JobMapper extends Mapper<WordPair, LongWritable, WordPair, LongWritable> {
        @Override
        protected void map(WordPair key, LongWritable value, Context context) throws IOException, InterruptedException {
            if (key.getW2().toString().equals(WordPair.WildCard)) {
                // <w, *> => <w, *>
                context.write(key, value);
            } else {
                // <w1, w2> => <w2, w1>
                WordPair wp = new WordPair(key.getW2(), key.getW1(), key.getDecade());
                context.write(wp, value);
            }
        }
    }

    static class JobPartitioner extends Partitioner<WordPair, LongWritable> {
        public int getPartition(WordPair wordPair, LongWritable longWritable, int numPartitions) {
            return wordPair.getDecade().get() % numPartitions;
        }
    }

    static class JobReducer extends Reducer<WordPair, LongWritable, KTree<WordPair>, LongWritable> {
        private LongWritable output = new LongWritable();
        private int k = 0;
        private long c2 = 0;
        private KTree<WordPair> pmiTree;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            k = context.getConfiguration().getInt("k", 10);
            pmiTree = new KTree<WordPair>(k);
        }

        private double pmi(WordPair pair, long total) {
            // TODO

            return 0.0;
        }

        @Override
        protected void reduce(WordPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;

            for (LongWritable value : values) {
                total += value.get();
            }
            output.set(total);

            double pmi = pmi(key, total);
            pmiTree.add(new Node<WordPair>(key, pmi));

            context.write(pmiTree, output); // TODO: check
        }
    }

    static class JobOutputFormat extends FileOutputFormat<KTree<WordPair>, LongWritable> {
        public RecordWriter<KTree<WordPair>, LongWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Path path = FileOutputFormat.getOutputPath(taskAttemptContext);

            Path fullPath = new Path(path, "output_" + taskAttemptContext.getTaskAttemptID().getTaskID().getId());

            FileSystem fs = path.getFileSystem(taskAttemptContext.getConfiguration());
            FSDataOutputStream fileOut = fs.create(fullPath, taskAttemptContext);
            return new JobRecordWriter(fileOut);
        }
    }

    static class JobRecordWriter extends RecordWriter<KTree<WordPair>, LongWritable> {
        private DataOutputStream output;

        public JobRecordWriter(DataOutputStream output) {
            this.output = output;
        }

        public void write(KTree<WordPair> tree, LongWritable longWritable) throws IOException, InterruptedException {
            WordPair firstPair = tree.first().getKey();
            int decade = firstPair.getDecade().get();

            output.writeBytes(String.format("Decade: %d\n", decade));

            Node<WordPair> currentNode;
            int i = 0;
            while (!tree.isEmpty()) {
                currentNode = tree.pollLast();
                output.writeBytes(String.format("(%s) = %f", currentNode.getKey().toString(), currentNode.getValue()));
            }

            output.writeBytes("\n\n");
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            output.close();
        }
    }

    public Job getJobWiring(String inputPath, String outputPath, String k) throws IOException {
        Configuration conf = new Configuration();
        conf.set("k", k);

        Job job = Job.getInstance(getConf(), WordPairsPMICalc.class.getSimpleName());
        job.setJarByClass(WordPairsPMICalc.class);
        job.setMapperClass(WordPairsPMICalc.JobMapper.class);
        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setPartitionerClass(WordPairsPMICalc.JobPartitioner.class);

        job.setOutputKeyClass(KTree.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(JobOutputFormat.class);

        job.setInputFormatClass(SequenceFileInputFormat.class); // LZO Compressed files

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public int run(String[] args) throws Exception {
        System.out.println("Input directory: " + args[0]);
        System.out.println("Output directory: " + args[1]);
        System.out.println("k: " + args[2]);

        Job job = getJobWiring(args[0], args[1], args[2]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
