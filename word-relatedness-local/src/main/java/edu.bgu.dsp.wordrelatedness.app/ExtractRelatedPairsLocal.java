package edu.bgu.dsp.wordrelatedness.app;

import edu.bgu.dsp.wordrelatedness.jobs.NGramsToWordPairs;
import edu.bgu.dsp.wordrelatedness.jobs.WordPairsPMICalc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtractRelatedPairsLocal extends Configured implements Tool {
    private static final String Input = "input";
    private static final String IntermediateOutput = "inter_output";
    private static final String FinalOutput = "final_output";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage:\njava -jar ExtractRelatedPairs.jar <k>");
            return;
        }

        try {
            Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.err.println("Usage:\njava -jar ExtractRelatedPairs.jar <k>\nk argument must be a valid number!");
            return;
        }

        try {
            ToolRunner.run(new Configuration(), new ExtractRelatedPairsLocal(), args);
        } catch (Exception e) {
            System.err.println("Error during running the job: \n" + e.getMessage());
        }
    }

    public int run(String[] args) throws Exception {
        NGramsToWordPairs.main(new String[] { Input, IntermediateOutput});

        ToolRunner.run(getConf(), new WordPairsPMICalc(), new String[] {IntermediateOutput, FinalOutput, args[0]});

        return 0;
    }
}
