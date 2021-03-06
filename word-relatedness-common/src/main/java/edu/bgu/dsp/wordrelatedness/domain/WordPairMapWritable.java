package edu.bgu.dsp.wordrelatedness.domain;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

/**
 * Created by lidanh on 13/06/2016.
 */
public class WordPairMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Writable, Writable> entry : entrySet()) {
            sb.append(String.format("[%s -> %s],", entry.getKey(), entry.getValue()));
        }

        return sb.toString();
    }
}
