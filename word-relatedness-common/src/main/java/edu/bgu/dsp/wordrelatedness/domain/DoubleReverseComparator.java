package edu.bgu.dsp.wordrelatedness.domain;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by lidanh on 14/06/2016.
 */
public class DoubleReverseComparator extends WritableComparator {
    protected DoubleReverseComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -1 * super.compare(a, b);
    }
}