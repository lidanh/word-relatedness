package edu.bgu.dsp.wordrelatedness.domain;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPair implements Writable, WritableComparable<WordPair> {
    public static String WildCard = "*";

    private Text w1;
    private Text w2;
    private IntWritable decade;
    private LongWritable n;
    private LongWritable c;

    /**
     * Created by lidanh on 10/06/2016.
     */
    public WordPair(Text w1, Text w2, IntWritable decade) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = decade;
    }

    private WordPair() {
        this.w1 = new Text();
        this.w2 = new Text();
        this.decade = new IntWritable();
    }

    public boolean isTotalForDecade() {
        return w1.toString().equals(WildCard) && w2.toString().equals(WildCard);
    }

    public WordPair(String w1, String w2, Integer decade) {
        this(new Text(w1), new Text(w2), new IntWritable(decade));
    }

    public int compareTo(WordPair other) {
        int decadeCompare = decade.compareTo(other.getDecade());
        if (decadeCompare == 0) {
            int w1Compare = w1.compareTo(other.getW1());
            if (w1Compare == 0) {
                return w2.compareTo(other.getW2());
            } else {
                return w1Compare;
            }
        } else {
            return decadeCompare;
        }
    }

    public static WordPair read(DataInput in) throws IOException {
        WordPair wordPair = new WordPair();
        wordPair.readFields(in);
        return wordPair;
    }

    public void write(DataOutput out) throws IOException {
        w1.write(out);
        w2.write(out);
        decade.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        w1.readFields(in);
        w2.readFields(in);
        decade.readFields(in);
    }

    @Override
    public String toString() {
        return String.format("%d,%s,%s", decade.get(), w1.toString(), w2.toString());
    }

//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        WordPair wordPair = (WordPair) o;
//
//        if (w2 != null ? !w2.equals(wordPair.w2) : wordPair.w2 != null) return false;
//        if (w1 != null ? !w1.equals(wordPair.w1) : wordPair.w1 != null) return false;
//
//        return true;
//    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordPair wordPair = (WordPair) o;

        if (w1 != null ? !w1.toString().equals(wordPair.w1.toString()) : wordPair.w1 != null) return false;
        if (w2 != null ? !w2.toString().equals(wordPair.w2.toString()) : wordPair.w2 != null) return false;
        return decade != null ? decade.get() == wordPair.decade.get() : wordPair.decade == null;

    }

    @Override
    public int hashCode() {
        int result = w1 != null ? w1.hashCode() : 0;
        result = 31 * result + (w2 != null ? w2.hashCode() : 0);
        result = 31 * result + (decade != null ? decade.hashCode() : 0);
        return result;
    }

    public Text getW1() {
        return w1;
    }

    public Text getW2() {
        return w2;
    }

    public IntWritable getDecade() {
        return decade;
    }

    public LongWritable getN() {
        return n;
    }

    public LongWritable getC() {
        return c;
    }

    public void setN(LongWritable n) {
        this.n = n;
    }

    public void setC(LongWritable c) {
        this.c = c;
    }
}