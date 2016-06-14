package edu.bgu.dsp.wordrelatedness.domain;

public class WordsPair {
    private final double score;
    private final String pair;

    public WordsPair(String pair, double score) {
        this.pair = pair;
        this.score = score;
    }

    public double getScore() {
        return score;
    }

    public String getPair() {
        return pair;
    }
}
