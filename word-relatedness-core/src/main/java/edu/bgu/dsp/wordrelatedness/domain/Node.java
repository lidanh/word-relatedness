package edu.bgu.dsp.wordrelatedness.domain;

/**
 * Created by lidanh on 11/06/2016.
 */
public class Node<T> implements Comparable<Node<T>> {
    T key;
    double value;

    public Node(T key, double value) {
        this.key = key;
        this.value = value;
    }

    public int compareTo(Node<T> other) {
        if (this.value >= other.value) {
            return 1;
        } else {
            return -1;
        }
    }

    public T getKey() {
        return key;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Node{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
