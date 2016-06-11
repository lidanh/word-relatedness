package edu.bgu.dsp.wordrelatedness.domain;

import java.util.TreeSet;

/**
 * Created by lidanh on 11/06/2016.
 */
public class KTree<T> extends TreeSet<Node<T>> {
    private static final long serialVersionUID = 5580822628684386902L;
    private int k;

    public KTree(int k) {
        super();
        this.k = k;
    }

    @Override
    public boolean add(Node<T> node) {
        boolean isSucess = super.add(node);

        if (super.size() > k) {
            pollFirst();
        }

        return isSucess;
    }
}

