package com.gpml.reco.cf;

import java.util.HashMap;
import java.util.Map;

public class SimilarityItem implements Comparable<SimilarityItem> {

    private final String firstNode;
    private final String secondNode;
    private float similarity;

    public SimilarityItem(String firstNode, String secondNode, float sim) {
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.similarity = sim;
    }

    public String getFirstNode() {
        return firstNode;
    }

    public String getSecondNode() {
        return secondNode;
    }

    public float getSimilarity() {
        return similarity;
    }

    public void setSimilarity(float similarity) {
        this.similarity = similarity;
    }


    @Override
    public int compareTo(SimilarityItem o) {
        if (o == null) {
            return 1;
        }

        if (this.getSimilarity() > o.getSimilarity()) {
            return 1;
        }
        if (this.getSimilarity() == o.getSimilarity()) {
            return 0;
        }
        return -1;
    }
}
