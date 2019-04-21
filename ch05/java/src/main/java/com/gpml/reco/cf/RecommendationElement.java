package com.gpml.reco.cf;

public class RecommendationElement implements Comparable<RecommendationElement> {

    private final String otherNode;
    private float score;

    public RecommendationElement(String otherNode, float score) {
        this.otherNode = otherNode;
        this.score = score;
    }

    public String getOtherNode() {
        return otherNode;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    @Override
    public int compareTo(RecommendationElement o) {
        if (o == null) {
            return 1;
        }

        if (this.getScore() > o.getScore()) {
            return 1;
        }
        if (this.getScore() == o.getScore()) {
            return 0;
        }
        return -1;
    }
}
