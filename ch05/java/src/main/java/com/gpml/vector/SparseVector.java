package com.gpml.vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SparseVector  {

    private Integer cardinality;
    private List<Long> indexes;
    private List<Float> values;

    public SparseVector() {
    }

    public SparseVector(int cardinality, List<Long> indexes, List<Float> values) {
        this.cardinality = cardinality;
        this.indexes = indexes;
        this.values = values;
    }

    public static SparseVector fromMap(Map<Long, Float> map) {
        int cardinality = map.size();
        List<Long> index = map.keySet().stream()
                .sorted()
                .collect(Collectors.toList());
        List<Float> values = IntStream.range(0, cardinality)
                .mapToObj(i -> map.get(index.get(i)))
                .collect(Collectors.toList());
        return new SparseVector(cardinality, index, values);
    }

    public static SparseVector fromList(List<Float> vector) {
        int cardinality = vector.get(0).intValue();
        List<Long> indexes = vector.subList(1, cardinality + 1).stream().map((x) -> x.longValue()).collect(Collectors.toList());
        List<Float> values = vector.subList(cardinality + 1, 2 * cardinality + 1).stream().collect(Collectors.toList());
        return new SparseVector(cardinality, indexes, values);
    }

    public void setArray(float[] vector) {
        this.cardinality = Float.valueOf(vector[0]).intValue();
        this.indexes = new ArrayList<>();
        this.values = new ArrayList<>();
        for (int i = 1; i < vector.length; i++) {
            if (i >= 1 && i < cardinality + 1) {
                indexes.add(Float.valueOf(vector[i]).longValue());
            } else {
                values.add(vector[i]);
            }
        }
    }

    public List<Float> getList() {
        List<Float> vectorAsList = new ArrayList<>(Collections.nCopies(cardinality * 2 + 1, 0.0f));
        vectorAsList.set(0, cardinality.floatValue());
        final int offset = cardinality;
        IntStream.range(0, cardinality).forEach((k) -> {
            float pos = indexes.get(k).floatValue();
            Float value = values.get(k);
            vectorAsList.set(k + 1, pos);
            vectorAsList.set(offset + 1 + k, value);
        });
        return vectorAsList;
    }

    public float[] getArray() {
        float[] vector = new float[cardinality * 2 + 1];
        vector[0] = cardinality.floatValue();
        final int offset = cardinality;
        IntStream.range(0, cardinality).forEach((k) -> {
            float pos = indexes.get(k).floatValue();
            Float value = values.get(k);
            vector[k + 1] = pos;
            vector[offset + 1 + k] = value;
        });
        return vector;
    }

    public Integer getCardinality() {
        return cardinality;
    }

    public List<Long> getIndexes() {
        return indexes;
    }

    public List<Float> getValues() {
        return values;
    }

    public float dot(SparseVector other) {

        SparseVector otherSparseVector = other;

        if (this.values == null ||
                this.cardinality == 0 ||
                otherSparseVector.values == null ||
                otherSparseVector.cardinality == 0) {
            return 0f;
        }

        final AtomicReference<Float> sum = new AtomicReference<>(0f);
        int xIndex = 0;
        int yIndex = 0;

        while (true) {
            if (indexes.get(xIndex).longValue() == otherSparseVector.getIndexes().get(yIndex).longValue()) {
                float curValue = sum.get();
                curValue += values.get(xIndex) * otherSparseVector.getValues().get(yIndex);
                sum.set(curValue);
                xIndex++;
                yIndex++;
            } else if (indexes.get(xIndex) > otherSparseVector.getIndexes().get(yIndex)) {
                yIndex++;
            } else {
                xIndex++;
            }
            if (xIndex == cardinality
                    || yIndex == otherSparseVector.getCardinality()) {
                break;
            }
        }
        return sum.get();
    }

    public float norm() {
        return Double.valueOf(Math.sqrt(this.dot(this))).floatValue();
    }

    public static float getCosinceSimilarity(SparseVector xVector, SparseVector yVector) {
        float a = xVector.dot(yVector);
        float b = xVector.norm() * yVector.norm();
        if (b > 0) {
            return a / b;
        } else {
            return 0f;
        }
    }

    @Override
    public String toString() {
        return getList().toString(); //To change body of generated methods, choose Tools | Templates.
    }
}
