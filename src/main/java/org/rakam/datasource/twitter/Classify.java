package org.rakam.datasource.twitter;

import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;
import com.google.common.base.Throwables;

import java.io.File;
import java.io.IOException;

public class Classify {
    String[] categories;
    LMClassifier lmc;

    public Classify() {
        try {

            lmc = (LMClassifier) AbstractExternalizable.readObject(new File("classifier.txt"));
            categories = lmc.categories();
        } catch (ClassNotFoundException|IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public boolean isPositive(String text) {
        ConditionalClassification classification = lmc.classify(text);
        return classification.bestCategory().equals("Positive");
    }
}
