/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rakam.datasource.twitter;

import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;
import com.google.common.base.Throwables;

import java.io.IOException;

public class Classify {
    String[] categories;
    LMClassifier lmc;

    public Classify() {
        try {
            lmc = (LMClassifier) AbstractExternalizable.readResourceObject("/classifier.txt");
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
