/*
 * Copyright 2020-2021 HL7.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package org.hl7.tinkar.component;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.hl7.tinkar.dto.*;

/**
 *
 * @author kec
 */
public interface SemanticVersion extends Version, Semantic {

    ImmutableList<Object> getFields();

    default SemanticVersionDTO toChangeSetThing() {
        MutableList<Object> convertedFields = Lists.mutable.empty();
        convertedFields.forEach(objectToConvert -> {
            if (objectToConvert instanceof Concept concept) {
                convertedFields.add(new ConceptDTO(concept.getComponentUuids()));
            } else if (objectToConvert instanceof DefinitionForSemantic definitionForSemantic) {
                convertedFields.add(new DefinitionForSemanticDTO(definitionForSemantic.getComponentUuids()));
            } else if (objectToConvert instanceof Semantic semantic) {
                convertedFields.add(new SemanticDTO(semantic.getComponentUuids(), semantic.getDefinitionForSemantic(),
                        semantic.getReferencedComponent()));
            } else if (objectToConvert instanceof Number number) {
                convertedFields.add(number);
            } else if (objectToConvert instanceof String string) {
                convertedFields.add(string);
            } else {
                throw new UnsupportedOperationException("Can't convert:\n  " + objectToConvert + "\nin\n  " + this);
            }
        });
        return new SemanticVersionDTO(getComponentUuids(),
                getReferencedComponent().getComponentUuids(),
                getDefinitionForSemantic().getComponentUuids(),
                getStamp().toChangeSetThing(), convertedFields.toImmutable());
    }

}
