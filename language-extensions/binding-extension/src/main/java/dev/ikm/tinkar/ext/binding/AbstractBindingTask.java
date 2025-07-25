package dev.ikm.tinkar.ext.binding;

import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.TrackingCallable;
import dev.ikm.tinkar.coordinate.language.calculator.LanguageCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.entity.*;
import dev.ikm.tinkar.terms.EntityProxy;

import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class AbstractBindingTask extends TrackingCallable<Void> {

    private final Stream.Builder<Entity<? extends EntityVersion>> conceptStreamBuilder = Stream.builder();
    private final Stream.Builder<Entity<? extends EntityVersion>> patternStreamBuilder = Stream.builder();
    private final Stream.Builder<Entity<? extends EntityVersion>> semanticStreamBuilder = Stream.builder();
    protected final Stream<Entity<? extends EntityVersion>> concepts;
    protected final Stream<Entity<? extends EntityVersion>> patterns;
    protected final Stream<Entity<? extends EntityVersion>> semantics;
    protected final Consumer<String> outputConsumer;
    protected final LanguageCalculator languageCalculator;
    protected final StampCalculator stampCalculator;

    public AbstractBindingTask(Stream<Entity<? extends EntityVersion>> entities,
                               Consumer<String> outputConsumer,
                               LanguageCalculator languageCalculator,
                               StampCalculator stampCalculator){
        this.concepts = entities.filter(entity -> entity instanceof ConceptEntity<? extends ConceptEntityVersion>);
        this.patterns = entities.filter(entity -> entity instanceof PatternEntity<? extends PatternEntityVersion>);
        this.semantics = entities.filter(entity -> entity instanceof SemanticEntity<? extends SemanticEntityVersion>);
        this.outputConsumer = outputConsumer;
        this.languageCalculator = languageCalculator;
        this.stampCalculator = stampCalculator;
    }

    public AbstractBindingTask(EntityProxy.Concept module,
                               Consumer<String> outputConsumer,
                               LanguageCalculator languageCalculator,
                               StampCalculator stampCalculator){
        PrimitiveData.get().forEachStampNid(stampNid -> {
            stampCalculator.latest(stampNid).ifPresent(stampEntityVersion -> {
                if (stampEntityVersion.moduleNid() == module.nid()) {
                    PrimitiveData.get().forEachConceptNid(conceptNid -> aggregateEntitiesBySTAMP(conceptNid, stampNid, conceptStreamBuilder));
                    PrimitiveData.get().forEachPatternNid(patternNid -> aggregateEntitiesBySTAMP(patternNid, stampNid, patternStreamBuilder));
                    PrimitiveData.get().forEachSemanticNid(semanticNid -> aggregateEntitiesBySTAMP(semanticNid, stampNid, semanticStreamBuilder));
                }
            });
        });

        this.concepts = conceptStreamBuilder.build();
        this.patterns = patternStreamBuilder.build();
        this.semantics = semanticStreamBuilder.build();
        this.outputConsumer = outputConsumer;
        this.languageCalculator = languageCalculator;
        this.stampCalculator = stampCalculator;
    }

    public AbstractBindingTask(EntityProxy.Pattern pattern,
                               Consumer<String> outputConsumer,
                               LanguageCalculator languageCalculator,
                               StampCalculator stampCalculator){
        PrimitiveData.get().forEachSemanticNidOfPattern(pattern.nid(), semanticNid -> {
            Latest<SemanticEntityVersion> semanticEntityVersionLatest = stampCalculator.latest(semanticNid);
            if (semanticEntityVersionLatest.isPresent()) {
                SemanticEntityVersion semanticEntityVersion = semanticEntityVersionLatest.get();
                int referencedComponentNid = semanticEntityVersion.referencedComponentNid();
                stampCalculator.latest(referencedComponentNid).ifPresent(entityVersion -> {
                    switch (entityVersion.versionDataType()) {
                        case CONCEPT_VERSION -> conceptStreamBuilder.add( (Entity<? extends EntityVersion>) entityVersion.chronology());
                        case SEMANTIC_VERSION -> semanticStreamBuilder.add( (Entity<? extends EntityVersion>) entityVersion.chronology());
                        case PATTERN_VERSION -> patternStreamBuilder.add( (Entity<? extends EntityVersion>) entityVersion.chronology());
                    }
                });
            }
        });

        this.concepts = conceptStreamBuilder.build();
        this.patterns = patternStreamBuilder.build();
        this.semantics = semanticStreamBuilder.build();
        this.outputConsumer = outputConsumer;
        this.languageCalculator = languageCalculator;
        this.stampCalculator = stampCalculator;
    }

    private void aggregateEntitiesBySTAMP(int nid, int stampNid, Stream.Builder<Entity<? extends EntityVersion>> streamBuilder) {
        Entity<? extends EntityVersion> entity = EntityService.get().getEntityFast(nid);
        boolean hasStamp = entity.stampNids().contains(stampNid);
        if (hasStamp) {
            streamBuilder.add(entity);
        }
    }

    public abstract void addData(Stream<Entity<? extends EntityVersion>> entities);
    public abstract void addData(EntityProxy.Concept module);
    public abstract void addData(EntityProxy.Pattern pattern);

}
