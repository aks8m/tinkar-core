package org.hl7.tinkar.coordinate.logic;


import java.util.Objects;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.hl7.tinkar.common.binary.Decoder;
import org.hl7.tinkar.common.binary.DecoderInput;
import org.hl7.tinkar.common.binary.Encoder;
import org.hl7.tinkar.common.binary.EncoderOutput;
import org.hl7.tinkar.common.service.PrimitiveData;
import org.hl7.tinkar.component.Concept;
import org.hl7.tinkar.coordinate.ImmutableCoordinate;
import org.hl7.tinkar.entity.Entity;
import org.hl7.tinkar.terms.ConceptFacade;
import org.hl7.tinkar.terms.PatternFacade;

@RecordBuilder
public record LogicCoordinateRecord(int classifierNid,
                                    int descriptionLogicProfileNid,
                                    int inferredAxiomsPatternNid,
                                    int statedAxiomsPatternNid,
                                    int conceptMemberPatternNid,
                                    int statedNavigationPatternNid,
                                    int inferredNavigationPatternNid,
                                    int rootNid)
        implements LogicCoordinate, ImmutableCoordinate, LogicCoordinateRecordBuilder.With {


    private static final int marshalVersion = 2;


    @Override
    @Encoder
    public void encode(EncoderOutput out) {
        out.writeNid(this.classifierNid);
        out.writeNid(this.descriptionLogicProfileNid);
        out.writeNid(this.inferredAxiomsPatternNid);
        out.writeNid(this.statedAxiomsPatternNid);
        out.writeNid(this.conceptMemberPatternNid);
        out.writeNid(this.statedNavigationPatternNid);
        out.writeNid(this.inferredNavigationPatternNid);
        out.writeNid(this.rootNid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogicCoordinate that)) return false;
        return classifierNid() == that.classifierNid() &&
                descriptionLogicProfileNid() == that.descriptionLogicProfileNid() &&
                inferredAxiomsPatternNid() == that.inferredAxiomsPatternNid() &&
                statedAxiomsPatternNid() == that.statedAxiomsPatternNid() &&
                conceptMemberPatternNid() == that.conceptMemberPatternNid() &&
                statedNavigationPatternNid() == that.statedNavigationPatternNid() &&
                inferredNavigationPatternNid() == that.inferredNavigationPatternNid() &&
                rootNid() == that.rootNid();
    }

    @Override
    public int hashCode() {
        return Objects.hash(classifierNid(), descriptionLogicProfileNid(), inferredAxiomsPatternNid(),
                statedAxiomsPatternNid(), conceptMemberPatternNid(), statedNavigationPatternNid(),
                inferredNavigationPatternNid(), rootNid());
    }

    public static LogicCoordinateRecord make(int classifierNid,
                                             int descriptionLogicProfileNid,
                                             int inferredAxiomsPatternNid,
                                             int statedAxiomsPatternNid,
                                             int conceptMemberPatternNid,
                                             int statedNavigationPatternNid,
                                             int inferredNavigationPatternNid,
                                             int rootNid)  {
        return new LogicCoordinateRecord(classifierNid, descriptionLogicProfileNid,
                        inferredAxiomsPatternNid, statedAxiomsPatternNid, conceptMemberPatternNid, statedNavigationPatternNid,
                        inferredNavigationPatternNid, rootNid);
    }

    public static LogicCoordinateRecord make(ConceptFacade classifier,
                                             ConceptFacade descriptionLogicProfile,
                                             PatternFacade inferredAxiomsPattern,
                                             PatternFacade statedAxiomsPattern,
                                             PatternFacade conceptMemberPattern,
                                             PatternFacade statedNavigationPattern,
                                             PatternFacade inferredNavigationPattern,
                                             ConceptFacade root)  {
        return new LogicCoordinateRecord(classifier.nid(), descriptionLogicProfile.nid(),
                        inferredAxiomsPattern.nid(), statedAxiomsPattern.nid(), conceptMemberPattern.nid(), statedNavigationPattern.nid(),
                        inferredNavigationPattern.nid(), root.nid());
    }

    @Decoder
    public static LogicCoordinateRecord decode(DecoderInput in) {
        int objectMarshalVersion = in.encodingFormatVersion();
        switch (objectMarshalVersion) {
            case 1:
            case marshalVersion:
                return new LogicCoordinateRecord(in.readNid(), in.readNid(), in.readNid(), in.readNid(),
                        in.readNid(), in.readNid(), in.readNid(), in.readNid());
            default:
                throw new UnsupportedOperationException("Unsupported version: " + objectMarshalVersion);
        }
    }

    @Override
    public String toString() {
        return "LogicCoordinateImpl{" +
                "stated axioms: " + PrimitiveData.text(this.statedAxiomsPatternNid) + "<" + this.statedAxiomsPatternNid + ">,\n" +
                "inferred axioms: " + PrimitiveData.text(this.inferredAxiomsPatternNid) + "<" + this.inferredAxiomsPatternNid + ">, \n" +
                "profile: " + PrimitiveData.text(this.descriptionLogicProfileNid) + "<" + this.descriptionLogicProfileNid + ">, \n" +
                "classifier: " + PrimitiveData.text(this.classifierNid) + "<" + this.classifierNid + ">, \n" +
                "concept members: " + PrimitiveData.text(this.conceptMemberPatternNid) + "<" + this.conceptMemberPatternNid + ">, \n" +
                "stated navigation: " + PrimitiveData.text(this.statedNavigationPatternNid) + "<" + this.statedNavigationPatternNid + ">, \n" +
                "inferred navigation: " + PrimitiveData.text(this.inferredNavigationPatternNid) + "<" + this.inferredNavigationPatternNid + ">, \n" +
                "root:" + PrimitiveData.text(this.rootNid) + "<" + this.rootNid + ">,\n" +
        "}";
    }
    @Override
    public LogicCoordinateRecord toLogicCoordinateRecord() {
        return this;
    }


}
