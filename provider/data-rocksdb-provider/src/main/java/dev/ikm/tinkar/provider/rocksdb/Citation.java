package dev.ikm.tinkar.provider.rocksdb;

import java.io.Serializable;

public record Citation(int semanticNid, int patternNid ) implements Serializable {
}
