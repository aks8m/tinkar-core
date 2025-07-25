package dev.ikm.tinkar.provider.rocksdb;

public enum ColumnFamily {

	NID_TO_CONCEPT_MAP(1, "nid-to-concept-map"),
	NID_TO_SEMANTIC_MAP(2, "nid-to-semantic-map"),
	NID_TO_PATTERN_MAP(3, "nid-to-pattern-map"),
	NID_TO_STAMP_MAP(4, "nid-to-stamp-map"),
	UUID_TO_NID_MAP(5, "uuid-to-nid-map"),
	NID_TO_CITATION_MAP(6, "nid-to-citation-map"),
	NID_TO_SEMANTIC_NIDS_MAP(7, "nid-to-semantic-nids-map");


	private final int index;
	private final String name;

	ColumnFamily(int index, String name) {
		this.index = index;
		this.name = name;
	}

	public int getIndex() {
		return index;
	}

	public String getName() {
		return name;
	}

	public byte[] getBytes() {
		return name.getBytes();
	}
}
