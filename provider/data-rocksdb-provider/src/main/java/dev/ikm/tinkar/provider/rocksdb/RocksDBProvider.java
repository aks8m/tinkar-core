package dev.ikm.tinkar.provider.rocksdb;

import dev.ikm.tinkar.common.alert.AlertStreams;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.service.DataActivity;
import dev.ikm.tinkar.common.service.NidGenerator;
import dev.ikm.tinkar.common.service.PrimitiveDataSearchResult;
import dev.ikm.tinkar.common.service.PrimitiveDataService;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.service.TinkExecutor;
import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.entity.ConceptEntity;
import dev.ikm.tinkar.entity.PatternEntity;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.provider.search.Indexer;
import dev.ikm.tinkar.provider.search.RecreateIndex;
import dev.ikm.tinkar.provider.search.Searcher;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ObjIntConsumer;

public class RocksDBProvider implements PrimitiveDataService, NidGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBProvider.class.getSimpleName());

	protected static RocksDBProvider INSTANCE;
	private static final File defaultDataDirectory = new File("target/rocksdb/");
	private static final String databaseDirectoryName = "rocksdb";
	protected LongAdder writeSequence = new LongAdder();
	private final AtomicInteger nextNid;

	private final RocksDB rocksDB;
	private final List<ColumnFamilyHandle> columnFamilyHandler;
	private final Convert convert;

	private final Indexer indexer;
	private final Searcher searcher;

	public RocksDBProvider() throws IOException {
		Stopwatch stopwatch = new Stopwatch();
		LOG.info("Opening RocksDBProvider");
		//Get database root directory and create if necessary
		File configuredRoot = ServiceProperties.get(ServiceKeys.DATA_STORE_ROOT, defaultDataDirectory);
		configuredRoot.mkdirs();

		//Get rocksdb database directory and create if necessary
		File databaseDirectory = new File(configuredRoot, databaseDirectoryName);
		databaseDirectory.mkdirs();
		LOG.info("RocksDBProvider opening on directory: {}", databaseDirectory.getAbsolutePath());

		//Load RocksDB library
		RocksDB.loadLibrary();

		//Setup for using Column Families to handle different Tinkar Component Objects
		ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
		final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
		columnFamilyDescriptors.addFirst(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
		for (ColumnFamily columnFamily : ColumnFamily.values()) {
			columnFamilyDescriptors.add(columnFamily.getIndex(), new ColumnFamilyDescriptor(columnFamily.getBytes(), columnFamilyOptions));
		}
		columnFamilyHandler = new ArrayList<>();

		//Setup database options
		final DBOptions dbOptions = new DBOptions()
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true);

		//Instantiate RocksDB conversion helper class
		this.convert = new Convert();

		//Create instance of RocksDB
		try {
			rocksDB = RocksDB.open(dbOptions, databaseDirectory.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandler);
			RocksDBProvider.INSTANCE = this;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}

		//Find next Nid to be used when writing new components to the database
		this.nextNid = new AtomicInteger(findLastNid());

		stopwatch.stop();
		LOG.info("Opened RocksDBProvider in {}", stopwatch.durationString());

		//Setup for Lucene Index and Search
		File indexDir = new File(configuredRoot, "lucene");
		this.indexer = new Indexer(indexDir.toPath());
		this.searcher = new Searcher();
	}

	private ColumnFamilyHandle handle(ColumnFamily columnFamily) {
		return columnFamilyHandler.get(columnFamily.getIndex());
	}

	@Override
	public int newNid() {
		return nextNid.getAndIncrement();
	}

	@Override
	public long writeSequence() {
		return writeSequence.sum();
	}

	@Override
	public void close() {
		Stopwatch stopwatch = new Stopwatch();
		LOG.info("Closing RocksDBProvider");

		try {
			for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandler) {
				columnFamilyHandle.close();
			}
			rocksDB.close();
			indexer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			stopwatch.stop();
			LOG.info("Closing RocksDBProvider in {}", stopwatch.durationString());
		}
	}

	public void save() {
		try {
			indexer.commit();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private int findLastNid(){
		int lastUsedNid = Integer.MIN_VALUE;
		for(ColumnFamily columnFamily : ColumnFamily.values()) {
			int lastNidFromColumn = iterateToLastEntryAndGetKey(columnFamily);
			if(lastNidFromColumn > lastUsedNid) {
				lastUsedNid = lastNidFromColumn;
			}
		}
		return lastUsedNid;
	}

	private int iterateToLastEntryAndGetKey(ColumnFamily columnFamily) {
		int lastNid = Integer.MIN_VALUE;
		try (RocksIterator iterator = rocksDB.newIterator(handle(columnFamily))) {
			iterator.seekToLast();
			if (iterator.isValid()) {
				byte[] key = iterator.key();
				if (key != null) {
					lastNid = convert.bytesToInt(key);
				}
			}
		}
		return lastNid;
	}

	@Override
	public int nidForPublicId(PublicId publicId) {
		return nidForUuids(publicId.asUuidList());
	}

	@Override
	public int nidForUuids(UUID... uuids) {
		try {
			if (uuids.length == 0) {
				throw new IllegalStateException("uuidList cannot be empty");
			} else if (uuids.length == 1) {
				byte[] key = convert.uuidToBytes(uuids[0]);
				byte[] nid = rocksDB.get(handle(ColumnFamily.UUID_TO_NID_MAP), key);
				if (nid == null) {
					int newNid = newNid();
					rocksDB.put(
							handle(ColumnFamily.UUID_TO_NID_MAP),
							key,
							convert.integerToBytes(newNid)
					);
					return newNid;
				}
			} else {
				boolean isMissing = false;
				int foundValue = Integer.MIN_VALUE;

				for (UUID uuid : uuids) {
					byte[] key = convert.uuidToBytes(uuid);
					byte[] nid = rocksDB.get(handle(ColumnFamily.UUID_TO_NID_MAP), key);
					if (nid == null) {
						isMissing = true;
					} else {
						if (foundValue == Integer.MIN_VALUE) {
							foundValue = convert.bytesToInt(nid);
						} else {
							if (foundValue != convert.bytesToInt(nid)) {
								String message = "Multiple nids for: " +
										Arrays.stream(uuids).sorted() +
										" first value: " + foundValue +
										" second value: " + convert.bytesToInt(nid);
								throw new IllegalStateException(message);
							}
						}
					}
				}
				if (!isMissing) {
					return foundValue;
				}
				if (foundValue == Integer.MIN_VALUE) {
					foundValue = newNid();
				}
				for (UUID uuid : uuids) {
					byte[] key = convert.uuidToBytes(uuid);
					rocksDB.put(
							handle(ColumnFamily.UUID_TO_NID_MAP),
							key,
							convert.integerToBytes(foundValue)
					);
				}
				return foundValue;
			}
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}

		return Integer.MIN_VALUE;
	}

	@Override
	public int nidForUuids(ImmutableList<UUID> uuidList) {
		return nidForUuids(uuidList.toArray(new UUID[0]));
	}

	@Override
	public boolean hasUuid(UUID uuid) {
		byte[] key = convert.uuidToBytes(uuid);
		return rocksDB.keyExists(
			handle(ColumnFamily.UUID_TO_NID_MAP),
			key
		);
	}

	@Override
	public boolean hasPublicId(PublicId publicId) {
		return publicId.asUuidList().stream().anyMatch(this::hasUuid);
	}

	@Override
	public void forEach(ObjIntConsumer<byte[]> action) {
		iterateWithAction(action, ColumnFamily.NID_TO_CONCEPT_MAP);
		iterateWithAction(action, ColumnFamily.NID_TO_SEMANTIC_MAP);
		iterateWithAction(action, ColumnFamily.NID_TO_PATTERN_MAP);
		iterateWithAction(action, ColumnFamily.NID_TO_STAMP_MAP);
	}

	private void iterateWithAction(ObjIntConsumer<byte[]> action, ColumnFamily columnFamily) {
		try (RocksIterator iterator = rocksDB.newIterator(handle(columnFamily))) {
			iterator.seekToFirst();
			while(iterator.isValid()) {
				byte[] key = iterator.key();
				byte[] value = iterator.value();
				action.accept(value, convert.bytesToInt(key));
				iterator.next();
			}
		}
	}

	@Override
	public void forEachParallel(ObjIntConsumer<byte[]> action) {
		forEach(action);
	}

	@Override
	public void forEachParallel(ImmutableIntList nids, ObjIntConsumer<byte[]> action) {
		nids.forEach(nid -> {
			byte[] key = convert.integerToBytes(nid);
			ColumnFamily columnFamily = getColumnFamilyFromKey(key);
			if (columnFamily != null) {
				iterateWithAction(action, columnFamily);
			}
		});
	}

	@Override
	public byte[] getBytes(int nid) {
		byte[] key = convert.integerToBytes(nid);
		ColumnFamily columnFamily = getColumnFamilyFromKey(key);
		if (columnFamily != null) {
			try {
				return rocksDB.get(handle(columnFamily), key);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	private ColumnFamily getColumnFamilyFromKey(byte[] key) {
		ColumnFamily columnFamily;
		if (rocksDB.keyExists(handle(ColumnFamily.NID_TO_CONCEPT_MAP), key)) {
			return ColumnFamily.NID_TO_CONCEPT_MAP;
		} else if (rocksDB.keyExists(handle(ColumnFamily.NID_TO_SEMANTIC_MAP), key)) {
			return ColumnFamily.NID_TO_SEMANTIC_MAP;
		} else if (rocksDB.keyExists(handle(ColumnFamily.NID_TO_PATTERN_MAP), key)) {
			return ColumnFamily.NID_TO_PATTERN_MAP;
		} else if (rocksDB.keyExists(handle(ColumnFamily.NID_TO_STAMP_MAP), key)) {
			return ColumnFamily.NID_TO_STAMP_MAP;
		}
		return null;
	}


	@Override
	public byte[] merge(int nid, int patternNid, int referencedComponentNid, byte[] value, Object sourceObject, DataActivity activity) {
		//Merge and write to proper column family based on Entity type
		byte[] mergedBytes = switch (sourceObject) {
			case ConceptEntity conceptEntity -> mergeAndWrite(nid, value, ColumnFamily.NID_TO_CONCEPT_MAP);
			case SemanticEntity semanticEntity -> mergeAndWrite(nid, value, ColumnFamily.NID_TO_SEMANTIC_MAP);
			case PatternEntity patternEntity -> mergeAndWrite(nid, value, ColumnFamily.NID_TO_PATTERN_MAP);
			case StampEntity stampEntity -> mergeAndWrite(nid, value, ColumnFamily.NID_TO_STAMP_MAP);
			default -> throw new IllegalArgumentException("Unsupported sourceObject: " + sourceObject);
		};

		if (patternNid != Integer.MAX_VALUE) {
			//Write additional citations data (Component -> Semantic, Pattern
			Citation citation = new Citation(nid, patternNid);
			byte[] nidKey = convert.integerToBytes(referencedComponentNid);
			byte[] existingCitationBytes;
			byte[] newCitationsBytes;
			try {
				existingCitationBytes = rocksDB.get(handle(ColumnFamily.COMPONENT_NID_TO_CITATION_MAP), nidKey);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
			if (existingCitationBytes != null) {
				Citation[] existingCitations = convert.bytesToCitationArray(existingCitationBytes);
				Citation[] newCitations = new Citation[existingCitations.length + 1];
				System.arraycopy(existingCitations, 0, newCitations, 0, existingCitations.length);
				newCitations[newCitations.length - 1] = citation;
				newCitationsBytes = convert.citationArrayToBytes(newCitations);
				try {
					rocksDB.put(handle(ColumnFamily.COMPONENT_NID_TO_CITATION_MAP), nidKey, newCitationsBytes);
				} catch (RocksDBException e) {
					throw new RuntimeException(e);
				}
			} else {
				Citation[] newCitations = new Citation[1];
				newCitations[0] = citation;
				newCitationsBytes = convert.citationArrayToBytes(newCitations);

			}
			try {
				rocksDB.put(handle(ColumnFamily.COMPONENT_NID_TO_CITATION_MAP), nidKey, newCitationsBytes);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}

			//Write/Update Mapping of Semantics to their Patterns
			byte[] patternKey = convert.integerToBytes(patternNid);
			byte[] existingSemanticNidsBytes;
			byte[] newSemanticNidsBytes;
			try {
				existingSemanticNidsBytes = rocksDB.get(handle(ColumnFamily.PATTERN_NID_TO_SEMANTIC_NIDS_MAP), patternKey);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
			if (existingSemanticNidsBytes != null) {
				int[] existingSemanticNids = convert.bytesToIntArray(existingSemanticNidsBytes);
				int[] newSemanticNids = new int[existingSemanticNids.length + 1];
				System.arraycopy(existingSemanticNids, 0, newSemanticNids, 0, existingSemanticNids.length);
				newSemanticNids[newSemanticNids.length - 1] = nid;
				newSemanticNidsBytes = convert.intArrayToBytes(newSemanticNids);
			} else {
				int[] newSemanticNids = new int[1];
				newSemanticNids[0] = nid;
				newSemanticNidsBytes = convert.intArrayToBytes(newSemanticNids);
			}
			try {
				rocksDB.put(handle(ColumnFamily.PATTERN_NID_TO_SEMANTIC_NIDS_MAP), patternKey, newSemanticNidsBytes);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}

		writeSequence.increment();
		this.indexer.index(sourceObject);
		return mergedBytes;
	}

	private byte[] mergeAndWrite(int nid, byte[] newEntity, ColumnFamily columnFamily) {
		byte[] bytesToWrite;
		byte[] key = convert.integerToBytes(nid);
		try {
			byte[] oldBytes = rocksDB.get(handle(columnFamily), key);
			if (oldBytes != null) {
				bytesToWrite = PrimitiveDataService.merge(oldBytes, newEntity);
			} else {
				bytesToWrite = newEntity;
			}
			rocksDB.put(handle(columnFamily), key, bytesToWrite);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		return bytesToWrite;
	}

	@Override
	public PrimitiveDataSearchResult[] search(String query, int maxResultSize) throws Exception {
		return searcher.search(query, maxResultSize);
	}

	@Override
	public CompletableFuture<Void> recreateLuceneIndex() {
		 return CompletableFuture.supplyAsync(() -> {
            try {
                return TinkExecutor.ioThreadPool().submit(new RecreateIndex(this.indexer)).get();
            } catch (InterruptedException | ExecutionException ex) {
                AlertStreams.dispatchToRoot(new CompletionException("Error encountered while creating Lucene indexes." +
                        "Search and Type Ahead Suggestions may not function as expected.", ex));
            }
            return null;
        });
	}

	@Override
	public void forEachSemanticNidOfPattern(int patternNid, IntProcedure procedure) {
		byte[] key = convert.integerToBytes(patternNid);
		boolean isPattern = rocksDB.keyExists(handle(ColumnFamily.PATTERN_NID_TO_SEMANTIC_NIDS_MAP), key);

		if (!isPattern) {
			throw new IllegalStateException("Trying to iterate elements for entity that is not a pattern: " + patternNid);
		}
		byte[] semanticNidsBytes;
		try {
			semanticNidsBytes = rocksDB.get(handle(ColumnFamily.PATTERN_NID_TO_SEMANTIC_NIDS_MAP), key);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}

		if (semanticNidsBytes != null && semanticNidsBytes.length > 0) {
			int[] semanticNids = convert.bytesToIntArray(semanticNidsBytes);
			for(int i = 0; i < semanticNids.length; i++) {
				procedure.accept(semanticNids[i]);
			}
		}
	}

	@Override
	public void forEachPatternNid(IntProcedure procedure) {
		iterateWithIntProcedure(procedure, ColumnFamily.NID_TO_PATTERN_MAP);
	}

	@Override
	public void forEachConceptNid(IntProcedure procedure) {
		iterateWithIntProcedure(procedure, ColumnFamily.NID_TO_CONCEPT_MAP);
	}

	@Override
	public void forEachStampNid(IntProcedure procedure) {
		iterateWithIntProcedure(procedure, ColumnFamily.NID_TO_STAMP_MAP);
	}

	@Override
	public void forEachSemanticNid(IntProcedure procedure) {
		iterateWithIntProcedure(procedure, ColumnFamily.NID_TO_SEMANTIC_MAP);
	}

	private void iterateWithIntProcedure(IntProcedure procedure, ColumnFamily columnFamily) {
		try (RocksIterator iterator = rocksDB.newIterator(handle(columnFamily))) {
			iterator.seekToFirst();
			while(iterator.isValid()) {
				byte[] key = iterator.key();
				procedure.accept(convert.bytesToInt(key));
				iterator.next();
			}
		}
	}

	@Override
	public void forEachSemanticNidForComponent(int componentNid, IntProcedure procedure) {
		byte[] key = convert.integerToBytes(componentNid);
		byte[] citationArrayBytes;
		try {
			citationArrayBytes = rocksDB.get(handle(ColumnFamily.COMPONENT_NID_TO_CITATION_MAP), key);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if (citationArrayBytes != null) {
			Citation[] citations = convert.bytesToCitationArray(citationArrayBytes);
			for (Citation citation : citations) {
				procedure.accept(citation.semanticNid());
			}
		}
	}

	@Override
	public void forEachSemanticNidForComponentOfPattern(int componentNid, int patternNid, IntProcedure procedure) {
		byte[] key = convert.integerToBytes(componentNid);
		byte[] citationArrayBytes;
		try {
			citationArrayBytes = rocksDB.get(handle(ColumnFamily.COMPONENT_NID_TO_CITATION_MAP), key);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
		if (citationArrayBytes != null) {
			Citation[] citations = convert.bytesToCitationArray(citationArrayBytes);
			for (Citation citation : citations) {
				if (citation.patternNid() == patternNid) {
					procedure.accept(citation.semanticNid());
				}
			}
		}
	}

	@Override
	public void addCanceledStampNid(int stampNid) {
		PrimitiveDataService.super.addCanceledStampNid(stampNid);
	}

	@Override
	public String name() {
		return "RocksDB data";
	}

	@Override
	public boolean isCanceledStampNid(int stampNid) {
		return PrimitiveDataService.super.isCanceledStampNid(stampNid);
	}

}
