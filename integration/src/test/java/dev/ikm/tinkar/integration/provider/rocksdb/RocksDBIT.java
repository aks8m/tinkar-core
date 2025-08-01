package dev.ikm.tinkar.integration.provider.rocksdb;

import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.io.FileUtil;
import dev.ikm.tinkar.entity.ConceptEntity;
import dev.ikm.tinkar.entity.ConceptEntityVersion;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityCountSummary;
import dev.ikm.tinkar.entity.EntityFactory;
import dev.ikm.tinkar.entity.EntityVersion;
import dev.ikm.tinkar.entity.PatternEntity;
import dev.ikm.tinkar.entity.PatternEntityVersion;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.StampEntityVersion;
import dev.ikm.tinkar.integration.TestConstants;
import dev.ikm.tinkar.integration.helper.DataStore;
import dev.ikm.tinkar.integration.helper.TestHelper;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class RocksDBIT {

	private static Logger LOG = LoggerFactory.getLogger(RocksDBIT.class.getSimpleName());


	private static final File DATASTORE_ROOT = TestConstants.createFilePathInTargetFromClassName.apply(RocksDBIT.class);
	private static EntityCountSummary entityCountSummary;

	@BeforeAll
	public static void setup() {
		LOG.info("JVM Version: " + System.getProperty("java.version"));
		LOG.info("JVM Name: " + System.getProperty("java.vm.name"));
		LOG.info(ServiceProperties.jvmUuid());
		FileUtil.recursiveDelete(DATASTORE_ROOT);
		TestHelper.startDataBase(DataStore.ROCKSDB_STORE, DATASTORE_ROOT);
		entityCountSummary = TestHelper.loadDataFile(TestConstants.PB_STARTER_DATA_REASONED);
	}

	@Test
	public void nidForPublicIdTest() {
		assertEquals(TinkarTerm.ROLE.nid(), PrimitiveData.get().nidForPublicId(TinkarTerm.ROLE));
	}

	@Test
	public void hasUUIDTest() {
		assertTrue(PrimitiveData.get().hasUuid(TinkarTerm.ROLE.uuids()[0]));
	}

	@Test
	public void hasPublicIdTest() {
		assertTrue(PrimitiveData.get().hasPublicId(TinkarTerm.ROLE.publicId()));
	}

	@Test
	public void forEachTest() {
		AtomicLong conceptCount = new AtomicLong();
		AtomicLong semanticCount = new AtomicLong();
		AtomicLong patternCount = new AtomicLong();
		AtomicLong stampCount = new AtomicLong();
//
		PrimitiveData.get().forEach((bytes, value) -> {
			Entity<? extends EntityVersion> entity = EntityFactory.make(bytes);
			switch (entity) {
				case ConceptEntity<? extends ConceptEntityVersion> conceptEntity -> {
					assertEquals(conceptEntity, Entity.getFast(value));
					conceptCount.getAndIncrement();
				}
				case SemanticEntity<? extends SemanticEntityVersion> semanticEntity -> {
					assertEquals(semanticEntity, Entity.getFast(value));
					semanticCount.getAndIncrement();
				}
				case PatternEntity<? extends PatternEntityVersion> patternEntity -> {
					assertEquals(patternEntity, Entity.getFast(value));
					patternCount.getAndIncrement();
				}
				case StampEntity<? extends StampEntityVersion> stampEntity -> {
					assertEquals(stampEntity, Entity.getFast(value));
					stampCount.getAndIncrement();
				}
				default -> fail("Unexpected entity: " + entity);
			}
		});
//		assertEquals(RocksDBIT.conceptCount, conceptCount.get());
//		assertEquals(RocksDBIT.semanticCount, semanticCount.get());
//		assertEquals(RocksDBIT.patternCount, patternCount.get());
//		assertEquals(RocksDBIT.stampCount, stampCount.get());
	}

	@Test
	public void forEachParallelTest() {

	}

	@Test
	public void getBytesTest() {

	}

	@Test
	public void searchTest() {
		LOG.info(entityCountSummary.toString());
	}

	@Test
	public void forEachSemanticNidOfPatternTest() {

	}

	@Test
	public void forEachPatternNidTest() {

	}

	@Test
	public void forEachConceptNidTest() {

	}

	@Test
	public void forEachStampNidTest() {

	}

	@Test
	public void forEachSemanticNidTest() {

	}

	@Test
	public void forEachSemanticNidForComponentTest() {

	}

	@Test
	public void forEachSemanticNidForComponentOfPatternTest() {

	}

}
