module dev.ikm.tinkar.provider.rocksdb {
	requires rocksdbjni;
	requires dev.ikm.tinkar.provider.search;
	requires dev.ikm.jpms.activej.common;
	requires org.slf4j;
	requires dev.ikm.jpms.eclipse.collections;
	requires dev.ikm.jpms.eclipse.collections.api;

	uses dev.ikm.tinkar.common.service.LoadDataFromFileController;

	provides dev.ikm.tinkar.common.service.DataServiceController with
			dev.ikm.tinkar.provider.rocksdb.RocksDBOpenController,
			dev.ikm.tinkar.provider.rocksdb.RocksDBNewController;
}