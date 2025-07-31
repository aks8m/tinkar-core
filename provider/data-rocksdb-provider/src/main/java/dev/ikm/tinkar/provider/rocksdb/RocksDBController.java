package dev.ikm.tinkar.provider.rocksdb;

import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.PrimitiveDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RocksDBController implements DataServiceController<PrimitiveDataService> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBController.class.getSimpleName());

	@Override
	public boolean isValidDataLocation(String name) {
		return false;
	}

	@Override
	public Class<? extends PrimitiveDataService> serviceClass() {
		return PrimitiveDataService.class;
	}

	@Override
	public boolean running() {
		if (RocksDBProvider.INSTANCE != null) {
			return true;
		}
		return false;
	}

	@Override
	public void stop() {
		RocksDBProvider.INSTANCE.close();
		RocksDBProvider.INSTANCE = null;
	}

	@Override
	public void save() {
		RocksDBProvider.INSTANCE.save();
	}

	@Override
	public void reload() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public PrimitiveDataService provider() {
		if (RocksDBProvider.INSTANCE == null) {
			start();
		}
		return RocksDBProvider.INSTANCE;
	}

	@Override
	public String toString() {
		return controllerName();
	}
}
