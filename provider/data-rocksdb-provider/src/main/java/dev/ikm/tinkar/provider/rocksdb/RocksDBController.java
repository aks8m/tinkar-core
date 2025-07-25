package dev.ikm.tinkar.provider.rocksdb;

import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.DataUriOption;
import dev.ikm.tinkar.common.service.PrimitiveDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBController implements DataServiceController<PrimitiveDataService> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBController.class.getSimpleName());

	@Override
	public boolean isValidDataLocation(String name) {
		return false;
	}

	@Override
	public void setDataUriOption(DataUriOption option) {

	}

	@Override
	public String controllerName() {
		return "Open RocksDB";
	}

	@Override
	public Class<? extends PrimitiveDataService> serviceClass() {
		return null;
	}

	@Override
	public boolean running() {
		return false;
	}

	@Override
	public void start() {

	}

	@Override
	public void stop() {

	}

	@Override
	public void save() {

	}

	@Override
	public void reload() {

	}

	@Override
	public PrimitiveDataService provider() {
		return null;
	}
}
