package dev.ikm.tinkar.provider.rocksdb;

import dev.ikm.tinkar.common.service.DataUriOption;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;

import java.io.IOException;

public class RocksDBOpenController extends RocksDBController {

	public static final String CONTROLLER_NAME = "Open RocksDB Store";

	@Override
	public void setDataUriOption(DataUriOption option) {
		ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, option.toFile());
	}

	@Override
	public String controllerName() {
		return CONTROLLER_NAME;
	}

	@Override
	public void start() {
		if (RocksDBProvider.INSTANCE == null) {
			try {
				new RocksDBProvider();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
