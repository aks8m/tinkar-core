package dev.ikm.tinkar.provider.rocksdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.UUID;

public class Convert {

	public byte[] uuidToBytes(UUID uuid) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}

	public byte[] integerToBytes(int integer) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[4]);
		bb.putInt(integer);
		return bb.array();
	}

	public int bytesToInt(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		return bb.getInt();
	}

	public byte[] longArrayToBytes(long[] longs) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(longs.length * Long.BYTES);
		byteBuffer.order(ByteOrder.BIG_ENDIAN);
		LongBuffer longBuffer = byteBuffer.asLongBuffer();
		longBuffer.put(longs);
		return byteBuffer.array();
	}

	public long[] bytesToLongArray(byte[] bytes) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.order(ByteOrder.BIG_ENDIAN);
		LongBuffer longBuffer = byteBuffer.asLongBuffer();
		long[] longArray = new long[longBuffer.remaining()];
		longBuffer.get(longArray);
		return longArray;
	}

	public byte[] citationArrayToBytes(Citation[] citation) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
			 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(citation);
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Citation[] bytesToCitationArray(byte[] bytes) {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			 ObjectInputStream ois = new ObjectInputStream(bis)) {
			return (Citation[]) ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] intArrayToBytes(int[] intArray) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
			 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(intArray);
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int[] bytesToIntArray(byte[] bytes) {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			 ObjectInputStream ois = new ObjectInputStream(bis)) {
			return (int[]) ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
