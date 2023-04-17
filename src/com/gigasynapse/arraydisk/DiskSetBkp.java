package com.gigasynapse.arraydisk;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.gigasynapse.common.Hash;

/*
 * The set uses the array file as a base and it stores the set index in blocks 
 * controlled by a set map, as follows:
 *  
 * blockSetCounter:  [0, 0, ...]
 * blockSetHashId:   [0, 0, ...]
 * blockSetPointers: [blockSet1Pointer, blockSet2Pointer, ...]
 * blockSet1: [(collision, pointer), (collision, pointer), ...]
 *   
 *    
 * +---------+--------+-------------------------------------------------------+
 * | Address | offset | description                                           |
 * +---------+--------+-------------------------------------------------------+
 * |    0x20 |      8 | blockSetPointersAddress: a pointer to the block in    |
 * |         |        | disk with the address of each blockSetPointers        |
 * +---------+--------+-------------------------------------------------------+
 * |    0x28 |      8 | blockSetHashIdAddress: a pointer to the block in      |
 * |         |        | disk with the id of each Hash assigned to a block     |
 * +---------+--------+-------------------------------------------------------+
 * |    0x30 |      8 | blockSetCounterAddress: a pointer to the block in     |
 * |         |        | disk with the items counter for each blockSet         |
 * +---------+--------+-------------------------------------------------------+
 * |    0x38 |      4 | bockSetEmptyIndex: an index to the first empty        |
 * |         |        | blockSetPointers                                      |
 * +---------+--------+-------------------------------------------------------+
 */

public class DiskSetBkp<E> extends DiskArray<E> {
	protected long blockSetPointersAddress;
	protected long blockSetPointers[];
	
	protected long blockSetHashIdAddress;	
	protected byte blockSetHashId[];
	
	protected long blockSetCounterAddress;
	protected int blockSetCounter[];
	
	protected int bockSetEmptyIndex;	

	public DiskSetBkp(Class<E> typeArgumentClass) 
			throws IOException {
		this(typeArgumentClass, 1024 * 1024, 1024, 10240, 0);
	}
	
	public DiskSetBkp(Class<E> typeArgumentClass, int blockMapSize, 
			int blockContentSize) throws IOException {
		this(typeArgumentClass, blockMapSize, blockContentSize, 10240, 
				0);
	}

	public DiskSetBkp(Class<E> typeArgumentClass, int blockMapSize, 
			int blockContentSize, int emptyDiskNodesSize, int offset) 
					throws IOException {
		super(typeArgumentClass, blockMapSize, blockContentSize, 
				emptyDiskNodesSize, offset);

	}
	
	/*************************************************************************
	 * The recover method will bring the state of the file to a valid state.
	 * In case of a failure during the write process to a disk, a 
	 * Write Ahead Logging file will be checked and the last write process
	 * will be repeated. This does not avoid data loss
	 * 
	 * @throws IOException
	 */
	protected void recoverDiskSet() {
		try {
			WALContent walContent = walFile.get();
			if (walContent == null) {
				return;
			}
			
			if (walContent.id == -1) {
				return;
			}
			ByteBuffer buffer = ByteBuffer.wrap(walContent.data);
			
			long fileAddr = 0;
			int index = 0;
			switch (walContent.id) {
			case 6:
			    fileAddr = buffer.getLong();
			    byte collision = buffer.get();
			    int pointer = buffer.getInt();
			    BlockSetItem blockSetItem = 
			    		new BlockSetItem(collision, pointer);
			    
			    rafBin.seek(fileAddr);
				rafBin.writeByte(blockSetItem.collision);
				rafBin.writeInt(blockSetItem.pointer);
				break;

			case 7:
				// recovering from a saving failure
				index = buffer.getInt();
				long address = buffer.getLong();
				blockSetPointers[index] = address;
				
				rafBin.seek(blockSetPointersAddress + index * Long.BYTES);
				rafBin.writeLong(blockSetPointers[index]);
				break;
				
			case 8:
				index = buffer.getInt();
				byte hashId = buffer.get();
				blockSetHashId[index] = hashId;
				
				rafBin.seek(blockSetHashIdAddress + index);
				rafBin.writeByte(blockSetHashId[index]);
				break;

			case 9:
			    bockSetEmptyIndex = buffer.getInt();
				rafBin.seek(0x38);
				rafBin.writeInt(bockSetEmptyIndex);
				break;

			case 10:
			    blockSetPointersAddress = buffer.getLong();
			    
				rafBin.seek(0x20);
				rafBin.writeLong(blockSetPointersAddress);
				break;

			case 11:
			    blockSetHashIdAddress = buffer.getLong();
				rafBin.seek(0x28);
				rafBin.writeLong(blockSetHashIdAddress);
				break;
				
			case 12:
				blockSetCounterAddress = buffer.getLong();
				rafBin.seek(0x30);
				rafBin.writeLong(blockSetCounterAddress);
				break;

			case 13:
				index = buffer.getInt();
				blockSetCounter[index] = buffer.getInt();
				rafBin.seek(blockSetCounterAddress + index * Integer.BYTES);
				rafBin.writeInt(blockSetCounter[index]);
				break;
			}
		} catch (IOException e) {
			LOGGER.warning("Invalid WAL file");
		}
	}
	
	
	public void open(File file) throws IOException {
		super.open(file);
		
		loadBlockSetPointersAddress();
		loadBlockSetPointers();
			
		loadBlockSetCounterAddress();
		loadBlockSetCounter();
		
		loadBlockSetHashIdAddress();
		loadBlockSetHashId();
		recoverDiskSet();
	}
	

	protected void clear() throws IOException {
		super.clear();
		
		createBlockSetPointersAddress();
		createBlockSetHashIdAddress();
		createBlockSetCounterAddress();
		createBockSet();
	}
	
	protected void createBlockSetPointersAddress() throws IOException {
		blockSetPointersAddress = rafBin.length();
		saveBlockSetPointersAddress();
		createBlockSetPointers();
	}
	
	protected void saveBlockSetPointersAddress() throws IOException {
	    walFile.set(10);
	    walFile.add(blockSetPointersAddress);
	    walFile.flush();	    		
		
		rafBin.seek(0x20);
		rafBin.writeLong(blockSetPointersAddress);
		walFile.clean();
	}
	
	protected void loadBlockSetPointersAddress() throws IOException {
		rafBin.seek(0x20);
		blockSetPointersAddress = rafBin.readLong();
	}
	
	protected void createBlockSetPointers() throws IOException {
		rafBin.seek(blockSetPointersAddress);
		
		FileOutputStream fos = new FileOutputStream(rafBin.getFD());
		BufferedOutputStream bos = new BufferedOutputStream(fos);
	    DataOutputStream dos = new DataOutputStream(bos);		
		
		blockSetPointers = new long[blockMapSize];
		for(int i = 0; i < blockSetPointers.length; i++) {
			blockSetPointers[i] = 0;
			dos.writeLong(blockSetPointers[i]);
		}
		dos.flush();
	}

	protected void loadBlockSetPointers() throws IOException {
		rafBin.seek(blockSetPointersAddress);
		FileInputStream fis = new FileInputStream(rafBin.getFD());
	    BufferedInputStream bis = new BufferedInputStream(fis);
	    DataInputStream dis = new DataInputStream(bis);		
		
		blockSetPointers = new long[blockMapSize];
		for(int i = 0; i < blockSetPointers.length; i++) {
			blockSetPointers[i] = dis.readLong();
		}
	}
	
	protected void setBlockSetPointers(int index, long address) 
			throws IOException  {
	    walFile.set(7);
	    walFile.add(index);
	    walFile.add(address);
	    walFile.flush();	    		
		
		blockSetPointers[index] = address;
		rafBin.seek(blockSetPointersAddress + index * Long.BYTES);
		rafBin.writeLong(blockSetPointers[index]);
		
		walFile.clean();
	}
	
	protected void createBlockSetHashIdAddress() throws IOException {
		blockSetHashIdAddress = rafBin.length();
		saveBlockSetHashIdAddress();
		createBlockSetHashId();
	}
	
	protected void saveBlockSetHashIdAddress() throws IOException {
	    walFile.set(11);
	    walFile.add(blockSetHashIdAddress);
	    walFile.flush();	    		
		
		rafBin.seek(0x28);
		rafBin.writeLong(blockSetHashIdAddress);
		walFile.clean();
	}
	
	protected void loadBlockSetHashIdAddress() throws IOException {
		rafBin.seek(0x28);
		blockSetHashIdAddress = rafBin.readLong();
	}
	
	protected void createBlockSetHashId() throws IOException {
		rafBin.seek(blockSetHashIdAddress);
		
		FileOutputStream fos = new FileOutputStream(rafBin.getFD());
		BufferedOutputStream bos = new BufferedOutputStream(fos);
	    DataOutputStream dos = new DataOutputStream(bos);		
		
		blockSetHashId = new byte[blockMapSize];
		for(int i = 0; i < blockSetHashId.length; i++) {
			blockSetHashId[i] = 0;
			dos.writeByte(blockSetHashId[i]);
		}
		dos.flush();
	}
	
	protected void loadBlockSetHashId() throws IOException {
		rafBin.seek(blockSetHashIdAddress);
		blockSetHashId = new byte[blockMapSize];
		for(int i = 0; i < blockSetHashId.length; i++) {
			blockSetHashId[i] = rafBin.readByte();
		}
	}
	
	protected void setBlockSetHashId(int index, byte hashId) 
			throws IOException  {
		blockSetHashId[index] = hashId;
		
	    walFile.set(8);
	    walFile.add(index);
	    walFile.add(hashId);
	    walFile.flush();	    		
		
		rafBin.seek(blockSetHashIdAddress + index);
		rafBin.writeByte(blockSetHashId[index]);
		walFile.clean();
	}
		
	protected byte getBlockSetHashId(int index) { 
		return blockSetHashId[index];
	}
	
	protected void createBlockSetCounterAddress() throws IOException {
		blockSetCounterAddress = rafBin.length();
		saveBlockSetCounterAddress();
		createBlockSetCounter();
	}
	
	protected void createBlockSetCounter() throws IOException {
		rafBin.seek(blockSetCounterAddress);
		
		FileOutputStream fos = new FileOutputStream(rafBin.getFD());
		BufferedOutputStream bos = new BufferedOutputStream(fos);
	    DataOutputStream dos = new DataOutputStream(bos);		
		
		blockSetCounter = new int[blockMapSize];
		for(int i = 0; i < blockSetCounter.length; i++) {
			blockSetCounter[i] = 0;
			dos.writeInt(blockSetCounter[i]);
		}
		dos.flush();
	}
	
	protected void loadBlockSetCounter() throws IOException {
		rafBin.seek(blockSetCounterAddress);
		FileInputStream fis = new FileInputStream(rafBin.getFD());
	    BufferedInputStream bis = new BufferedInputStream(fis);
	    DataInputStream dis = new DataInputStream(bis);		
		
		blockSetCounter = new int[blockMapSize];
		for(int i = 0; i < blockSetCounter.length; i++) {
			blockSetCounter[i] = dis.readInt();
		}
	}
	
	protected void saveBlockSetCounterAddress() throws IOException {
	    walFile.set(12);
	    walFile.add(blockSetCounterAddress);
	    walFile.flush();	    		
		
		rafBin.seek(0x30);
		rafBin.writeLong(blockSetCounterAddress);
		walFile.clean();
	}
	
	protected void loadBlockSetCounterAddress() throws IOException {
		rafBin.seek(0x30);
		blockSetCounterAddress = rafBin.readLong();
	}
	
	protected void saveBlockSetCounter(int index) 
			throws IOException  {
	    walFile.set(13);
	    walFile.add(index);
	    walFile.add(blockSetCounter[index]);
	    walFile.flush();	    		
		
		rafBin.seek(blockSetCounterAddress + index * Integer.BYTES);
		rafBin.writeInt(blockSetCounter[index]);
		walFile.clean();
	}
	
	protected void saveBockSetEmptyIndex() throws IOException {
	    walFile.set(9);
	    walFile.add(bockSetEmptyIndex);
	    walFile.flush();	    		

		rafBin.seek(0x38);
		rafBin.writeInt(bockSetEmptyIndex);
		walFile.clean();
	}
	
	protected void loadBockSetEmptyIndex() throws IOException {
		rafBin.seek(0x38);
		bockSetEmptyIndex = rafBin.readInt();
	}
	
	protected int createBockSet() throws IOException {
		int bockSetId = bockSetEmptyIndex;
		int size = blockContentSize;
		if (bockSetId != 0) {
			size = 1031;
		}
		
		long blockSetAddress = rafBin.length();
		rafBin.seek(blockSetAddress);
		
		FileOutputStream fos = new FileOutputStream(rafBin.getFD());
		BufferedOutputStream bos = new BufferedOutputStream(fos);
	    DataOutputStream dos = new DataOutputStream(bos);		
				
		for(int i = 0; i < size; i++) {
			// set collision to zero
			dos.writeByte(0);
			// set item address to -1
			dos.writeInt(-1);
		}
		dos.flush();
		
		if (blockSetCounter.length <= bockSetEmptyIndex) {
			throw new RuntimeException("Max number of blocks reached!");
		}
		
		blockSetCounter[bockSetEmptyIndex] = 0;
		saveBlockSetCounter(bockSetEmptyIndex);
		setBlockSetHashId(bockSetEmptyIndex, (byte) 0);
		setBlockSetPointers(bockSetEmptyIndex, blockSetAddress);
		
		bockSetEmptyIndex++;
		saveBockSetEmptyIndex();
		return bockSetId;
	}
	
	protected BlockSetItem getBlockSetItem(int blockId, long index) 
			throws IOException {
		long blockSetAddress = blockSetPointers[blockId];
		long fileAddr = blockSetAddress + index * BlockSetItem.bytes(); 
		rafBin.seek(fileAddr);
		byte collision = rafBin.readByte();
		int pointer = rafBin.readInt();
		BlockSetItem blockSetItem = new BlockSetItem(collision, pointer);
		return blockSetItem;
	}
	
	protected void setBlockSetItem(int blockId, long index, 
			BlockSetItem blockSetItem) throws IOException {
		long blockSetAddress = blockSetPointers[blockId];
		long fileAddr = blockSetAddress + index * BlockSetItem.bytes(); 
	    if (rafBin.length() <= fileAddr) {
	    	rafBin.setLength(fileAddr);
	    }
	    
	    walFile.set(6);
	    walFile.add(fileAddr);
	    walFile.add(blockSetItem.collision);
	    walFile.add(blockSetItem.pointer);
	    walFile.flush();	    
	    
		rafBin.seek(fileAddr);
		rafBin.writeByte(blockSetItem.collision);
		rafBin.writeInt(blockSetItem.pointer);
		walFile.clean();
	}
	
	public Integer getId(E e) throws IOException {
	    byte data[] = toBytes(e);
	    return get(0, data);
	}
	
	private synchronized int get(int blockId, byte data[]) throws IOException {
		int size = blockContentSize;
		if (blockId != 0) {
			size = 1031;
		}
		
	    byte hashFunctionId = getBlockSetHashId(blockId);
	    long hash = Hash.compute(data, hashFunctionId) % size;
	    
	    BlockSetItem blockSetItem = getBlockSetItem(blockId, hash);
	    
	    if (blockSetItem.collision == 0) {
	    	if (blockSetItem.pointer == -1) {
	    		return -1;
	    	}
	    	// que value from DiskArray	    	
	    	E value = get(blockSetItem.pointer);
	    	if (value == null) {
	    		return -1;
	    	}
	    	byte data2[] = toBytes(value);
	    	if (Arrays.equals(data, data2)) {
	    		return blockSetItem.pointer;
	    	}
	    	return -1;
	    }
	    return get(blockSetItem.pointer, data);
	}
	
	public synchronized int add(E e) throws IOException {
		Integer id = getId(e);
		if (id != -1) {
			return id;
		}
		
		id = super.add(e);
		
		byte data[] = toBytes(e);		
		add(0 , id, data);
		
		return id;
	}	
	
	private synchronized void add(int blockId, int id, byte data[]) 
			throws IOException {
		int size = blockContentSize;
		
		// just first block should be large, following blocks can be small
		// this saves a lot of disk space 
		if (blockId != 0) {
			size = 1031;
		}
		
	    byte hashFunctionId = getBlockSetHashId(blockId);
	    long hash = Hash.compute(data, hashFunctionId) % size;
	    
	    BlockSetItem blockSetItem = getBlockSetItem(blockId, hash);
	    
	    if (blockSetItem.collision == 0) {
	    	// if new item, save item pointer to ArrayDisk 
	    	if (blockSetItem.pointer == -1) {	
	    		blockSetItem.pointer = id;
	    		setBlockSetItem(blockId, hash, blockSetItem);
		    	blockSetCounter[blockId]++;
		    	saveBlockSetCounter(blockId); 
	    	} else {
	    		// there is already a item using this HashSet spot
	    		// we need to recover it
	    		E value = get(blockSetItem.pointer);
	    		byte data2[] = toBytes(value);

	    	    byte hashId = Hash.getHashId(data, data2, size);
	    	    if (hashId == -1) {
	    	    	throw new RuntimeException(String
	    					.format("Failed to find a hash id: %s, %s\n", 
	    							toObject(data), toObject(data2))); 
	    	    }
	    		// we create a new blockSet
	    	    int newBlockId = createBockSet();	
	    	    setBlockSetHashId(newBlockId, hashId);
	    		add(newBlockId, blockSetItem.pointer, data2);
	    		add(newBlockId, id, data);
	    		
	    	    BlockSetItem newBlockSetItem = new BlockSetItem((byte) 1, 
	    	    		newBlockId);
	    	    setBlockSetItem(blockId, hash, newBlockSetItem);	    	    
	    	}
	    } else {
	    	add(blockSetItem.pointer, id, data);	    	
	    }
	}

	public static void unitTest() throws IOException {
		File file = new File("/tmp/diskset.bin");
		file.delete();
		DiskSetBkp diskSet = new DiskSetBkp(String.class, 4, 10);
		
		boolean toCreate = !file.exists();
		diskSet.open(file);
			
		diskSet.add("rodrigo");
		diskSet.add("rodrigo");
		diskSet.add("rodrigo");
		diskSet.add("vania");		
		diskSet.add("rafael");
		diskSet.add("gabriel");
		diskSet.add("milena");
		diskSet.add("gustavo");
		diskSet.add("arminio");
		diskSet.add("helena");
		diskSet.add("jabuti");
		diskSet.add("pera");
		
		System.out.println(diskSet.getId("rodrigo"));
		System.out.println(diskSet.getId("vania"));
		System.out.println(diskSet.getId("rafael"));
		System.out.println(diskSet.getId("gabriel"));
		System.out.println(diskSet.getId("milena"));
		System.out.println(diskSet.getId("gustavo"));
		System.out.println(diskSet.getId("arminio"));
		System.out.println(diskSet.getId("helena"));
		System.out.println(diskSet.getId("pera"));
		System.out.println(diskSet.getId("jabuti"));
		System.out.println(diskSet.getId("anajulia"));
		diskSet.close();
	}
	
	public static void main(String[] args) throws Exception {
		unitTest();
	}	
}
