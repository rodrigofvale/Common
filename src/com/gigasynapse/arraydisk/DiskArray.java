package com.gigasynapse.arraydisk;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;

/*
 * In a high level, the disk array file has the following core blocks:
 * 
 * Map to Blocks: this map exists to allow the disk size to grow as needed 
 * EmptySpaces: array aiming to try to reuse file spots used by deleted/updated
 *              content
 * Blocks: array with longs pointing to the array content.
 * 
 * Example: For an array with map size = 3, block size = 4, empty size = 2 
 *          and 1 content, we will have:
 * 
 * Map: [Block1Pointer, 0, 0]
 * EmptySpaces: [(0, 0), (0, 0)]
 * Block1: [Content1Pointer, 0, 0, 0]
 * Content1 as [CONTENT_SIZE, CONTENT_IN_BYTES]
 * 
 * New content will be added to the next empty spot in Block1. When Block1 gets
 * full, a new block will be created and Map will point to it as shown below  
 * 
 * Map: [Block1Pointer, Block2Pointer, 0]
 * EmptySpaces: [(0, 0), (0, 0)]
 * Block1: [Pointer2Content1, Pointer2Content2, Pointer2Content3, 
 *          Pointer2Content4]          
 * Content1 as [CONTENT_SIZE, CONTENT_IN_BYTES]
 * Content2 as [CONTENT_SIZE, CONTENT_IN_BYTES]
 * Content3 as [CONTENT_SIZE, CONTENT_IN_BYTES]
 * Content4 as [CONTENT_SIZE, CONTENT_IN_BYTES]
 * Block2: [Pointer2Content5, 0, 0, 0]
 * Content5 as [CONTENT_SIZE, CONTENT_IN_BYTES]
 * 
 * In order to recover item 0 in the array we first identify the block index
 * by calculating 0 / blockSize, and the spot inside the block by calculating
 * 0 mod blockSize.  
 * 
 * Updating a content:
 * To update a content the application will check if the new content fits 
 * in the area used by the old content by checking the CONTENT_SIZE, if
 * so, new content will be stored in the same disk area of old content. If not,
 * new content will be stored at the end of the file
 * 
 * Deleting a content:
 * To delete a content the application will set the content pointer of block[N]
 * to 0
 * 
 * Updating EmptySpaces:
 * In order to reuse empty file areas left by a delete/update process the 
 * application will keep an array pointing to empty slots. For the new content
 * to be added, the application will check for availability of the closed slot 
 * (in size), it there is any empty space available, this file spot will be 
 * used. The EmptySpaces is an array with finite size, when full it will start
 * to replace it content by new empty space that are larger than the ones 
 * available. If the new empty content is small than all spots available in
 * EmptySpaces, it will be dropped and this file will keep this empty spot until
 * a compress() call .  
 *  
 * The DiskArray file structure is the following
 *    
 * +---------------------------------------------------------------------------+
 * | 0000 - 0003 : total items in array (int)
 * | 0004 - 0007 : size of BlockMap array (int)
 * | 0008 - 000B : size of EmptySpace array (int)
 * | 000C - 000F : size of BlockContent array (int) 
 * | 0010 - 0017 : BlockMap array pointer
 * | 0018 - 001F : EmptySpace array pointer
 * | 0020 - 009F : 128 bytes reserved to be used by any application that would 
 * |               like to leverage the ArrayDisk file
 * | 00A0 -      : Data content 
 * +---------------------------------------------------------------------------+ 
 */

public class DiskArray<E> {
	private Class<E> typeArgumentClass;
	protected int totalItems;
	protected int blockMapSize;
	protected int emptyDiskNodesSize;
	protected int blockContentSize;
	protected long blockMapAddress;
	protected long[] blockMap; 
	protected long emptyDiskNodesAddress;
	protected EmptyDiskNode[] emptyDiskNodes;
	protected RandomAccessFile rafBin;
	protected boolean cleared = false;
		
	// The offset allows multiple DiskArray to use the same File.
	// This is useful, for example, to have a HashMap implementation
	// where one DiskArray can be used to store Keys and another DiskArray 
	// can be used to store Values in the same file
	protected int offset;
	
	public DiskArray(Class<E> typeArgumentClass) 
			throws IOException {
		this(typeArgumentClass, 1024 * 1024, 1024, 10240, 0);
	}
	
	public DiskArray(Class<E> typeArgumentClass, File fileBin, int mapSize, 
			int tableBlockSize) throws IOException {
		this(typeArgumentClass, mapSize, tableBlockSize, 10240, 0);
	}

	public DiskArray(Class<E> typeArgumentClass, int blockMapSize, 
			int blockContentSize, int emptyDiskNodesSize, int offset) 
					throws IOException {		
		this.blockMapSize = blockMapSize;
		this.blockMap = new long[blockMapSize];		
		this.blockContentSize = blockContentSize;
		this.emptyDiskNodesSize = emptyDiskNodesSize;
		this.emptyDiskNodes = new EmptyDiskNode[emptyDiskNodesSize];
		this.typeArgumentClass = typeArgumentClass;
		this.offset = offset;
	}
	
	public void open(File file, boolean toBeCreate) throws IOException {
		rafBin = FileFactory.get(file, "rw");
		
		if (toBeCreate) {
			clear(); 
		}
		
		loadTotalItems();
		loadBlockMapSize();
		loadEmptySpaceSize();
		loadBlockContentSize();
		loadBlockMapAddress();
		loadEmptyDiskNodesAddress();			
		loadBlockMap();
		loadEmptyDiskNodes();

		validate(blockMapSize, blockContentSize, offset, emptyDiskNodesSize);	
	}
	
	public void close() throws IOException {
		//rafBin.close();
	}
	
	public synchronized int add(E e) throws IOException {
	    return add(totalItems, e);
	}

	public synchronized int add(int i, E e) throws IOException {
		// TODO: Need to deal with updates with same size content
		byte data[] = toBytes(e);
		
	    if (i >= (long) blockContentSize * blockMapSize) {
	    	throw new IndexOutOfBoundsException(String.format("DiskArray "
	    			+ "reached maximum address capacity for settings:\n"
	    			+ "addressMapSize %d\ntableBlockSize %d", blockMapSize,
	    			blockContentSize));
	    }
	    
	    int blockMapIndex = i / blockContentSize;
	    long blockIndex = i % blockContentSize;
	    long valueAddress;
	    
	    if (blockMap[blockMapIndex] == 0) {
	    	createNewBlock(blockMapIndex);
	    } else {
	    	rafBin.seek(blockMap[blockMapIndex] + blockIndex * Long.BYTES);
	    	valueAddress = rafBin.readLong();
	    	if (valueAddress != 0) {
	    		rafBin.seek(valueAddress);
	    		int size = rafBin.readInt();
	    		if (size == data.length) {
	    			rafBin.write(data);
	    			return i;
	    		} else {
	    			// content size + 4 bytes for the size (int)
	    			addEmptySlot(valueAddress, size + 4);
	    		}
	    	}
	    }
	    
	    int emptyDiskNodeId = findEmptyDiskNode(data.length);
	    if (emptyDiskNodeId == -1) {
	    	valueAddress = rafBin.length();
	    } else {
	    	valueAddress = emptyDiskNodes[emptyDiskNodeId].fileIndex;
	    }
		rafBin.seek(valueAddress);
		rafBin.writeInt(data.length);
		rafBin.write(data);
		
		rafBin.seek(blockMap[blockMapIndex] + blockIndex * Long.BYTES);
		rafBin.writeLong(valueAddress);
		
		if (emptyDiskNodeId != -1) {
			emptyDiskNodes[emptyDiskNodeId].size -= (data.length + 4);
			// free space should consider 4 bytes for size + at least 1 byte
			// for content
			if (emptyDiskNodes[emptyDiskNodeId].size < 5) {
				emptyDiskNodes[emptyDiskNodeId].fileIndex = 0;
			} else {
				emptyDiskNodes[emptyDiskNodeId].fileIndex += data.length; 
			}
			saveEmptyDiskNodes(emptyDiskNodeId);
		}
		
		if (i + 1 > totalItems) {
			setTotalItems(i + 1);
		}
		return i;
	}
	
	private void addEmptySlot(long address, int size) throws IOException {
		int minIndex = -1;
		int minSize = -1;
		for(int i = 0; i < emptyDiskNodes.length; i++) {
			if (emptyDiskNodes[i].size == 0) {				
				emptyDiskNodes[i].fileIndex = address;
				emptyDiskNodes[i].size = size;
				saveEmptyDiskNodes(i);				
				return;
			}
			
			if (minSize == -1) {
				minSize = emptyDiskNodes[i].size;
				minIndex = i;									
			} else if (emptyDiskNodes[i].size < minSize) {
				minSize = emptyDiskNodes[i].size;
				minIndex = i;
			}
		}
		
		emptyDiskNodes[minIndex].fileIndex = address;
		emptyDiskNodes[minIndex].size = size;
		saveEmptyDiskNodes(minIndex);
	}

	protected void clear() throws IOException {
		if (offset == 0) {
			rafBin.setLength(0);
		}
		setTotalItems(0);
		setBlockMapSize(blockMapSize);
		setEmptySpaceSize(emptyDiskNodesSize);
		setBlockContentSize(blockContentSize);
		setBlockMapAddress();
		setEmptyDiskNodesAddress();
		if (offset == 0) {
			clearReservedBytes();
		}
		setBlockMapAddress();
		for(int i = 0; i < blockMap.length; i++)
			blockMap[i] =0;
		setBlockMap(blockMap);
		setEmptyDiskNodesAddress();
		for(int i = 0; i < emptyDiskNodes.length; i++) {
			if (emptyDiskNodes[i] == null) 
					emptyDiskNodes[i] = new EmptyDiskNode(0, 0);
			else {
				emptyDiskNodes[i].fileIndex = 0;
				emptyDiskNodes[i].size = 0;
			}
		}
		setEmptyDiskNodes(emptyDiskNodes);
		cleared = true;
	}
	
	protected void clearReservedBytes() throws IOException {
		// reserved bytes can be used only once in a file, if an offset is not
		// zero, it means that another Object is trying to use the same file
		// so nothing should be saved
		if (offset == 0) {
			rafBin.seek(0x20);
			for(int i = 0; i < 128; i++) 
				rafBin.writeByte(0);
		}
	}
	
	public void compact() {
		throw new java.lang.UnsupportedOperationException("Not supported yet.");
	}
	
	private long createNewBlock(int index) 
			throws IOException {
	    // creates the first block
	    byte data[] = new byte[blockContentSize * Long.BYTES];
	    long blockAddress = rafBin.length();
	    rafBin.seek(blockAddress);
	    rafBin.write(data);
	    
	    blockMap[index] = blockAddress; 
	    rafBin.seek(blockMapAddress + index * Long.BYTES);
	    rafBin.writeLong(blockAddress);	    
	    return blockAddress;
	}

	public synchronized void del(int i) throws IOException {
		
	    if (i >= (long) blockContentSize * blockMapSize) {
	    	throw new IndexOutOfBoundsException(String.format("DiskArray "
	    			+ "reached maximum address capacity for settings:\n"
	    			+ "addressMapSize %d\ntableBlockSize %d", blockMapSize,
	    			blockContentSize));
	    }
	    
	    int blockMapIndex = i / blockContentSize;
	    
	    if (blockMap[blockMapIndex] == 0) {
	    	createNewBlock(blockMapIndex);
	    }
	    	    
	    long blockIndex = i % blockContentSize;	    
		rafBin.seek(blockMap[blockMapIndex] + blockIndex * Long.BYTES);		
		long valueAddress = rafBin.readLong();
		if (valueAddress == 0) {
			return;
		}
		rafBin.seek(valueAddress);
		int size = rafBin.readInt();
		rafBin.seek(blockMap[blockMapIndex] + blockIndex * Long.BYTES);
		rafBin.writeLong(0);
		
		addEmptySlot(valueAddress, size + 4);
		
		if (i + 1 == totalItems) {
			setTotalItems(i);
		}
	}

	private int findEmptyDiskNode(int size) throws IOException {
		int minIndex = -1;
		int minSize = -1;
		size += 4;
		
		for(int i = 0; i < emptyDiskNodes.length; i++) {
			if ((minSize == -1) && (emptyDiskNodes[i].size >= size)) {
				minSize = emptyDiskNodes[i].size;
				minIndex = i;									
			} else if ((minSize != -1) && (emptyDiskNodes[i].size >= size) 
					&& (emptyDiskNodes[i].size < minSize)) {
				minSize = emptyDiskNodes[i].size;
				minIndex = i;
			}
		}
		
		return minIndex;
	}

	public synchronized E get(int i) 
			throws IOException {	    
	    if (i >= totalItems) {
	    	throw new IndexOutOfBoundsException("Index: " + i + ", Size: " + 
	    			totalItems);
	    }
	    
	    int blockMapIndex = i / blockContentSize;
	    long blockIndex = i % blockContentSize;	    
		rafBin.seek(blockMap[blockMapIndex] + blockIndex * Long.BYTES);
		long valueAddress = rafBin.readLong();
		if (valueAddress == 0) {
			return null;
		}
		rafBin.seek(valueAddress);		
		int size = rafBin.readInt();
		byte data[] = new byte[size];
		rafBin.read(data);
		E key = toObject(data);
		return key;
	}
	
	public NodeDisk getFirst() throws IOException {
		E e = get(0);
		return new NodeDisk(0, e);
	}
	
	public NodeDisk getNext(NodeDisk nodeDisk) throws IOException {
		int i = nodeDisk.key + 1;
		
		if (i >= size()) {
			return null;
		}
		
		E e = get(i);
		return new NodeDisk(i, e);
	}

	// return Iterator instance
    public Iterator<E> iterator() {
    	return (Iterator<E>) new DiskArrayIterator<E>(this);
    }
    
    public long length() throws IOException {
    	return rafBin.length();
    }
	
	protected void loadBlockContentSize() throws IOException {
		rafBin.seek(offset + 0x0C);
		blockContentSize = rafBin.readInt();
	}
	
	protected void loadBlockMap() throws IOException {
		rafBin.seek(blockMapAddress);

		FileInputStream fis = new FileInputStream(rafBin.getFD());
	    BufferedInputStream bis = new BufferedInputStream(fis);
	    DataInputStream dis = new DataInputStream(bis);
		
		for(int i = 0; i < blockMap.length; i++) 
			blockMap[i] = dis.readLong();
	}
	
	protected void loadBlockMapAddress() throws IOException {
		rafBin.seek(offset + 0x10);
		blockMapAddress = rafBin.readLong();
	}

	
	protected void loadBlockMapSize() throws IOException {
		rafBin.seek(offset + 0x04);
		blockMapSize = rafBin.readInt(); 
	}
	
	protected void loadEmptyDiskNodes() throws IOException {
		emptyDiskNodes = new EmptyDiskNode[emptyDiskNodesSize]; 
	    rafBin.seek(emptyDiskNodesAddress);
	    for(int i = 0; i < emptyDiskNodesSize; i++) {
	    	byte data[] = new byte[EmptyDiskNode.bytes()];
	    	rafBin.read(data);
	    	emptyDiskNodes[i] = EmptyDiskNode.fromBytes(data);
	    }
	}
		
	protected void loadEmptyDiskNodesAddress() throws IOException {
		rafBin.seek(offset + 0x18);
		emptyDiskNodesAddress = rafBin.readLong();
	}
	
	protected void loadEmptySpaceSize() throws IOException {
		rafBin.seek(offset + 0x08);
		emptyDiskNodesSize = rafBin.readInt();
	}
	
	protected void loadTotalItems() throws IOException {
		rafBin.seek(offset + 0x00);
		totalItems = rafBin.readInt();
	}
	
	public void printStats() throws IOException {
		System.out.println("Total Items: " + size());
	}
	
	private void saveEmptyDiskNodes(int i) throws IOException {
	    rafBin.seek(emptyDiskNodesAddress + i * EmptyDiskNode.bytes());
	    rafBin.write(emptyDiskNodes[i].toBytes());
	}
	
	protected void setBlockContentSize(int blockContentSize) throws IOException {
		rafBin.seek(offset + 0x0C);
		rafBin.writeInt(blockContentSize);
		this.blockContentSize = blockContentSize; 
	}
	
	protected void setBlockMap(long[] blockMap) throws IOException {
		rafBin.seek(blockMapAddress);
		
		FileOutputStream fos = new FileOutputStream(rafBin.getFD());
		BufferedOutputStream bos = new BufferedOutputStream(fos);
	    DataOutputStream dos = new DataOutputStream(bos);
		
		for(int i = 0; i < blockMap.length; i++) 
			dos.writeLong(blockMap[i]);
		dos.flush();
		this.blockMap = blockMap;
	}
	
	protected void setBlockMapAddress() throws IOException {
		blockMapAddress = rafBin.length();
		rafBin.seek(offset + 0x10);
		rafBin.writeLong(blockMapAddress);
		this.blockMapAddress = blockMapAddress; 
	}
	
	protected void setBlockMapSize(int blockMapSize) throws IOException {
		rafBin.seek(offset + 0x04);
		rafBin.writeInt(blockMapSize);
		this.blockMapSize = blockMapSize; 
	}
	
    protected void setEmptyDiskNodes(EmptyDiskNode[] emptyDiskNode) throws IOException {
	    rafBin.seek(emptyDiskNodesAddress);
		FileOutputStream fos = new FileOutputStream(rafBin.getFD());
		BufferedOutputStream bos = new BufferedOutputStream(fos);
	    DataOutputStream dos = new DataOutputStream(bos);
			    
	    for(int i = 0; i < emptyDiskNodes.length; i++) {
	    	byte data[] = emptyDiskNodes[i].toBytes();
	    	dos.write(data);
	    }
	    dos.flush();
	    this.emptyDiskNodes = emptyDiskNodes;
	}
	
	
	protected void setEmptyDiskNodesAddress() throws IOException {
		emptyDiskNodesAddress = rafBin.length();
		rafBin.seek(offset + 0x18);
		rafBin.writeLong(emptyDiskNodesAddress);
		this.emptyDiskNodesAddress = emptyDiskNodesAddress; 
	}
	
	protected void setEmptySpaceSize(int emptySpaceSize) throws IOException {
		rafBin.seek(offset + 0x08);
		rafBin.writeInt(emptySpaceSize);
		this.emptyDiskNodesSize = emptySpaceSize; 
	}
	
	protected void setTotalItems(int totalItems) throws IOException {
		rafBin.seek(offset + 0x00);
		rafBin.writeInt(totalItems);
		this.totalItems = totalItems; 
	}

	public int size() throws IOException {
	    return totalItems;
	}
	
	protected byte[] toBytes(Object element) {
		byte data[] = null;
		ByteBuffer byteBuffer = null;
		switch (element.getClass().getSimpleName()) {
		case "String":
			return ((String) element).getBytes();
		case "Date":
			return ByteBuffer.allocate(Long.BYTES).putLong(((Date) element).getTime()).array();
		case "Integer":
			return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) element).array();
		case "Float":
			return ByteBuffer.allocate(Float.BYTES).putFloat((Float) element).array();
		case "Short":
			return ByteBuffer.allocate(Short.BYTES).putShort((Short) element).array();
		case "Long":
			return ByteBuffer.allocate(Long.BYTES).putLong((Long) element).array();
		case "Double":
			return ByteBuffer.allocate(Double.BYTES).putDouble((Double) element).array();
		case "Character":
			return ByteBuffer.allocate(Character.BYTES).putChar((Character) element).array();
		case "int[]":
			int values[] = (int[]) element;			
			byteBuffer = ByteBuffer.allocate(Integer.BYTES * values.length);
			for(int i = 0; i < values.length; i++) {
				byteBuffer.putInt(values[i]);
			}
			return byteBuffer.array();
		case "float[]":
			float floatValues[] = (float[]) element;			
			byteBuffer = ByteBuffer.allocate(Float.BYTES * floatValues.length);
			for(int i = 0; i < floatValues.length; i++) {
				byteBuffer.putFloat(floatValues[i]);
			}
			return byteBuffer.array();
		case "double[]":
			double doubleValues[] = (double[]) element;			
			byteBuffer = ByteBuffer.allocate(Double.BYTES * doubleValues.length);
			for(int i = 0; i < doubleValues.length; i++) {
				byteBuffer.putDouble(doubleValues[i]);
			}
			return byteBuffer.array();
		case "short[]":
			short shortValues[] = (short[]) element;			
			byteBuffer = ByteBuffer.allocate(Short.BYTES * shortValues.length);
			for(int i = 0; i < shortValues.length; i++) {
				byteBuffer.putShort(shortValues[i]);
			}
			return byteBuffer.array();
		case "long[]":
			long longValues[] = (long[]) element;			
			byteBuffer = ByteBuffer.allocate(Long.BYTES * longValues.length);
			for(int i = 0; i < longValues.length; i++) {
				byteBuffer.putLong(longValues[i]);
			}
			return byteBuffer.array();
		case "byte[]":
			return (byte[]) element;			
		default:
			return ((GenericDiskElement) element).toBytes();
		}		
	}

	@SuppressWarnings("unchecked")
	protected E toObject(byte data[]) {		
		try {
			String name = typeArgumentClass.getSimpleName();
			ByteBuffer byteBuffer = null;
			int items = 0;
			switch (typeArgumentClass.getSimpleName()) {
			case "String":
				return (E) new String(data);
			case "Date":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Long time = byteBuffer.getLong();					
				return (E) new Date(time);
			case "Integer":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Integer value = byteBuffer.getInt();
				return (E) value;
			case "Float":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Float f = byteBuffer.getFloat();
				return (E) f;
			case "Short":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Short s = byteBuffer.getShort();
				return (E) s;
			case "Long":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Long l = byteBuffer.getLong();
				return (E) l;
			case "Double":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Double d = byteBuffer.getDouble();
				return (E) d;
			case "Character":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Character c = byteBuffer.getChar();
				return (E) c;
			case "int[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Integer.BYTES);
				int intArray[] = new int[items];
				for(int i = 0; i < items; i++) {
					intArray[i] = byteBuffer.getInt();  
				}
				return (E) intArray;
			case "float[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Float.BYTES);
				float floatArray[] = new float[items];
				for(int i = 0; i < items; i++) {
					floatArray[i] = byteBuffer.getFloat();  
				}
				return (E) floatArray;
			case "double[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Double.BYTES);
				double doubleArray[] = new double[items];
				for(int i = 0; i < items; i++) {
					doubleArray[i] = byteBuffer.getDouble();  
				}
				return (E) doubleArray;
			case "short[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Short.BYTES);
				short shortArray[] = new short[items];
				for(int i = 0; i < items; i++) {
					shortArray[i] = byteBuffer.getShort();  
				}
				return (E) shortArray;
			case "long[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Long.BYTES);
				long longArray[] = new long[items];
				for(int i = 0; i < items; i++) {
					longArray[i] = byteBuffer.getLong();  
				}
				return (E) longArray;
			case "byte[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length);
				byte byteArray[] = new byte[items];
				byteBuffer.get(byteArray);
				return (E) byteArray;
			default:
				E item = typeArgumentClass.getDeclaredConstructor().newInstance();
				((GenericDiskElement) item).fromBytes(data);
				return item;
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	protected void validate(int blockMapSize, int blockContentSize, int offset, 
			int emptySpaceSize) throws IOException {
		loadBlockMapSize();
		if (this.blockMapSize != blockMapSize) {
	    	throw new IOException(String.format("Invalid blockMapSize. "
	    			+ "Value on disk is %d, value specified by the app is %d. "
	    			+ "Please use %d.", this.blockMapSize, blockMapSize,
	    			this.blockMapSize));							
		}
		loadEmptySpaceSize();
		if (this.emptyDiskNodesSize != emptySpaceSize) {
	    	throw new IOException(String.format("Invalid emptySpaceSize. "
	    			+ "Value on disk is %d, value specified by the app is %d. "
	    			+ "Please use %d.", this.emptyDiskNodesSize, emptySpaceSize,
	    			this.emptyDiskNodesSize));							
		}
		loadBlockContentSize();
		if (this.blockContentSize != blockContentSize) {
	    	throw new IOException(String.format("Invalid blockContentSize. "
	    			+ "Value on disk is %d, value specified by the app is %d. "
	    			+ "Please use %d.", this.blockContentSize, blockContentSize,
	    			this.blockContentSize));							
		}		
	}
	
	public static void main(String[] args) throws Exception {
		unitTest();
	}
	public static void unitTest() throws IOException {
		File file = new File("/tmp/diskarray2.bin");
		file.delete();
		DiskArray hashSetDisk = new DiskArray(String.class, 2, 3, 10, 0);
		DiskArray hashSetDisk2 = new DiskArray(String.class, 10, 10, 10, 0x20);
		
		boolean toCreate = !file.exists();
		hashSetDisk.open(file, toCreate);
		hashSetDisk2.open(file, toCreate);
		
		
		System.out.println(hashSetDisk.add("0"));
		System.out.println(hashSetDisk.add("1"));
		System.out.println(hashSetDisk.add("2"));
		System.out.println(hashSetDisk.add("3"));
		System.out.println(hashSetDisk.add("4"));
		System.out.println(hashSetDisk.add("5"));
		System.out.println(hashSetDisk.add("6"));
		System.out.println(hashSetDisk.add("7"));
		
		
		System.out.println(hashSetDisk2.add("vania"));		
		System.out.println(hashSetDisk2.add("milena"));		
		System.out.println(hashSetDisk2.add("joão"));
		System.out.println(hashSetDisk2.add("pedro"));
		
		System.out.println(hashSetDisk.get(0));
		System.out.println(hashSetDisk2.get(0));
		
		hashSetDisk2.del(1);
		hashSetDisk.del(0);
		
		hashSetDisk2.add(2, "malu");

		System.out.println(hashSetDisk.size());
		System.out.println(hashSetDisk2.size());
		
		System.out.println("-------");
		
		Iterator<String> i = hashSetDisk.iterator();
		while(i.hasNext()) {
			System.out.println(i.next());
		}		
		
		System.out.println("-------");
		
		i = hashSetDisk2.iterator();
		while(i.hasNext()) {
			System.out.println(i.next());
		}		
	}}
