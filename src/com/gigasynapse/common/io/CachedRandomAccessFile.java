package com.gigasynapse.common.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class CachedRandomAccessFile {
	private static HashMap<String, CachedRandomAccessFile> fabric = 
			new HashMap<String, CachedRandomAccessFile>(); 
	private HashMap<Integer, CacheItem[]> cache;
	private int maxItems;
	private RandomAccessFile raf;
	private long filePointer;
	private long fileLength;
	private int cacheBlockSize;
	private int cacheSize;
	private File file;

	class CacheItem {
		boolean modified;
		byte value;
	}
	
	public static CachedRandomAccessFile get(File file, String mode, 
			int cacheSize) throws FileNotFoundException {
		String fileName = file.getAbsolutePath();
		if (fabric.containsKey(fileName)) {
			return fabric.get(fileName);
		}
		
		CachedRandomAccessFile cachedRandomAccessFile = 
				new CachedRandomAccessFile(file, mode, cacheSize);
		
		fabric.put(fileName, cachedRandomAccessFile);
		return cachedRandomAccessFile;		
	}

	public static void closeAll() {
		Collection<CachedRandomAccessFile> collection = fabric.values();
		collection.forEach(item -> {
			try {
				item.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		fabric.clear();
	}
	
	public static void flushAll() {
		Collection<CachedRandomAccessFile> collection = fabric.values();
		collection.forEach(item -> {
			try {
				item.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}
	
	public File getFile() {
		return file;
	}
	
	
	public CachedRandomAccessFile(File file, String mode, int cacheBlockSize, 
			int cacheSize) throws FileNotFoundException {
		this.cacheBlockSize = cacheBlockSize;
		this.cacheSize = cacheSize;
		this.file = file;
		maxItems = cacheSize / cacheBlockSize;
		cache = new HashMap<Integer, CacheItem[]>();
		raf = new RandomAccessFile(file, mode);
		try {
			fileLength = raf.length();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		filePointer = 0;		
	}
	
	public CachedRandomAccessFile(File file, String mode, int cacheSize) 
			throws FileNotFoundException {
		this(file, mode, 5120, cacheSize);
	}

	private void flush(long writePos, ByteBuffer buffer) throws IOException {
		buffer.capacity();
		buffer.position();
		byte bytes[] = new byte[buffer.position()];
		buffer.rewind();
		buffer.get(bytes);
		buffer.clear();
		if (raf.length() < writePos + bytes.length) {
			raf.setLength(writePos + bytes.length);
		}
		raf.seek(writePos);
		raf.write(bytes);		
	}
	
	private void flush(Iterator<Integer> i, int cacheBlock) throws IOException {
		CacheItem cacheItems[] = cache.get(cacheBlock);
		long writePos = -1;
		ByteBuffer buffer = ByteBuffer.allocate(cacheBlockSize);
		
		for(int ii = 0; ii < cacheItems.length; ii++) {
			CacheItem cacheItem = cacheItems[ii];
			if ((cacheItem != null) && (cacheItem.modified)) {
				if (writePos == -1) {				
					writePos = cacheBlock * cacheBlockSize + ii;
				}
				buffer.put(cacheItem.value);
			} else {
				if (writePos != -1) {
					flush(writePos, buffer);
					writePos = -1;
				}
			}
		}
		if (writePos != -1) {
			flush(writePos, buffer);
		}
		
		i.remove();
	}
	
	private void flushOne() throws IOException {
		Set<Integer> cacheBlocks = cache.keySet();
		Iterator<Integer> i = cache.keySet().iterator();
		// remove a non modified cache
		while (i.hasNext()) {
			int cacheBlock = i.next();
			flush(i, cacheBlock);
			return;
		}
	}

	public synchronized void flush() throws IOException {
		Set<Integer> cacheBlocks = cache.keySet();
		Iterator<Integer> i = cache.keySet().iterator();
		// remove a non modified cache
		while (i.hasNext()) {
			int cacheBlock = i.next();
			flush(i, cacheBlock);
		}
	}

	
    /**
     * Writes the specified byte to this file. The write starts at
     * the current file pointer.
     *
     * @param      b   the <code>byte</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */	
	public synchronized void write(byte b) throws IOException {
		if (cacheSize == 0) {
			raf.write(b);
			return;
		}
		
		int cacheBlock = (int) (filePointer / cacheBlockSize);
		int index = (int) (filePointer % cacheBlockSize);

		if (cache.containsKey(cacheBlock)) {
			CacheItem cacheItems[] = cache.get(cacheBlock);
			if (cacheItems[index] == null) {
				cacheItems[index] = new CacheItem(); 
			}
			cacheItems[index].value = b;
			cacheItems[index].modified = true;
		} else {
			if (cache.size() >= maxItems) {
				flushOne();
			}
			CacheItem cacheItems[] = new CacheItem[cacheBlockSize];
			cacheItems[index] = new CacheItem(); 
			cacheItems[index].value = b;
			cacheItems[index].modified = true;
			cache.put(cacheBlock, cacheItems);
		}
		filePointer++;
		if (fileLength < filePointer) {
			fileLength = filePointer;
		}
	}
	
    /**
     * Writes <code>b.length</code> bytes from the specified byte array
     * to this file, starting at the current file pointer.
     *
     * @param      b   the data.
     * @exception  IOException  if an I/O error occurs.
     */
	public synchronized void write(byte[] b) throws IOException {
		if (cacheSize == 0) {
			raf.write(b);
			return;
		}

		for(int i = 0; i < b.length; i++) {
			write(b[i]);
		}		
	}

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this file.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs.
     */
	public void	write(byte[] b, int off, int len) throws IOException {
		writeBytes(b, off, len);
	}
	
    /**
     * Writes the specified byte to this file. The write starts at
     * the current file pointer.
     *
     * @param      b   the <code>byte</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */	
	public void	write(int b) throws IOException {
		write((byte) b);
	}
	
    /**
     * Writes a <code>short</code> to the file as two bytes, high byte first.
     * The write starts at the current position of the file pointer.
     *
     * @param      v   a <code>short</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeShort(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
        //written += 2;
    }

    /**
     * Writes a <code>char</code> to the file as a two-byte value, high
     * byte first. The write starts at the current position of the
     * file pointer.
     *
     * @param      v   a <code>char</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeChar(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
        //written += 2;
    }

    /**
     * Writes an <code>int</code> to the file as four bytes, high byte first.
     * The write starts at the current position of the file pointer.
     *
     * @param      v   an <code>int</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeInt(int v) throws IOException {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>>  8) & 0xFF);
        write((v >>>  0) & 0xFF);
        //written += 4;
    }

    /**
     * Writes a <code>long</code> to the file as eight bytes, high byte first.
     * The write starts at the current position of the file pointer.
     *
     * @param      v   a <code>long</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeLong(long v) throws IOException {
        write((int)(v >>> 56) & 0xFF);
        write((int)(v >>> 48) & 0xFF);
        write((int)(v >>> 40) & 0xFF);
        write((int)(v >>> 32) & 0xFF);
        write((int)(v >>> 24) & 0xFF);
        write((int)(v >>> 16) & 0xFF);
        write((int)(v >>>  8) & 0xFF);
        write((int)(v >>>  0) & 0xFF);
        //written += 8;
    }

    /**
     * Converts the float argument to an <code>int</code> using the
     * <code>floatToIntBits</code> method in class <code>Float</code>,
     * and then writes that <code>int</code> value to the file as a
     * four-byte quantity, high byte first. The write starts at the
     * current position of the file pointer.
     *
     * @param      v   a <code>float</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.lang.Float#floatToIntBits(float)
     */
    public final void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }	

	public void	writeBoolean(boolean v) throws IOException {
		write(v ? (byte) 1: (byte) 0);
	}

	public void	writeByte(int v) throws IOException {
		write((byte) v);
	}
	
    /**
     * Writes the string to the file as a sequence of bytes. Each
     * character in the string is written out, in sequence, by discarding
     * its high eight bits. The write starts at the current position of
     * the file pointer.
     *
     * @param      s   a string of bytes to be written.
     * @exception  IOException  if an I/O error occurs.
     */
	public void	writeBytes(String s) throws IOException {
		write(s.getBytes());
	}
	
	private void writeBytes(byte b[], int off, int len) throws IOException {
		byte data[] = new byte[len];
		System.arraycopy(b, off, data, 0, len);
		write(data);
	}

    /**
     * Writes a string to the file as a sequence of characters. Each
     * character is written to the data output stream as if by the
     * <code>writeChar</code> method. The write starts at the current
     * position of the file pointer.
     *
     * @param      s   a <code>String</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.RandomAccessFile#writeChar(int)
     */
    public final void writeChars(String s) throws IOException {
        int clen = s.length();
        int blen = 2*clen;
        byte[] b = new byte[blen];
        char[] c = new char[clen];
        s.getChars(0, clen, c, 0);
        for (int i = 0, j = 0; i < clen; i++) {
            b[j++] = (byte)(c[i] >>> 8);
            b[j++] = (byte)(c[i] >>> 0);
        }
        writeBytes(b, 0, blen);
    }

    /**
     * Converts the double argument to a <code>long</code> using the
     * <code>doubleToLongBits</code> method in class <code>Double</code>,
     * and then writes that <code>long</code> value to the file as an
     * eight-byte quantity, high byte first. The write starts at the current
     * position of the file pointer.
     *
     * @param      v   a <code>double</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.lang.Double#doubleToLongBits(double)
     */
    public final void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    /**
     * Writes a string to the file using
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a>
     * encoding in a machine-independent manner.
     * <p>
     * First, two bytes are written to the file, starting at the
     * current file pointer, as if by the
     * <code>writeShort</code> method giving the number of bytes to
     * follow. This value is the number of bytes actually written out,
     * not the length of the string. Following the length, each character
     * of the string is output, in sequence, using the modified UTF-8 encoding
     * for each character.
     *
     * @param      str   a string to be written.
     * @exception  IOException  if an I/O error occurs.
     */
	public void	writeUTF(String str) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos); 
		dos.writeUTF(str);
		write(baos.toByteArray());
		dos.close();
	}
	
	/**
     * Reads in a string from this file. The string has been encoded
     * using a
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a>
     * format.
     * <p>
     * The first two bytes are read, starting from the current file
     * pointer, as if by
     * <code>readUnsignedShort</code>. This value gives the number of
     * following bytes that are in the encoded string, not
     * the length of the resulting string. The following bytes are then
     * interpreted as bytes encoding characters in the modified UTF-8 format
     * and are converted into characters.
     * <p>
     * This method blocks until all the bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     a Unicode string.
     * @exception  EOFException            if this file reaches the end before
     *               reading all the bytes.
     * @exception  IOException             if an I/O error occurs.
     * @exception  UTFDataFormatException  if the bytes do not represent
     *               valid modified UTF-8 encoding of a Unicode string.
     * @see        java.io.RandomAccessFile#readUnsignedShort()
     */
    public final String readUTF() throws IOException {
    	throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }	

	public void	close() throws IOException {
		raf.close();
		String fileName = file.getAbsolutePath();
		fabric.remove(fileName);
	}
	
	public void	seek(long pos) throws IOException {
		if (cacheSize == 0) {
			raf.seek(pos);
		}

		filePointer = pos;
	}
	
    /**
     * Returns the unique {@link java.nio.channels.FileChannel FileChannel}
     * object associated with this file.
     *
     * <p> The {@link java.nio.channels.FileChannel#position()
     * </code>position<code>} of the returned channel will always be equal to
     * this object's file-pointer offset as returned by the {@link
     * #getFilePointer getFilePointer} method.  Changing this object's
     * file-pointer offset, whether explicitly or by reading or writing bytes,
     * will change the position of the channel, and vice versa.  Changing the
     * file's length via this object will change the length seen via the file
     * channel, and vice versa.
     *
     * @return  the file channel associated with this file
     *
     * @since 1.4
     * @spec JSR-51
     */
    public final FileChannel getChannel() {
    	return raf.getChannel();
    }	

    /**
     * Returns the opaque file descriptor object associated with this
     * stream. </p>
     *
     * @return     the file descriptor object associated with this stream.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.FileDescriptor
     */
    public final FileDescriptor getFD() throws IOException {
    	if (raf.getFD() != null) {
            return raf.getFD();
        }
        throw new IOException();
    }
    
    /**
     * Reads up to <code>b.length</code> bytes of data from this file
     * into an array of bytes. This method blocks until at least one byte
     * of input is available.
     * <p>
     * Although <code>RandomAccessFile</code> is not a subclass of
     * <code>InputStream</code>, this method behaves in exactly the
     * same way as the {@link InputStream#read(byte[])} method of
     * <code>InputStream</code>.
     *
     * @param      b   the buffer into which the data is read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             this file has been reached.
     * @exception  IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or if
     * some other I/O error occurs.
     * @exception  NullPointerException If <code>b</code> is <code>null</code>.
     */
	public int read(byte[] b) throws IOException {
		if (cacheSize == 0) {
			return raf.read(b);
		}

		int cacheBlock = (int) (filePointer / cacheBlockSize);
		int index = (int) (filePointer % cacheBlockSize);
		int delta = cacheBlockSize - index;
		
		// this blocks will divide the size on b in cacheBlockSize and read
		// each block		
		if (b.length > delta) {
			int totalRead = 0; 
			for(int i = 0; i < b.length;) {
				byte[] tmp = new byte[delta];			
				int bytesRead = read(tmp); 
				if (bytesRead == -1) {
					return bytesRead; 
				}
				
				System.arraycopy(tmp, 0, b, i, bytesRead);
				i += delta;
				totalRead += bytesRead;
				if (bytesRead < delta) {
					return totalRead;
				}				
				int remainingBytes = b.length - i; 
				
				// for the second and forward interactions, delta should be 
				// the size of cacheBlockSize
				delta = remainingBytes > cacheBlockSize ? 
						cacheBlockSize : remainingBytes;
			}
			return totalRead;
		}
		
		// if in this point of code, it is guaranteed that size of b is <= 
		// cacheBlockSize
		int totalBytesRead = 0;
		if (cache.containsKey(cacheBlock)) {
			CacheItem cacheItems[] = cache.get(cacheBlock);
			for(int i = 0; i < b.length; ){
				int firstItem = i + index;			
				if (cacheItems[firstItem] == null) {
					int lastItem = firstItem; 
					while ((lastItem  < cacheBlockSize) && 
							(cacheItems[lastItem] == null) &&
							(lastItem - index < b.length)) {
						lastItem++;
					}
					int size = lastItem - firstItem;
					byte tmp[] = new byte[size];
										 
					raf.seek(filePointer);
					
					int bytesRead = raf.read(tmp);
					if (bytesRead == -1) {
						return totalBytesRead > 0 ? totalBytesRead : bytesRead; 
					}
					
					for(int ii = 0; ii < bytesRead; ii++) {
						cacheItems[firstItem + ii] = new CacheItem();
						cacheItems[firstItem + ii].value = tmp[ii];
						cacheItems[firstItem + ii].modified = false;
					}
					 
					System.arraycopy(tmp, 0, b, i, bytesRead);
					i += bytesRead;

					filePointer += bytesRead;
					totalBytesRead += bytesRead;
				} else {
					b[i] = cacheItems[firstItem].value;
					totalBytesRead++;
					filePointer++;
					i++;
				}
			}
			return totalBytesRead;
		}
		
		// no cache, we need to read it from file
		if (cache.size() >= maxItems) {
			flushOne();
		}
		raf.seek(filePointer);
		int bytesRead = raf.read(b);
		if (bytesRead == -1) {
			return bytesRead; 
		}
		filePointer += bytesRead; 
		CacheItem cacheItems[] = new CacheItem[cacheBlockSize];
		for (int i = 0; i < bytesRead; i++) {
			cacheItems[index + i] = new CacheItem();
			cacheItems[index + i].value = b[i];
			cacheItems[index + i].modified = false;
		}
		cache.put(cacheBlock, cacheItems);
		
		return bytesRead;
	}
	
    /**
     * Reads a signed eight-bit value from this file. This method reads a
     * byte from the file, starting from the current file pointer.
     * If the byte read is <code>b</code>, where
     * <code>0&nbsp;&lt;=&nbsp;b&nbsp;&lt;=&nbsp;255</code>,
     * then the result is:
     * <blockquote><pre>
     *     (byte)(b)
     * </pre></blockquote>
     * <p>
     * This method blocks until the byte is read, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return     the next byte of this file as a signed eight-bit
     *             <code>byte</code>.
     * @exception  EOFException  if this file has reached the end.
     * @exception  IOException   if an I/O error occurs.
     */
	public byte	readByte() throws IOException {
		if (cacheSize == 0) {
			return raf.readByte();
		}
		
		int cacheBlock = (int) (filePointer / cacheBlockSize);
		int index = (int) (filePointer % cacheBlockSize);

		if (cache.containsKey(cacheBlock)) {
			CacheItem cacheItems[] = cache.get(cacheBlock);
			if (cacheItems[index] == null) {
				raf.seek(filePointer);
				byte value = raf.readByte();
				cacheItems[index] = new CacheItem();								
				cacheItems[index].value = value;
				cacheItems[index].modified = false;
			}
			filePointer++;
			return cacheItems[index].value;
		}
		
		if (cache.size() >= maxItems) {
				flushOne();
		}
		CacheItem cacheItems[] = new CacheItem[cacheBlockSize];
		cacheItems[index] = new CacheItem();
		raf.seek(filePointer);
		byte value = raf.readByte();
		cacheItems[index].value = value;
		cacheItems[index].modified = false;
		filePointer++;
		return cacheItems[index].value;
	}
	
	public int skipBytes(int n) {
		filePointer += n;
		return n;
	}
	
    /**
     * Reads a character from this file. This method reads two
     * bytes from the file, starting at the current file pointer.
     * If the bytes read, in order, are
     * <code>b1</code> and <code>b2</code>, where
     * <code>0&nbsp;&lt;=&nbsp;b1,&nbsp;b2&nbsp;&lt;=&nbsp;255</code>,
     * then the result is equal to:
     * <blockquote><pre>
     *     (char)((b1 &lt;&lt; 8) | b2)
     * </pre></blockquote>
     * <p>
     * This method blocks until the two bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next two bytes of this file, interpreted as a
     *                  <code>char</code>.
     * @exception  EOFException  if this file reaches the end before reading
     *               two bytes.
     * @exception  IOException   if an I/O error occurs.
     */
    public final char readChar() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char)((ch1 << 8) + (ch2 << 0));
    }
	
    /**
     * Reads a byte of data from this file. The byte is returned as an
     * integer in the range 0 to 255 (<code>0x00-0x0ff</code>). This
     * method blocks if no input is yet available.
     * <p>
     * Although <code>RandomAccessFile</code> is not a subclass of
     * <code>InputStream</code>, this method behaves in exactly the same
     * way as the {@link InputStream#read()} method of
     * <code>InputStream</code>.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             file has been reached.
     * @exception  IOException  if an I/O error occurs. Not thrown if
     *                          end-of-file has been reached.
     */	
	public int read() throws IOException {
		return Byte.toUnsignedInt(readByte());
	}
	
    /**
     * Reads up to <code>len</code> bytes of data from this file into an
     * array of bytes. This method blocks until at least one byte of input
     * is available.
     * <p>
     * Although <code>RandomAccessFile</code> is not a subclass of
     * <code>InputStream</code>, this method behaves in exactly the
     * same way as the {@link InputStream#read(byte[], int, int)} method of
     * <code>InputStream</code>.
     *
     * @param      b     the buffer into which the data is read.
     * @param      off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param      len   the maximum number of bytes read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception  IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or if
     * some other I/O error occurs.
     * @exception  NullPointerException If <code>b</code> is <code>null</code>.
     * @exception  IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>b.length - off</code>
     */
	public int read(byte[] b, int off, int len) throws IOException {
		return readBytes(b, off, len);
	}
	
    /**
     * Reads a sub array as a sequence of bytes.
     * @param b the buffer into which the data is read.
     * @param off the start offset of the data.
     * @param len the number of bytes to read.
     * @exception IOException If an I/O error has occurred.
     */	
	private int readBytes(byte b[], int off, int len) throws IOException {
		byte data[] = new byte[len];
		System.arraycopy(b, off, data, 0, len);
		return read(data);		
	}	
	
	public void	readFully(byte[] b) throws IOException {
		read(b);
	}

	public void	readFully(byte[] b, int off, int len) throws IOException {
		readBytes(b, off, len);
	}
	
	
    /**
     * Reads a <code>boolean</code> from this file. This method reads a
     * single byte from the file, starting at the current file pointer.
     * A value of <code>0</code> represents
     * <code>false</code>. Any other value represents <code>true</code>.
     * This method blocks until the byte is read, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return     the <code>boolean</code> value read.
     * @exception  EOFException  if this file has reached the end.
     * @exception  IOException   if an I/O error occurs.
     */
    public final boolean readBoolean() throws IOException {
        int ch = this.read();
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }
	
    /**
     * Reads a <code>float</code> from this file. This method reads an
     * <code>int</code> value, starting at the current file pointer,
     * as if by the <code>readInt</code> method
     * and then converts that <code>int</code> to a <code>float</code>
     * using the <code>intBitsToFloat</code> method in class
     * <code>Float</code>.
     * <p>
     * This method blocks until the four bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next four bytes of this file, interpreted as a
     *             <code>float</code>.
     * @exception  EOFException  if this file reaches the end before reading
     *             four bytes.
     * @exception  IOException   if an I/O error occurs.
     * @see        java.io.RandomAccessFile#readInt()
     * @see        java.lang.Float#intBitsToFloat(int)
     */
    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }
	
    /**
     * Reads a signed 32-bit integer from this file. This method reads 4
     * bytes from the file, starting at the current file pointer.
     * If the bytes read, in order, are <code>b1</code>,
     * <code>b2</code>, <code>b3</code>, and <code>b4</code>, where
     * <code>0&nbsp;&lt;=&nbsp;b1, b2, b3, b4&nbsp;&lt;=&nbsp;255</code>,
     * then the result is equal to:
     * <blockquote><pre>
     *     (b1 &lt;&lt; 24) | (b2 &lt;&lt; 16) + (b3 &lt;&lt; 8) + b4
     * </pre></blockquote>
     * <p>
     * This method blocks until the four bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next four bytes of this file, interpreted as an
     *             <code>int</code>.
     * @exception  EOFException  if this file reaches the end before reading
     *               four bytes.
     * @exception  IOException   if an I/O error occurs.
     */
    public final int readInt() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        int ch3 = this.read();
        int ch4 = this.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }
	
    /**
     * Reads a signed 64-bit integer from this file. This method reads eight
     * bytes from the file, starting at the current file pointer.
     * If the bytes read, in order, are
     * <code>b1</code>, <code>b2</code>, <code>b3</code>,
     * <code>b4</code>, <code>b5</code>, <code>b6</code>,
     * <code>b7</code>, and <code>b8,</code> where:
     * <blockquote><pre>
     *     0 &lt;= b1, b2, b3, b4, b5, b6, b7, b8 &lt;=255,
     * </pre></blockquote>
     * <p>
     * then the result is equal to:
     * <p><blockquote><pre>
     *     ((long)b1 &lt;&lt; 56) + ((long)b2 &lt;&lt; 48)
     *     + ((long)b3 &lt;&lt; 40) + ((long)b4 &lt;&lt; 32)
     *     + ((long)b5 &lt;&lt; 24) + ((long)b6 &lt;&lt; 16)
     *     + ((long)b7 &lt;&lt; 8) + b8
     * </pre></blockquote>
     * <p>
     * This method blocks until the eight bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next eight bytes of this file, interpreted as a
     *             <code>long</code>.
     * @exception  EOFException  if this file reaches the end before reading
     *               eight bytes.
     * @exception  IOException   if an I/O error occurs.
     */
    public final long readLong() throws IOException {
        return ((long)(readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }
	
    /**
     * Reads a signed 16-bit number from this file. The method reads two
     * bytes from this file, starting at the current file pointer.
     * If the two bytes read, in order, are
     * <code>b1</code> and <code>b2</code>, where each of the two values is
     * between <code>0</code> and <code>255</code>, inclusive, then the
     * result is equal to:
     * <blockquote><pre>
     *     (short)((b1 &lt;&lt; 8) | b2)
     * </pre></blockquote>
     * <p>
     * This method blocks until the two bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next two bytes of this file, interpreted as a signed
     *             16-bit number.
     * @exception  EOFException  if this file reaches the end before reading
     *               two bytes.
     * @exception  IOException   if an I/O error occurs.
     */
    public final short readShort() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }
	
	public long	length() throws IOException {		
		long l = raf.length();
		return fileLength > l ? fileLength : l;
	}

    /**
     * Reads a <code>double</code> from this file. This method reads a
     * <code>long</code> value, starting at the current file pointer,
     * as if by the <code>readLong</code> method
     * and then converts that <code>long</code> to a <code>double</code>
     * using the <code>longBitsToDouble</code> method in
     * class <code>Double</code>.
     * <p>
     * This method blocks until the eight bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next eight bytes of this file, interpreted as a
     *             <code>double</code>.
     * @exception  EOFException  if this file reaches the end before reading
     *             eight bytes.
     * @exception  IOException   if an I/O error occurs.
     * @see        java.io.RandomAccessFile#readLong()
     * @see        java.lang.Double#longBitsToDouble(long)
     */
    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

	public void	setLength(long newLength) throws IOException {
		raf.setLength(newLength);
	}
	
	public long	getFilePointer() {
		return filePointer;
	}
	
    /**
     * Reads the next line of text from this file.  This method successively
     * reads bytes from the file, starting at the current file pointer,
     * until it reaches a line terminator or the end
     * of the file.  Each byte is converted into a character by taking the
     * byte's value for the lower eight bits of the character and setting the
     * high eight bits of the character to zero.  This method does not,
     * therefore, support the full Unicode character set.
     *
     * <p> A line of text is terminated by a carriage-return character
     * (<code>'&#92;r'</code>), a newline character (<code>'&#92;n'</code>), a
     * carriage-return character immediately followed by a newline character,
     * or the end of the file.  Line-terminating characters are discarded and
     * are not included as part of the string returned.
     *
     * <p> This method blocks until a newline character is read, a carriage
     * return and the byte following it are read (to see if it is a newline),
     * the end of the file is reached, or an exception is thrown.
     *
     * @return     the next line of text from this file, or null if end
     *             of file is encountered before even one byte is read.
     * @exception  IOException  if an I/O error occurs.
     */	
    public final String readLine() throws IOException {
        StringBuffer input = new StringBuffer();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = read()) {
            case -1:
            case '\n':
                eol = true;
                break;
            case '\r':
                eol = true;
                long cur = getFilePointer();
                if ((read()) != '\n') {
                    seek(cur);
                }
                break;
            default:
                input.append((char)c);
                break;
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        return input.toString();
    }	
    
    /**
     * Reads an unsigned eight-bit number from this file. This method reads
     * a byte from this file, starting at the current file pointer,
     * and returns that byte.
     * <p>
     * This method blocks until the byte is read, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return     the next byte of this file, interpreted as an unsigned
     *             eight-bit number.
     * @exception  EOFException  if this file has reached the end.
     * @exception  IOException   if an I/O error occurs.
     */    
    public final int readUnsignedByte() throws IOException {
        int ch = this.read();
        if (ch < 0)
            throw new EOFException();
        return ch;
    }

    /**
     * Reads an unsigned 16-bit number from this file. This method reads
     * two bytes from the file, starting at the current file pointer.
     * If the bytes read, in order, are
     * <code>b1</code> and <code>b2</code>, where
     * <code>0&nbsp;&lt;=&nbsp;b1, b2&nbsp;&lt;=&nbsp;255</code>,
     * then the result is equal to:
     * <blockquote><pre>
     *     (b1 &lt;&lt; 8) | b2
     * </pre></blockquote>
     * <p>
     * This method blocks until the two bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return     the next two bytes of this file, interpreted as an unsigned
     *             16-bit integer.
     * @exception  EOFException  if this file reaches the end before reading
     *               two bytes.
     * @exception  IOException   if an I/O error occurs.
     */    
    public final int readUnsignedShort() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }    
    
	public static void main(String[] args) throws Exception {
		CachedRandomAccessFile craf1 = CachedRandomAccessFile.get(
				new File("/tmp/raf2.bin"), "rw", 1024);
		
		CachedRandomAccessFile craf2 = CachedRandomAccessFile.get(
				new File("/tmp/raf2.bin"), "rw", 1024);
		
		craf1.write(10);
		craf2.seek(0);
		System.out.println(craf2.read());
		craf1.flush();
		craf2.seek(0);
		System.out.println(craf2.read());		
	}
	
}
