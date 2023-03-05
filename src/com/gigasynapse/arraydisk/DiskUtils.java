package com.gigasynapse.arraydisk;

import java.nio.ByteBuffer;
import java.util.Date;

public class DiskUtils {
	public static byte[] toBytes(Object element) {
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
		default:
			return ((GenericDiskElement) element).toBytes();
		}		
	}
}
