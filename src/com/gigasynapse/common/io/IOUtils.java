package com.gigasynapse.common.io;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Date;

public class IOUtils {
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
		case "byte[]":
			return (byte[]) element;			
		default:
			return ((DiskElement) element).toBytes();
		}		
	}

	@SuppressWarnings("unchecked")
	public static Object toObject(Class typeArgumentClass, byte data[]) {		
		try {
			String name = typeArgumentClass.getSimpleName();
			ByteBuffer byteBuffer = null;
			int items = 0;
			switch (typeArgumentClass.getSimpleName()) {
			case "String":
				return new String(data);
			case "Date":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Long time = byteBuffer.getLong();					
				return new Date(time);
			case "Integer":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Integer value = byteBuffer.getInt();
				return value;
			case "Float":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Float f = byteBuffer.getFloat();
				return f;
			case "Short":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Short s = byteBuffer.getShort();
				return s;
			case "Long":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Long l = byteBuffer.getLong();
				return l;
			case "Double":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Double d = byteBuffer.getDouble();
				return d;
			case "Character":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				Character c = byteBuffer.getChar();
				return c;
			case "int[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Integer.BYTES);
				int intArray[] = new int[items];
				for(int i = 0; i < items; i++) {
					intArray[i] = byteBuffer.getInt();  
				}
				return intArray;
			case "float[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Float.BYTES);
				float floatArray[] = new float[items];
				for(int i = 0; i < items; i++) {
					floatArray[i] = byteBuffer.getFloat();  
				}
				return floatArray;
			case "double[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Double.BYTES);
				double doubleArray[] = new double[items];
				for(int i = 0; i < items; i++) {
					doubleArray[i] = byteBuffer.getDouble();  
				}
				return doubleArray;
			case "short[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Short.BYTES);
				short shortArray[] = new short[items];
				for(int i = 0; i < items; i++) {
					shortArray[i] = byteBuffer.getShort();  
				}
				return shortArray;
			case "long[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length / Long.BYTES);
				long longArray[] = new long[items];
				for(int i = 0; i < items; i++) {
					longArray[i] = byteBuffer.getLong();  
				}
				return longArray;
			case "byte[]":
				byteBuffer = ByteBuffer.allocate(data.length).put(data);
				byteBuffer.rewind();
				items = (int) (data.length);
				byte byteArray[] = new byte[items];
				byteBuffer.get(byteArray);
				return byteArray;
			default:
				Constructor constructor = typeArgumentClass
					.getDeclaredConstructor();
				DiskElement item = (DiskElement) constructor.newInstance();
				item.fromBytes(data);
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
}
