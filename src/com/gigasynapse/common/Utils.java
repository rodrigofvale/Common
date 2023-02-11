package com.gigasynapse.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Utils {	
	public static String toString(Date date) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(date);
	}

	public static Date toDate(String date) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		try {
			return dateFormat.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static <T> ArrayList<ArrayList<T>> split(ArrayList<T> original, int size) {
		// Create a list of sets to return.
		ArrayList<ArrayList<T>> result = new ArrayList<ArrayList<T>>();
	
		// Create an iterator for the original set.
		Iterator<T> it = original.iterator();
	
		// Create each new set.
		ArrayList<T> s = new ArrayList<T>();
		while (it.hasNext()) {
			T item = it.next();
			s.add(item);
			if (s.size() == size) {
				result.add(s);
				s = new ArrayList<T>();
			}
		}
		if (s.size() > 0) {
			result.add(s);
		}
		return result;
	}

	public static <T> List<Set<T>> split(Set<T> original, int size) {
		// Create a list of sets to return.
		ArrayList<Set<T>> result = new ArrayList<Set<T>>();
	
		// Create an iterator for the original set.
		Iterator<T> it = original.iterator();
	
		// Create each new set.
		HashSet<T> s = new HashSet<T>();
		while (it.hasNext()) {
			T item = it.next();
			s.add(item);
			if (s.size() == size) {
				result.add(s);
				s = new HashSet<T>();
			}
		}
		if (s.size() > 0) {
			result.add(s);
		}
		return result;
	}

	public static <K, V> List<HashMap<K, V>> splitMap(final HashMap<K, V> map, final int size) {
	    List<K> keys = new ArrayList<>(map.keySet());
	    List<HashMap<K, V>> parts = new ArrayList<>();
	    HashMap<K, V> part = new HashMap<K, V>();
	    parts.add(part);
	    Iterator<K> i = map.keySet().iterator();
	    int  count = 0;
	    int total = 0;
	    while (i.hasNext()) {
	    	K k = i.next();
	    	V v = map.get(k);
	    	part.put(k, v);
	    	count++;
	    	total++;
	    	if (count >= size) {
	    		count = 0;
	    		if (total < map.size()) {
	    			part = new HashMap<K, V>();
	    			parts.add(part);
	    		}
	    	}
	    }
	    return parts;
	}

	/**
	 * Get a diff between two dates
	 * @param date1 the oldest date
	 * @param date2 the newest date
	 * @param timeUnit the unit in which you want the diff
	 * @return the diff value, in the provided unit
	 */
	public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
		long diffInMillies = date2.getTime() - date1.getTime();
		return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
	}
}
