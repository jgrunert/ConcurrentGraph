package mthesis.concurrent_graph.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MiscUtil {

	public static <T> int getIntersectCount(Set<T> a, Set<T> b) {
		if (a.isEmpty() || b.isEmpty()) return 0;

		// Iterate over smaller set to be faster
		Set<T> smaller;
		Set<T> bigger;
		if (a.size() > b.size()) {
			bigger = a;
			smaller = b;
		}
		else {
			bigger = b;
			smaller = a;
		}

		int intersects = 0;
		for (T v : smaller) {
			if (bigger.contains(v))
				intersects++;
		}
		return intersects;
	}


	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}


	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueInverse(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}


	public static int defaultInt(Integer value) {
		return (value != null) ? (int) value : 0;
	}

	public static long defaultLong(Long value) {
		return (value != null) ? (long) value : 0;
	}
}
