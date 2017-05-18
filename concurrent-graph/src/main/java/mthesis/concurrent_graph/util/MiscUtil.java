package mthesis.concurrent_graph.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;

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

	public static int getIntersectCount(IntSet a, IntSet b) {
		if (a.isEmpty() || b.isEmpty()) return 0;

		// Iterate over smaller set to be faster
		IntSet smaller;
		IntSet bigger;
		if (a.size() > b.size()) {
			bigger = a;
			smaller = b;
		}
		else {
			bigger = b;
			smaller = a;
		}

		int intersects = 0;
		for (IntIterator it = smaller.iterator(); it.hasNext();) {
			if (bigger.contains(it.nextInt())) intersects++;
		}
		return intersects;
	}

	// More than 2x slower
	//	public static int getIntersectCount2(IntSet a, IntSet b) {
	//		if (a.isEmpty() || b.isEmpty()) return 0;
	//
	//		// Iterate over smaller set to be faster
	//		IntSet smaller;
	//		IntSet bigger;
	//		if (a.size() > b.size()) {
	//			bigger = a;
	//			smaller = b;
	//		}
	//		else {
	//			bigger = b;
	//			smaller = a;
	//		}
	//
	//		IntSet cloneSet = new IntOpenHashSet(smaller);
	//		cloneSet.retainAll(bigger);
	//		return cloneSet.size();
	//	}

	//	 Performance test
	//	public static void main(String[] args) {
	//		int numRuns = 10;
	//		int setASize = 20000;
	//		int setBSize = 400000;
	//		int setIntersectSize = 2000;
	//
	//		IntSet a = new IntOpenHashSet();
	//		IntSet b = new IntOpenHashSet();
	//		for (int i = 0; i < setASize; i++) {
	//			a.add(i);
	//		}
	//		for (int i = setASize - setIntersectSize, c = setBSize + setASize - setIntersectSize; i < c; i++) {
	//			b.add(i);
	//		}
	//
	//		long startTime = System.nanoTime();
	//		for (int i = 0; i < 1000; i++) {
	//			int inters = getIntersectCount(a, b);
	//		}
	//		long endTime = System.nanoTime();
	//		System.out.println("Intersect " + numRuns + "x intersect in " + (endTime - startTime) / 1000000 + "ms");
	//	}


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

	public static double defaultDouble(Double value) {
		return (value != null) ? (double) value : 0.0;
	}


	public static <K> int mapAdd(Map<K, Integer> map, K key, int add) {
		int sum = defaultInt(map.get(key)) + add;
		map.put(key, sum);
		return sum;
	}

	public static <K> int mapAvg(Map<K, Integer> map, K key, int add) {
		int sum = (defaultInt(map.get(key)) + add) / 2;
		map.put(key, sum);
		return sum;
	}

	public static <K> long mapAdd(Map<K, Long> map, K key, long add) {
		long sum = defaultLong(map.get(key)) + add;
		map.put(key, sum);
		return sum;
	}

	public static <K> double mapAdd(Map<K, Double> map, K key, double add) {
		double sum = defaultDouble(map.get(key)) + add;
		map.put(key, sum);
		return sum;
	}
}
