package mthesis.concurrent_graph.util;

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
}
