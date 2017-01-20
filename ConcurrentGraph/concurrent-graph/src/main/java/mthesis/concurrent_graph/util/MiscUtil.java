package mthesis.concurrent_graph.util;

import java.util.Set;

public class MiscUtil {

	public static <T> int getIntersectCount(Set<T> a, Set<T> b) {
		int intersects = 0;
		for (T v : a) {
			if (b.contains(v))
				intersects++;
		}
		return intersects;
	}
}
