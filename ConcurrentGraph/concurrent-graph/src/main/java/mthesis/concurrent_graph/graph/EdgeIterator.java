//package mthesis.concurrent_graph.graph;
//
//import java.util.Iterator;
//
//import mthesis.concurrent_graph.writable.BaseWritable;
//
///**
// * Iterator object to iterate over edges.
// *
// * @author semihsalihoglu
// *
// * @param <E> Writable object for the types of values on the edges.
// */
//public class EdgeIterator<E extends BaseWritable> implements Iterator<Edge<E>> {
//
//	private Edge<E> edge;
//	private int[] neighborIds;
//	private int numNeighbors;
//	private int currentNeighborIdLocation = -1;
//	protected byte[][] edgeValueBytes = null;
//	public E representativeEdgeValueInstance = null;
//
//	public EdgeIterator() {
//		this.edge = new Edge<E>();
//		this.neighborIds = null;
//		this.numNeighbors = -1;
//		this.edgeValueBytes = null;
//		this.currentNeighborIdLocation = -1;
//		this.representativeEdgeValueInstance = null;
//	}
//
//	@Override
//	public boolean hasNext() {
//		return currentNeighborIdLocation < numNeighbors;
//	}
//
//	@Override
//	public Edge<E> next() {
//		representativeEdgeValueInstance.read(edgeValueBytes[currentNeighborIdLocation], 0);
//		this.edge.setNeighborId(neighborIds[currentNeighborIdLocation++]);
//		return this.edge;
//	}
//
//	public void init(byte[][] edgeValueBytes, int[] neighborIds, int numNeighbors) {
//		this.neighborIds = neighborIds;
//		this.currentNeighborIdLocation = 0;
//		this.numNeighbors = numNeighbors;
//		this.edgeValueBytes = edgeValueBytes;
//	}
//
//	public void reset() {
//		this.currentNeighborIdLocation = 0;
//	}
//
//	public void setRepresentativeWritableInstance(E representativeEdgeValueInstance) {
//		this.representativeEdgeValueInstance = representativeEdgeValueInstance;
//		this.edge.setEdgeValue(this.representativeEdgeValueInstance);
//	}
//
//	@Override
//	public void remove() {
//		throw new UnsupportedOperationException(this.getClass().getName() +
//				" does not support removing elements.");
//	}
//}
