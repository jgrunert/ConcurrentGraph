package mthesis.concurrent_graph.worker;


public class SuperstepStats {
	public int ActiveVertices;
	public int SentControlMessages;
	public int SentVertexMessagesLocal;
	public int SentVertexMessagesUnicast;
	public int SentVertexMessagesBroadcast;
	public int SentVertexMessagesBuckets;
	public int ReceivedCorrectVertexMessages;
	public int ReceivedWrongVertexMessages;
	public int NewVertexMachinesDiscovered;
	public int TotalVertexMachinesDiscovered;
}
