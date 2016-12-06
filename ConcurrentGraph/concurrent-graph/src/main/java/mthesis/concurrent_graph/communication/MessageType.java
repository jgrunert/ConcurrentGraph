package mthesis.concurrent_graph.communication;

public enum MessageType {
	// Vertex to vertex message
	Vertex,

	// Message from workers to signal master that a superstep is finished
	Control_Node_Superstep_Finished,
	// Message from workers to signal master that the worker is completely finished
	Control_Node_Finished,

	// Message to signal workers to start with next superstep
	Control_Master_Next_Superstep,
	// Message to signal workers to finish, output and terminate
	Control_Master_Finish,
	// Message signal terminate
	//Control_Master_Terminate
}
