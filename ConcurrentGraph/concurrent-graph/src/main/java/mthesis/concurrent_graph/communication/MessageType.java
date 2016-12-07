package mthesis.concurrent_graph.communication;

public enum MessageType {
	// Vertex to vertex message
	Vertex,

	// Message from workers to signal master that a superstep is finished
	Control_Worker_Superstep_Finished,
	// Message from workers to workers to signal superstep barrier in a communication channel
	Control_Worker_Superstep_Channel_Barrier,
	// Message from workers to signal master that the worker is completely finished
	Control_Worker_Finished,

	// Message to signal workers to start with next superstep
	Control_Master_Next_Superstep,
	// Message to signal workers to finish, output and terminate
	Control_Master_Finish
}
