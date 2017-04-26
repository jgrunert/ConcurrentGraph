# Overview
[TODO Name] is a graph processing system that can process multiple queries concurrently in an optimized way.

# Architecture
TODO Master, Worker, Input, Output, Plotting, Apps

# Shortest Path Example

## Local Test Runner
SPLocalTestClusterMain can be used to start a local test cluster with master and workers on the local machine.
In the java project the local test runner can be started using the main method of mthesis.concurrent_graph.apps.shortestpath.SPSingleMasterMain

```
Usage: [configFile] [clusterConfigFile] [inputFile] [optional extraJvmPerWorker-bool]
```

configFile is a java properties file that defines the system configurations such as log level, networking configuration and operation modes. A default configuration file can be found in configs/configuration.properties

clusterConfigFile configures the local test cluster. For example configs/clusterconfig_local8.txt configures a local test with 8 workers.

inputFile must be a graph file TODO Format. One possibility to create this file is using https://github.com/jgrunert/SimpleOSM2Graph by converting OSM data to road network graphs.

If extraJvmPerWorker [experimental] is enabled, each worker will be started in a sepatarate jvm.

## Cluster Deployment
TODO

## Input Partitioning
There are different input partitioning strategies available: Direct, Hashed and [TODO LDG]. 
Direct means that the partitions are directly assigned, the vertex order is taken from the input files. 
Hashing means that vertices are distributed on partitions using a hash function.

The PartitioningStrategySelector selects a partitioning strategy based on the InputPartitioner config value.
