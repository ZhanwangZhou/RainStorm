

# RainStorm

RainStorm is a a stream-processing framework where users can specify a sequence of user-defined transformations on input data that produces streams of output data. RainStorm is based on a hybrid distributed systems (HyDFS) that allows the user to share, store, access, and write files across multiple devices in the network.

## Run with Command Line

Make sure following commands are executed at the project's **root** directory

To compile:

```shell
javac -d out -cp "lib/*" $(find src/ -name "*.java")
```

To start a server with its node ID, ip address, port for UDP, port for TCP, and LRU cache size.

```shell
java -cp "out:lib/*" main.java.hydfs.Leader <node_id> <ip_address> <port_tcp> <port_udp> <cache_size>
```

To start a stream-processing member with its node ID, ip address, port for UDP, port for TCP, and LRU cache size, and port for leader/worker:

```shell
java -cp "out:lib/*" main.java.rainStorm.Leader <node_id> <ip_address> <port_tcp> <port_udp> <cache_size> <leader_port>
```

and

```shell
java -cp "out:lib/*" main.java.rainStorm.Worker <node_id> <ip_address> <port_tcp> <port_udp> <cache_size> <worker_port>
```

## Runtime Commands

### HyDFS

**join <introducer_ip>:<introducer_port>**: join the group with introducer's ip address and TCP port.

**leave**: voluntarily leave the group (different from a failure).

**list_mem**: list the membership list.

**list_self**: list self’s id.

**list_mem_id**: list the membership list with each node's ring id.

**enable_sus / disable_sus**: enable/disable suspicion mode.

**status_sus**: display suspicion on/off status. This will also display a suspected node immediately to stdout at the VM terminal of the suspecting node.

**create \<Local Filepath\>:\<HyDFS Filename\>**: create a HyDFS with the same content as the local file.

**get \<HyDFS Filename\>:\<Local Filepath\>**: create a local file with the same content as the HyDFS file.

**append \<Local Filepath\> \<HyDFS Filename\>**: append all content of the local file to HyDFS file.

**merge \<HyDFS Filename\>**: merge all blocks of a HyDFS file.

**ls \<HyDFS Filename\>**: list the servers that stores the replicas of the HyDFS files.

**quit**: exit the entire program of the current server.

### RainStorm Leader Operations

**RainStorm <hydfs_src_file> <hydfs_dest_filename> <num_tasks>**: initiate RainStorm tasks

**list_server_mem**: list the HyDFS membership list.

**list_workers**: list available workers

**create \<Local Filepath\> \<HyDFS Filename\>**: create file to HyDFS file with the same content as the local file.

### RainStorm Worker Operations

**join <introducer_ip>:<introducer_port> <leader_port>**: join the leader with the leader's ip address and TCP port.

**create \<Local Filepath\> \<HyDFS Filename\>**: create file to HyDFS file with the same content as the local file.

**quit**: shutdown the worker 1.5 seconds after the next job is received.

## Design and Algorithm

**HyDFS File Storage**

In HyDFS, all nodes (devices) are placed inside a ring data structure. Each node maintains: 1. a membership list of all nodes, including their node IDs, IP, ports, current status; 2. a consistent hashing container used to determine the ring ID of each node and the node where each file is stored. When adding a new file to HyDFS, the file will be converted into a block, hashed and stored to a node. To avoid single/two point failures, the node's two sucessors will store the replicas of the block. To add content of an existing file, the content will be treated as a the existing file's new block, hashed and stored to the node. HyDFS also supports multi-append, where multiple files are appended at the same time.

**HyDFS Failure Detection**

HyDFS uses two failure detection modes: base mode and suspicion mode. In base mode, each node periodically sends heartbeats to its ring predecessor and successor. A node will detect the failure of its neightbor if it has not received heartbeat from its neighbor for certain mount of time (e.g. 5 seconds). The node will immediately disseminate the failure message to inform all members of the failure of the neighbor. In suspicion mode, each node instead periodically sends pings to a random member and waits for responses. If no response after certain amount of time, it will mark that member as suspicious and disseminate suspect messages. Both the suspected node and any node later receives ping from the suspected node can disseminate alive messages to eliminate the suspicious state. After another time period, if the node does not receive any alive message about the suspicious member, the node will mark the suspicous member as failed and disseminate failure messages.

**RainStorm Leader Architecture**

The Leader is responsible for accepting RainStorm commands, distributing tasks to Workers, and keeping track of stream tuples. After the user inputs a RainStorm commands, the leader reads from the specified HyDFS file and partition the file content into tuples. The tuples will be queued and wait for distribution to the worker. The Leader communicates with all Workers through TCP. Once a tuple is sent to its corresponding Worker, the Leader appends the tuple ID to a time table and waits for the Worker's execution result. If the result is received on time, the Leader will queue the result for the next operation. Otherwise, the leader will re-send the tuple, or reassign the tuple to another Worker if the current Worker fails. After all tasks are completed, the leader will inform all Workers, aggregate all results, write into the user specified HyDFS destination file, and wait for the user's next command.

**RainStorm Worker Architecture**

Workers serve as the core components for task execution. When a Worker receives a tuple from the Leader, the Worker appends the tuple into its queue if the tuple ID has not occurred before. The Worker continuously pops tuple from its queue for execution. If the task of the tuple is stateless, i.e. each event is independent without regard to previous event, then the Worker will immediately process the tuple by executing the user specified operations. If the task is stateful, the Worker will read the previous state if the key is already in the Worker's cache. Otherwise, the Worker fetches the previous state from HyDFS. Then, the Worker processes the tuple by executing the user specified operations. To reduce the load balance of the communication with the Leader and HyDFS, Workers update the processing results into their own caches. Once the cache is full, the Worker will then send all cached results back to the leader and write them to HyDFS. When the Worker receives the task completion notification from the leader, the Worker will clean up all cached data and queues, release resources, and print logs.