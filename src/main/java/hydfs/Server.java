package main.java.hydfs;

import java.io.*;
import java.net.ServerSocket;
import java.net.*;
import org.json.*;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Server {
    public final int nodeId; // self node id
    final String ipAddress; // self node's ip address
    final int portTCP; // self node's  tcp port
    final int portUDP; // self node's udp port

    public Boolean running; //  if self node is running
    private Boolean failureDetectionMode; // suspicion mode flag: false = heartbeat and true = PingAck+S

    public final ConsistentHashing ch; // Consistent Hashing object used to hash servers and files
    final LRUCache lruCache; // LRU cache storing recently read files
    final Set<String> localFiles; // set of HyDFS files on this server
    final Map<String, Integer> fileBlockMap; // map storing number of blocks of each HyDFS file
    public final HashMap<Integer, Node> membership; // membership of all nodes with node id as keys
    private long predecessorLastPingTime; // time of last ping received from the predecessor
    private int predecessorLastPingId; //  id of last pinging predecessor
    private long successorLastPingTime; // time of last ping received from the successor
    private int successorLastPingId; // id of the last pinging successor
    final Map<Integer, Long> lastPingTimes; // each node's time of last ping
    final Map<Integer, Integer> incarnationNumbers; // each node's local incarnation number
    final Map<String, Set<Integer>> unreceivedBlocks; // unreceived block during get and merge
    private Thread tempThread = null;

    final ServerSocket tcpServerSocket;
    final Logger logger;
    final Clock clock;


    /*
    Server class constructor
     */
    public Server(String[] args) throws IOException, NoSuchAlgorithmException {
        this.nodeId = Integer.parseInt(args[0]);
        this.ipAddress = args[1];
        this.portTCP = Integer.parseInt(args[2]);
        this.portUDP = Integer.parseInt(args[3]);
        this.lruCache = new LRUCache(Integer.parseInt(args[4]));

        this.running = true;
        this.failureDetectionMode = false;

        this.ch = new ConsistentHashing();
        this.localFiles = new HashSet<>();
        this.fileBlockMap = new HashMap<>();
        this.membership = new HashMap<>();
        this.lastPingTimes = new HashMap<>();
        this.incarnationNumbers = new HashMap<>();
        this.unreceivedBlocks = new HashMap<>();

        this.tcpServerSocket = new ServerSocket(portTCP);
        this.logger = Logger.getLogger("Server");
        this.clock = Clock.systemDefaultZone();

        ch.addServer(nodeId);
        membership.put(nodeId, new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        incarnationNumbers.put(nodeId, 0);

        logger.setLevel(Level.WARNING);

        // Initialize a directory to store HyDFS files
        File directory = new File("HyDFS" + nodeId);
        if (Files.exists(Paths.get(directory.getAbsolutePath()))) {
            deleteDirectoryRecursively(Paths.get(directory.getAbsolutePath()));
        }
        boolean created = directory.mkdir();
        if(created) {
            logger.info("HyDFS directory created");
        }else{
            logger.info("Failed to create HyDFS directory");
        }
        // Initialize a directory to cache recently read files
        File cacheDirectory = new File("Cache" + nodeId);
        if (Files.exists(Paths.get(cacheDirectory.getAbsolutePath()))) {
            deleteDirectoryRecursively(Paths.get(cacheDirectory.getAbsolutePath()));
        }
        created = cacheDirectory.mkdir();
        if(created) {
            logger.info("Cache directory created");
        }else{
            logger.info("Failed to create cache directory");
        }
    }


    /*
    Server-side TCP listen.
    Assign different types of incoming messages to their corresponding handle functions
     */
    public void tcpListen() {
        logger.info("Start TCP Listen");
        try{
            while(running){
                Socket tcpSocket = tcpServerSocket.accept();
                tcpSocket.setSoTimeout(5000);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(tcpSocket.getInputStream())
                );
                String jsonString = reader.readLine();
                if (jsonString == null) continue;
                JSONObject receivedMessage = new JSONObject(jsonString);
                String messageType = receivedMessage.getString("type");
                // System.out.println(receivedMessage);
                switch(messageType) {
                    case "Join":
                        handleJoin(receivedMessage);
                        break;
                    case "JoinResponse":
                        handleJoinResponse(receivedMessage);
                        break;
                    case "Leave":
                        handleLeave(receivedMessage);
                        break;
                    case "Failure":
                        handleFailure(receivedMessage);
                        break;
                    case "CreateFile":
                        handleCreateFile(receivedMessage);
                        break;
                    case "UpdateFile":
                        handleUpdateFile(receivedMessage);
                        break;
                    case "GetFile":
                        handleGetFile(receivedMessage);
                        break;
                    case "GetFromReplica":
                        handleGetFromReplica(receivedMessage);
                        break;
                    case "GetFileResponse":
                        handleGetFileResponse(receivedMessage);
                        break;
                    case "GetFromReplicaResponse":
                        handleGetFromReplicaResponse(receivedMessage);
                        break;
                    case "AppendFile":
                        handleAppendFile(receivedMessage);
                        break;
                    case "AppendFileFrom":
                        handleAppendMultiFiles(receivedMessage);
                        break;
                    case "MergeFile":
                        new Thread(() -> this.handleMergeFile(receivedMessage)).start();
                        break;
                    case "MergeFileFrom":
                        handleMergeFileFrom(receivedMessage);
                        break;
                    case "MergeFileResponse":
                        handleMergeFileResponse(receivedMessage);
                        break;
                    case "MergeFileFromResponse":
                        handleMergeFileFromResponse(receivedMessage);
                        break;
                    case "ModeUpdate":
                        handleModeUpdate(receivedMessage);
                        break;
                    case "RecreateFile":
                        handleRecreateFile(receivedMessage);
                        break;
                    default:
                        logger.warning("Unknown message type: " + messageType);
                }
            }
        }catch(IOException e){
            logger.info("Cannot read from TCP packet\n" + e.getMessage());
        }
    }


    /*
    Server-side UDP listen.
    Assign different types of incoming messages to their corresponding handle functions
     */
    public void udpListen(){
        logger.info("Start UDP Listen");
        try(DatagramSocket socket = new DatagramSocket(portUDP)){
            while(running){
                byte[] buffer = new byte[4096];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                ObjectInputStream ois = new ObjectInputStream(bais);
                JSONObject receivedMessage = new JSONObject((String) ois.readObject());
                String messageType = receivedMessage.getString("type");
                // System.out.println(receivedMessage);
                switch(messageType) {
                    case "Ping":
                        handlePing(receivedMessage);
                        break;
                    case "PingAck":
                        handlePingAck(receivedMessage);
                        break;
                    case "Suspect":
                        handleSuspect(receivedMessage);
                        break;
                    case "Alive":
                        handleAlive(receivedMessage);
                        break;
                    default:
                        logger.warning("Unknown message type: " + messageType);
                }
            }
        }catch(IOException | ClassNotFoundException e){
            logger.info("Cannot read from UDP packet\n" + e.getMessage());
        }
    }


    /*
    Periodical ping for failure detection.
    Regular mode: send heartbeats to predecessor and successor
    Suspicion mode: ping a random node and wait for ack
     */
    public void ping(){
        if (!running) return;
        JSONObject pingMessage = new JSONObject();
        pingMessage.put("type", "Ping");
        pingMessage.put("nodeId", nodeId);
        if (failureDetectionMode) {
            List<Integer> availableMembers = new ArrayList<>();
            for(int receiverId : membership.keySet()){
                if (membership.get(receiverId).getStatus().equals("alive")
                        || membership.get(receiverId).getStatus().equals("suspect")) {
                    availableMembers.add(receiverId);
                }
            }
            Node receiver = membership.get(availableMembers.get(
                    (int)(Math.random() * availableMembers.size())
            ));
            lastPingTimes.put(receiver.getNodeId(), clock.millis());
            sendUDP(receiver.getIpAddress(), receiver.getPortUDP(), pingMessage);
        } else {
            for (int receiverId : Arrays.asList(ch.getSuccessor(nodeId), ch.getPredecessor(nodeId))) {
                Node receiver = membership.get(receiverId);
                sendUDP(receiver.getIpAddress(), receiver.getPortUDP(), pingMessage);
            }
        }
    }


    /*
    Periodical ping check for failure detection.
    Regular mode: check heartbeats from predecessor and successor
    Suspicion mode: check ack from pinged nodes
     */
    public void checkPing() {
        if(!running) return;
        long currentTime = clock.millis();
        if (failureDetectionMode) {
            currentTime = clock.millis();
            for (Map.Entry<Integer, Long> entry : lastPingTimes.entrySet()) {
                int nodeId = entry.getKey();
                Node member = membership.get(nodeId);
                if (member == null) continue;
                long lastPingTime = entry.getValue();
                if (currentTime - lastPingTime > 3000 && member.getStatus().equals("alive")) {
                    JSONObject suspicionMessage = new JSONObject();
                    suspicionMessage.put("type", "Suspect");
                    suspicionMessage.put("nodeId", nodeId);
                    suspicionMessage.put("incarnation", incarnationNumbers.get(nodeId));
                    suspicionMessage.put("gossipCount", 0);
                    gossip(suspicionMessage, false);
                    lastPingTimes.put(nodeId, currentTime);
                } else if (currentTime - lastPingTime > 5000 && member.getStatus().equals("suspect")) {
                    JSONObject failureMessage = new JSONObject();
                    failureMessage.put("type", "Failure");
                    failureMessage.put("nodeId", nodeId);
                    failureMessage.put("gossipCount", 0);
                    gossip(failureMessage, true);
                }
            }
        } else {
            if(currentTime - predecessorLastPingTime > 5000) {
                JSONObject failureMessage = new JSONObject();
                failureMessage.put("type", "Failure");
                failureMessage.put("nodeId", this.predecessorLastPingId);
                failureMessage.put("gossipCount", 0);
                gossip(failureMessage, true);
            }
            if(currentTime - successorLastPingTime > 5000) {
                JSONObject failureMessage = new JSONObject();
                failureMessage.put("type", "Failure");
                failureMessage.put("nodeId", this.successorLastPingId);
                failureMessage.put("gossipCount", 0);
                gossip(failureMessage, true);
            }
        }
    }


    /*
    Send "Join" request to the introducer to join the network
     */
    public void join(String introIpAddress, int introPort){
        running = true;
        membership.clear();
        membership.put(nodeId, new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        JSONObject joinMessage = new JSONObject();
        joinMessage.put("type", "Join");
        joinMessage.put("nodeId", nodeId);
        joinMessage.put("ipAddress", ipAddress);
        joinMessage.put("portUDP", portUDP);
        joinMessage.put("portTCP", portTCP);
        sendTCP(introIpAddress, introPort, joinMessage);
        logger.info("Sent Join-Req Message");
    }


    /*
    Disseminate "Leave" message and temporarily leave the network
     */
    public void leave() {
        membership.get(nodeId).setStatus("leave");
        running = false;
        ch.removeServer(nodeId);
        JSONObject leaveMessage = new JSONObject();
        leaveMessage.put("type", "Leave");
        leaveMessage.put("nodeId", nodeId);
        leaveMessage.put("gossipCount", 0);
        gossip(leaveMessage, true);
    }


    /*
    Send "CreateFile" request to destination nodes to add a local file as a new HyDFS file
    Disseminate "UpdateFile" message to inform all nodes of the file update
     */
    public void createFile(String localFilename, String hydfsFilename) {
        if(fileBlockMap.containsKey(hydfsFilename)){
            System.out.println("Filename already exists in HyDFS");
            return;
        }
        int receiverId = ch.getServer("1_" + hydfsFilename);
        JSONObject createFileMessage = new JSONObject();
        createFileMessage.put("type", "CreateFile");
        createFileMessage.put("hydfsFilename", hydfsFilename);
        createFileMessage.put("blockNum", 1);
        try {
            byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
            createFileMessage.put("blockData", Base64.getEncoder().encodeToString(fileContent));
        } catch (IOException e) {
            logger.warning("Failed to read from the local file" + localFilename + e.getMessage());
            return;
        }
        for (int memberId : Arrays.asList(
                receiverId, ch.getSuccessor(receiverId), ch.getSuccessor2(receiverId)
        )) {
            Node member = membership.get(memberId);
            sendTCP(member.getIpAddress(), member.getPortTCP(), createFileMessage);
        }
        logger.info("Sent local file " + localFilename + " for HyDFS file creation.");
    }


    /*
    Send "GetFile" request to nodes for their file blocks.
    Concatenate all file blocks and add the concatenated file to cache.
    Copy the cache file to specified local file path.
     */
    public void getFile(String hydfsFilename, String localFilename) {
        // If the file is not inside HyDFS, return
        if(!fileBlockMap.containsKey(hydfsFilename)) {
            System.out.println("The file to be gotten does not exist in HyDFS");
            return;
        }
        String cacheFilename = "Cache" + nodeId + "/" + hydfsFilename;
        Path cacheFilePath = Paths.get(cacheFilename);
        // If the file is not inside cache, get the file from HyDFS and add it to cache
        if(fileBlockMap.get(hydfsFilename) != lruCache.get(hydfsFilename)){
            int blockNum = fileBlockMap.get(hydfsFilename);
            unreceivedBlocks.put(hydfsFilename, new HashSet<>());
            for(int i = 1; i <= blockNum; ++i) unreceivedBlocks.get(hydfsFilename).add(i);
            // Keep sending "GetFile" request until all blocks are received
            int secondsPassed = 0;
            while(!unreceivedBlocks.get(hydfsFilename).isEmpty()){
                if(secondsPassed % 3 == 0) {
                    for(int i: unreceivedBlocks.get(hydfsFilename)) {
                        Node receiver = membership.get(ch.getServer(i + "_" + hydfsFilename));
                        if(secondsPassed % 9 == 3) {
                            receiver = membership.get(ch.getSuccessor(receiver.getNodeId()));
                        } else if(secondsPassed % 9 == 6) {
                            receiver = membership.get(ch.getSuccessor2(receiver.getNodeId()));
                        }
                        JSONObject getFileMessage = new JSONObject();
                        getFileMessage.put("type", "GetFile");
                        getFileMessage.put("nodeId", nodeId);
                        getFileMessage.put("hydfsFilename", hydfsFilename);
                        getFileMessage.put("blockId", i);
                        getFileMessage.put("blockName", i + "_" + hydfsFilename);
                        sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), getFileMessage);
                    }
                }
                secondsPassed += 1;
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Received all file blocks for " + hydfsFilename);
            // Delete outdated cache file of the HyDFS file to be read
            try {
                Files.deleteIfExists(cacheFilePath);
            }catch(IOException e) {
                logger.warning("Failed to delete outdated cache file" + cacheFilename);
                return;
            }
            logger.info("Delete outdated cache file of " + hydfsFilename);
            // Append all block contents to the cached file
            List<String> blockFiles = new ArrayList<>();
            for(int i = 1; i <= blockNum; ++i){
                blockFiles.add("Cache" + nodeId + "/" + i + "_" + hydfsFilename);
            }
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(cacheFilename))) {
                for(String blockFile: blockFiles){
                    Path blockFilePath = Paths.get(blockFile);
                    writer.write(Files.readString(blockFilePath));
                    Files.deleteIfExists(blockFilePath);
                }
            } catch(IOException e) {
                logger.warning("Failed to write cache file " + cacheFilename);
            }
            logger.info("Appended all blocks to cache file of " + hydfsFilename);
            // Delete LRU cache file
            String deletedFilename = lruCache.put(hydfsFilename, fileBlockMap.get(hydfsFilename));
            try {
                if (deletedFilename != null) {
                    Files.deleteIfExists(Paths.get("Cache" + nodeId + "/" + deletedFilename));
                }
            } catch(IOException e) {
                logger.warning("Failed to delete LRU cache file");
            }
        }
        // Copy cached file to the specified local file path
        try {
        Files.copy(cacheFilePath, Paths.get(localFilename), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.warning("Failed to copy cache file to local path " + localFilename);
        }
        System.out.println("Succeed to get " + localFilename + " from HyDFS file " + hydfsFilename);
    }


    /*
    Send "GetFromReplica" request to get the first block of a HyDFS file from specified node.
    The target node should be specified with its <IP Address>:<TCP Port>.
    Mainly for debug purpose.
     */
    public void getFromReplica(String targetAddressPort, String hydfsFilename, String localFilename) {
        String[] addressParts = targetAddressPort.split(":");
        String ipAddress = addressParts[0];
        int port = Integer.parseInt(addressParts[1]);
        JSONObject getFileRequest = new JSONObject();
        getFileRequest.put("type", "GetFromReplica");
        getFileRequest.put("nodeId", nodeId);
        getFileRequest.put("blockName", "1_" + hydfsFilename);
        getFileRequest.put("localFilename", localFilename);
        sendTCP(ipAddress, port,getFileRequest);
        logger.info("Sent GetFromReplica request to replica at " + ipAddress + ":" + port);
    }


    /*
    Send "AppendFile" request to file block 1 owner to append local file content to a HyDFS file
     */
    public void appendFile(String localFilename, String hydfsFilename) {
        if(!fileBlockMap.containsKey(hydfsFilename)) {
            System.out.println("The file to be appended does not exist in HyDFS");
            return;
        }
        int blockId = fileBlockMap.get(hydfsFilename) + 1;
        Node receiver = membership.get(ch.getServer("1_" + hydfsFilename));
        try {
            byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
            JSONObject appendFileMessage = new JSONObject();
            appendFileMessage.put("type", "AppendFile");
            appendFileMessage.put("hydfsFilename", hydfsFilename);
            appendFileMessage.put("blockId", blockId);
            appendFileMessage.put("blockData", Base64.getEncoder().encodeToString(fileContent));
            sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), appendFileMessage);
        } catch (IOException e) {
            logger.warning("Failed to read from local file " + localFilename);
        }

    }


    /*
    Append multiple local files to a HyDFS file from different nodes concurrently
     */
    public void appendMultiFiles(String hydfsFilename, String nodeIds, String localFilenames) {
        // Process params
        if(!fileBlockMap.containsKey(hydfsFilename)) {
            System.out.println("The file to be appended does not exist in HyDFS");
            return;
        }
        String[] nodeIdArray = nodeIds.replaceAll("\\s", "").split(",");
        String[] localFilenameArray = localFilenames.replaceAll("\\s", "").split(",");
        if (nodeIdArray.length > localFilenameArray.length) {
            System.out.println("Please specify number of node IDs <= number of local filepath");
            return;
        }
        // Initiate a new thread to check the start and end time of multi-append
        if (tempThread != null) tempThread.interrupt();
        tempThread = new Thread(() -> appendMultiFilesTimeCheck(hydfsFilename, nodeIds));
        tempThread.start();
        // Send "AppendFileFrom" request to specified nodes
        for (int i = 0; i < localFilenameArray.length; ++i) {
            JSONObject appendFileRequestMessage = new JSONObject();
            appendFileRequestMessage.put("type", "AppendFileFrom");
            appendFileRequestMessage.put("hydfsFilename", hydfsFilename);
            appendFileRequestMessage.put("localFilename", localFilenameArray[i]);
            Node receiver;
            try {
                receiver = membership.get(Integer.parseInt(nodeIdArray[i % nodeIdArray.length]));
            } catch (NumberFormatException e) {
                System.out.println("Please specify node IDs as integers");
                return;
            }
            sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), appendFileRequestMessage);
        }
    }


    /*
    Send "Merge" request to initiate merge process.
    All blocks of the specified HyDFS file will be merged into one block.
     */
    public void mergeFile(String hydfsFilename) {
        if (!fileBlockMap.containsKey(hydfsFilename)) {
            System.out.println("The file to be merged does not exist in HyDFS");
            return;
        }
        if (tempThread != null) tempThread.interrupt();
        tempThread = new Thread(() -> mergeTimeCheck(hydfsFilename));
        tempThread.start();
        JSONObject mergeRequest = new JSONObject();
        mergeRequest.put("type", "MergeFile");
        mergeRequest.put("hydfsFilename", hydfsFilename);
        mergeRequest.put("requesterNodeId", this.nodeId);
        Node receiver = membership.get(ch.getServer("1_" + hydfsFilename));
        sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), mergeRequest);
        logger.info("Sent merge request for " + hydfsFilename + " to node " + receiver.getNodeId());
    }


    /*
    Initiates multi-append and merge operations on a specified file.
    For test purpose only.
     */
    public void appendMultiFilesAndMerge(String hydfsFilename, String nodeIds, String localFilenames) {
        appendMultiFiles(hydfsFilename, nodeIds, localFilenames);
        try {
            tempThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        mergeFile(hydfsFilename);
    }


    /*
    List all HyDFS files and their blocks stored on this server
     */
    public void listSelfStorage() {
        System.out.println("Node Id = " + this.nodeId + "; Ring ID = " + ch.getRingId(this.nodeId));
        if (localFiles.isEmpty()) {
            System.out.println("No files currently stored on server " + nodeId);
        } else {
            System.out.println("Files currently stored on server " + nodeId + ":");
            for (String localFile: localFiles) System.out.println(localFile);
        }
    }


    /*
    List all blocks of the specified HyDFS file and their locations
     */
    public void listFileLocation(String filename) {
        System.out.println("File " + filename + " has " + fileBlockMap.get(filename) + " blocks.");
        for(int i = 1; i <= fileBlockMap.get(filename); ++i) {
            String blockName = i + "_" + filename;
            System.out.println("The block " + blockName + " is stored at following nodes:");
            Node server1 = membership.get(ch.getServer(blockName));
            Node server2 = membership.get(ch.getSuccessor(server1.getNodeId()));
            Node server3 = membership.get(ch.getSuccessor2(server1.getNodeId()));
            for(Node server: Arrays.asList(server1, server2, server3)){
                System.out.println(
                        "Node ID = " + server.getNodeId()
                        + "; IP Address = " + server.getIpAddress()
                        + "; Node Status = " + server.getStatus()
                        + "; Ring ID = " + ch.getRingId(server.getNodeId())
                );
            }
        }
    }


    /*
    Print self node ID.
     */
    public void listSelf() {
        System.out.println("NodeId = " + nodeId);
    }


    /*
    List current membership.
    List each node's ID, IP address, and status.
     */
    public void listMem() {
        System.out.println("Current Membership List");
        for(int nodeId: membership.keySet()){
            System.out.println(
                    "Node ID = " + nodeId
                    + "; IP Address = " + membership.get(nodeId).getIpAddress()
                    + "; Node Status = " + membership.get(nodeId).getStatus()
            );
        }
    }


    /*
    List current membership.
    List each node's ID, IP address, status, and ring ID.
     */
    public void listMemRingIds() {
        System.out.println("Current Membership List and Ring IDs:");
        for(int nodeId: membership.keySet()){
            System.out.println(
                    "Node ID = " + nodeId
                    + "; IP Address = " + membership.get(nodeId).getIpAddress()
                    + "; Node Status = " + membership.get(nodeId).getStatus()
                    + "; Ring ID = " + ch.getRingId(nodeId)
            );
        }
    }


    /*
    Print current server mode.
     */
    public void statusSus() {
        System.out.println(failureDetectionMode ? "Suspicion mode enabled" : "Suspicion mode disabled");
    }


    /*
     Change suspicion mode.
     Disseminate "ModeUpdate" Message to inform all nodes of the mode update.
     */
    public void switchMode(boolean suspicionMode) {
        this.failureDetectionMode = suspicionMode;
        if (suspicionMode) {
            System.out.println("Suspicion mode enabled");
        } else {
            System.out.println("Suspicion mode disabled");
        }
        JSONObject suspicionMessage = new JSONObject();
        suspicionMessage.put("type", "ModeUpdate");
        suspicionMessage.put("suspicionMode", suspicionMode ? "enabled" : "disabled");
        suspicionMessage.put("gossipCount", 0);
        gossip(suspicionMessage, true);
    }


    /*
    Handle "Ping" messages.
    Regular mode: record ping receiving time and sender node id.
    Suspicion mode: respond with "PingAck" message.
     */
    private void handlePing(JSONObject message) {
        if (failureDetectionMode){
            JSONObject pingAckMessage = new JSONObject();
            pingAckMessage.put("type", "PingAck");
            pingAckMessage.put("nodeId", nodeId);
            pingAckMessage.put("incarnation", incarnationNumbers.get(nodeId));
            Node receiver = membership.get(message.getInt("nodeId"));
            sendUDP(receiver.getIpAddress(), receiver.getPortUDP(), pingAckMessage);
        } else {
            if (message.getInt("nodeId") == ch.getPredecessor(nodeId)) {
                predecessorLastPingTime = clock.millis();
                predecessorLastPingId = message.getInt("nodeId");
            }
            if (message.getInt("nodeId") == ch.getSuccessor(nodeId)) {
                successorLastPingTime = clock.millis();
                successorLastPingId = message.getInt("nodeId");
            }
        }
    }


    /*
    Handle "PingAck" messages.
    Remove the sender from lastPingTimes tracking list.
    Disseminate "Alive" message for the sender if it is being suspected.
     */
    private void handlePingAck(JSONObject message) {
        int senderNodeId = message.getInt("nodeId");
        int receivedIncarnation = message.getInt("incarnation");
        lastPingTimes.remove(senderNodeId);
        logger.info("Received PingAck from node: " + senderNodeId);
        Node senderNode = membership.get(senderNodeId);
        int currentIncarnation = incarnationNumbers.getOrDefault(senderNodeId, 0);
        if (senderNode.getStatus().equals("suspect") && receivedIncarnation > currentIncarnation) {
            incarnationNumbers.put(senderNodeId, receivedIncarnation);
            senderNode.setStatus("alive");
            JSONObject statusUpdateMessage = new JSONObject();
            statusUpdateMessage.put("type", "Alive");
            statusUpdateMessage.put("nodeId", senderNodeId);
            statusUpdateMessage.put("incarnation", receivedIncarnation);
            gossip(statusUpdateMessage, false);
        }
    }


    /*
    Handle "Alive" message.
    Suspicion mode only: check incarnation number and set suspected node to alive.
     */
    private void handleAlive(JSONObject message) {
        int aliveNodeId = message.getInt("nodeId");
        int receivedIncarnation = message.getInt("incarnation");
        int currentIncarnation = incarnationNumbers.getOrDefault(aliveNodeId, 0);
        if (receivedIncarnation > currentIncarnation) {
            membership.get(aliveNodeId).setStatus("alive");
            incarnationNumbers.put(aliveNodeId, receivedIncarnation);
            lastPingTimes.remove(aliveNodeId);
            logger.info("Node " + aliveNodeId + "status set to alive");
            gossip(message, false);
        }
    }


    /*
    Handle "Suspect" message.
    Suspicion mode only: disseminate "Alive" message if this node is suspected;
    Otherwise, update suspected node status.
     */
    private void handleSuspect(JSONObject message) {
        int suspectNodeId = message.getInt("nodeId");
        int incarnationNumber = message.getInt("incarnation");
        if (suspectNodeId == nodeId) {
            if (incarnationNumber > incarnationNumbers.get(nodeId)) {
                incarnationNumbers.put(nodeId, incarnationNumber + 1);
                JSONObject aliveMessage = new JSONObject();
                aliveMessage.put("type", "Alive");
                aliveMessage.put("nodeId", nodeId);
                aliveMessage.put("incarnation", incarnationNumbers.get(nodeId));
                aliveMessage.put("gossipCount", 0);
                gossip(aliveMessage, false);
            }
        } else {
            membership.get(suspectNodeId).setStatus("suspect");
            if (incarnationNumber > incarnationNumbers.get(suspectNodeId))
                incarnationNumbers.put(suspectNodeId, incarnationNumber);
            gossip(message, false);
        }
    }


    /*
    Handle "Join" message.
    Add the new node as member.
    If this node is introducer, respond to the requester, add gossipCount, and start gossip.
     */
    private void handleJoin(JSONObject message) {
        int joiningNodeId = message.getInt("nodeId");
        String joiningNodeIp = message.getString("ipAddress");
        int joiningNodePortUDP = message.getInt("portUDP");
        int joiningNodePortTCP = message.getInt("portTCP");
        Node joiningNode = new Node(joiningNodeId, joiningNodeIp, joiningNodePortUDP, joiningNodePortTCP);
        membership.put(joiningNodeId, joiningNode);
        ch.addServer(joiningNodeId);
        if (!incarnationNumbers.containsKey(joiningNodeId)) {
            incarnationNumbers.put(joiningNodeId, 0);
        }
        logger.info("Node " + joiningNode.getNodeId() + " joined successfully");
        if(!message.has("gossipCount")) {
            message.put("gossipCount", 0);
            JSONObject joinResponseMessage = new JSONObject();
            joinResponseMessage.put("type", "JoinResponse");
            JSONArray membersArray = new JSONArray();
            for (Node member : membership.values()) {
                JSONObject memberInfo = new JSONObject();
                memberInfo.put("nodeId", member.getNodeId());
                memberInfo.put("ipAddress", member.getIpAddress());
                memberInfo.put("portUDP", member.getPortUDP());
                memberInfo.put("portTCP", member.getPortTCP());
                memberInfo.put("status", member.getStatus());
                membersArray.put(memberInfo);
            }
            joinResponseMessage.put("members", membersArray);
            sendTCP(joiningNodeIp, joiningNodePortTCP, joinResponseMessage);
            logger.info("Send membership update message to new joined node" + joiningNodeId);
        }
        gossip(message, true);
    }


    /*
    Handle "JoinResponse".
    Read and add membership within the response.
     */
    private void handleJoinResponse(JSONObject message) {
        logger.info("Node " + this.nodeId + " received JoinResponse");
        JSONArray membershipArray = message.getJSONArray("members");
        membership.clear();
        for (int i = 0; i < membershipArray.length(); i++) {
            JSONObject memberInfo = membershipArray.getJSONObject(i);
            int memberId = memberInfo.getInt("nodeId");
            String memberIp = memberInfo.getString("ipAddress");
            int memberPortUDP = memberInfo.getInt("portUDP");
            int memberPortTCP = memberInfo.getInt("portTCP");
            String memberStatus = memberInfo.getString("status");
            Node memberNode = new Node(memberId, memberIp, memberPortUDP, memberPortTCP, memberStatus);
            membership.put(memberId, memberNode);
            ch.addServer(memberId);
            if (!incarnationNumbers.containsKey(memberId)) {
                incarnationNumbers.put(memberId, 0);
            }
        }
        System.out.println("Join successfully");
        logger.info("Finish updating membership List from introducer");
    }


    /*
    Handle "Leave" message.
    Update membership and re-replicate files as needed.
     */
    private void handleLeave(JSONObject message) {
        int leftNodeId = message.getInt("nodeId");
        if(ch.getRingId(leftNodeId) != -1) {
            reReplicationOnDrop(leftNodeId);
            membership.get(leftNodeId).setStatus("leave");
            System.out.println("Node" + leftNodeId + " has left. Set its status to \"leave\"");
            gossip(message, true);
        }
    }


    /*
    Handle "Failure" message.
    Remove failed node from membership and re-replicate files as needed.
     */
    private void handleFailure(JSONObject message) {
        int failedNodeId = message.getInt("nodeId");
        if(ch.getRingId(failedNodeId) != -1){
            reReplicationOnDrop(failedNodeId);
            membership.remove(failedNodeId);
            incarnationNumbers.remove(failedNodeId);
            lastPingTimes.remove(failedNodeId);
            System.out.println("Failed node " + failedNodeId + " has been removed from membership");
            if (this.nodeId == failedNodeId) this.running = false;
            gossip(message, true);
        }
    }


    /*
    Handle "CreateFile" request.
    Create a new file block with given blockNum in HyDFS and save it on this node.
     */
    private void handleCreateFile(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFilename");
        int blockNum = message.getInt("blockNum");
        byte[] data = Base64.getDecoder().decode(message.getString("blockData"));
        String filePath = "HyDFS" + nodeId + "/" + blockNum + "_" + hydfsFilename;
        try {
            Files.write(Paths.get(filePath), data);
        } catch (IOException e) {
            logger.warning("Failed to save block to local HyDFS directory.");
            return;
        }
        localFiles.add(blockNum + "_" + hydfsFilename);
        // Update fileBlockMap only if message blockNum is higher to avoid conflict on multi-append
        if (!fileBlockMap.containsKey(hydfsFilename) || blockNum > fileBlockMap.get(hydfsFilename)) {
            fileBlockMap.put(hydfsFilename, blockNum);
            JSONObject updateFileMessage = new JSONObject();
            updateFileMessage.put("type", "UpdateFile");
            updateFileMessage.put("hydfsFilename", hydfsFilename);
            updateFileMessage.put("blockNum", blockNum);
            updateFileMessage.put("gossipCount", 0);
            gossip(updateFileMessage, true);
            logger.info("Block " + blockNum + "_" + hydfsFilename + " has been saved to this node");
        }
    }


    /*
     Handle "UpdateFile" message and update fileBlockMap.
     */
    private void handleUpdateFile(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFilename");
        int blockNum = message.getInt("blockNum");
        if (!fileBlockMap.containsKey(hydfsFilename) || blockNum > fileBlockMap.get(hydfsFilename)) {
            fileBlockMap.put(hydfsFilename, blockNum);
            System.out.println("New block of " + hydfsFilename + "created.");
            System.out.println("Current block number = " + blockNum);
        }
        gossip(message, true);
    }


    /*
    Respond to "GetFile" request.
    Send "GetFileResponse" message with the requested block to the requester.
     */
    private void handleGetFile(JSONObject message) {
        Node requester = membership.get(message.getInt("nodeId"));
        String blockName = message.getString("blockName");
        String hydfsFilename = message.getString("hydfsFilename");
        int blockId = message.getInt("blockId");
        JSONObject responseMessage = new JSONObject();
        responseMessage.put("type", "GetFileResponse");
        responseMessage.put("hydfsFilename", hydfsFilename);
        responseMessage.put("blockName", blockName);
        responseMessage.put("blockId", blockId);
        try {
            String blockPath = "HyDFS" + nodeId + "/" + blockName;
            byte[] blockData = Files.readAllBytes(Paths.get(blockPath));
            responseMessage.put("fileData", Base64.getEncoder().encodeToString(blockData));
        } catch (IOException e) {
            logger.warning("Failed to read "+ blockName + " while handling GetFile request");
            return;
        }
        sendTCP(requester.getIpAddress(), requester.getPortTCP(), responseMessage);
        logger.info("Sent file block " + blockName + " to node " + requester.getNodeId());
    }


    /*
    Handle "GetFileResponse" message.
    Store the received block into cache.
     */
    private void handleGetFileResponse(JSONObject message) {
        int blockId = message.getInt("blockId");
        String hydfsFilename = message.getString("hydfsFilename");
        String blockName = message.getString("blockName");
        byte[] blockData = Base64.getDecoder().decode(message.getString("fileData"));
        try {
            Files.write(Paths.get("Cache" + nodeId + "/" + blockName), blockData);
        } catch (IOException e) {
            logger.warning("Failed to write " + blockName + " while handling GetFileResponse");
            return;
        }
        unreceivedBlocks.get(hydfsFilename).remove(blockId);
        logger.info("Received and cached block of file " + blockName);
    }


    /*
    Respond to "GetFromReplica" request.
    Send "GetFromReplicaResponse" message with the requested block to the requester.
     */
    private void handleGetFromReplica(JSONObject message) {
        Node requester = membership.get(message.getInt("nodeId"));
        String blockName = message.getString("blockName");
        String localFilename = message.getString("localFilename");
        JSONObject responseMessage = new JSONObject();
        responseMessage.put("type", "GetFromReplicaResponse");
        responseMessage.put("blockName", blockName);
        responseMessage.put("localFilename", localFilename);
        try {
            String blockPath = "HyDFS" + nodeId + "/" + blockName;
            byte[] blockData = Files.readAllBytes(Paths.get(blockPath));
            responseMessage.put("fileData", Base64.getEncoder().encodeToString(blockData));
        } catch (IOException e) {
            logger.warning("Failed to read " + blockName + " while handling GetFromReplica request");
            return;
        }
        sendTCP(requester.getIpAddress(), requester.getPortTCP(), responseMessage);
        logger.info("Sent file block " + blockName + " to node " + requester.getNodeId());
    }


    /*
    Handle "GetFromReplicaResponse" message.
    Store the received block to the specified local file path.
     */
    public void handleGetFromReplicaResponse(JSONObject message) {
        String blockName = message.getString("blockName");
        byte[] fileData = Base64.getDecoder().decode(message.getString("fileData"));
        String localFilename = message.getString("localFilename");
        Path localFilePath = Paths.get(localFilename);
        try {
            Files.write(localFilePath, fileData);
            logger.info("Received " + blockName + " data and saved to " + localFilename);
        } catch (IOException e) {
            logger.warning("Failed to write local file " + localFilename);
        }
    }


    /*
    Handle "Append" request.
    This node works as a leader to handle all "Append" requests of the HyDFS file,
    whose first block is stored at this node.
    Check and update the request's block ID.
    Send "CreateFile" message to successors to create the new block.
    Disseminate "UpdateFile" message to inform all nodes of the new block.
     */
    private void handleAppendFile(JSONObject message) {
        // Update message block ID if the local block ID is larger
        String hydfsFilename = message.getString("hydfsFilename");
        int currentBlockNum = fileBlockMap.get(hydfsFilename);
        int newBlockNum = currentBlockNum + 1;
        if(newBlockNum > message.getInt("blockId")) {
            message.put("blockId", newBlockNum);
        }
        byte[] data = Base64.getDecoder().decode(message.getString("blockData"));
        fileBlockMap.put(hydfsFilename, newBlockNum);
        // Create the file in current Node's successor and successor2
        JSONObject createFileMessage = new JSONObject();
        createFileMessage.put("type", "CreateFile");
        createFileMessage.put("hydfsFilename", hydfsFilename);
        createFileMessage.put("blockNum", newBlockNum);
        createFileMessage.put("blockData", Base64.getEncoder().encodeToString(data));
        Node receiver = membership.get(ch.getServer(newBlockNum + "_" + hydfsFilename));
        Node successor = membership.get(ch.getSuccessor(receiver.getNodeId()));
        Node successor2 = membership.get(ch.getSuccessor2(receiver.getNodeId()));
        sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), createFileMessage);
        sendTCP(successor.getIpAddress(), successor.getPortTCP(), createFileMessage);
        sendTCP(successor2.getIpAddress(), successor2.getPortTCP(), createFileMessage);
    }


    /*
    Handle "AppendFileFrom" request.
    Initiate a new append request from this node as requested by the multi-append command.
     */
    private void handleAppendMultiFiles(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFilename");
        String localFilename = message.getString("localFilename");
        appendFile(localFilename, hydfsFilename);
    }


    /*
    Handle "MergeFile" request.
    Disseminate "MergeFileFrom" message to request all blocks of the HyDFS file being merged.
    Append all blocks to the first block and replicate to successors.
    Disseminate "MergeFileResponse" message to inform all nodes of the completion of merge.
     */
    private void handleMergeFile(JSONObject message) {
        System.out.println("Received merge request at time: " + clock.millis());
        String hydfsFilename = message.getString("hydfsFilename");
        int requesterNodeId = message.getInt("requesterNodeId");
        int blockNum = fileBlockMap.get(hydfsFilename);
        String firstBlockName = "1_" + hydfsFilename;
        // Add all unreceived blocks to unreceivedBlocks[hydfsFilename]
        unreceivedBlocks.put(hydfsFilename, new HashSet<>());
        for (int i = 1; i <= blockNum; ++i)
            if (!localFiles.contains(i + "_" + hydfsFilename))
                unreceivedBlocks.get(hydfsFilename).add(i);
        // Disseminate "MergeFileFrom" message to request blocks from all nodes
        JSONObject mergeFileMessage = new JSONObject();
        mergeFileMessage.put("type", "MergeFileFrom");
        mergeFileMessage.put("hydfsFilename", hydfsFilename);
        mergeFileMessage.put("gossipCount", 0);
        gossip(mergeFileMessage, true);
        logger.info("Start disseminating \"MergeFileFrom\" requests");
        // Wait until all blocks are received
        while(!unreceivedBlocks.get(hydfsFilename).isEmpty()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Received all blocks of HyDFS file " + hydfsFilename);
        // Append all blocks to the first block of the HyDFS file
        List<String> blockFiles = new ArrayList<>();
        for(int i = 2; i <= blockNum; ++i)
            blockFiles.add("HyDFS" + nodeId + "/" + i + "_" + hydfsFilename);
        try(BufferedWriter writer = new BufferedWriter(
                new FileWriter("HyDFS" + nodeId + "/" + firstBlockName, true)
        )){
            for(String blockFile: blockFiles){
                Path blockFilePath = Paths.get(blockFile);
                writer.write(Files.readString(blockFilePath));
                Files.deleteIfExists(blockFilePath);
            }
        } catch (IOException e) {
            logger.warning("Failed to write to block " + firstBlockName);
        }
        logger.info("Finish writing all blocks to the first block " + firstBlockName);
        // Replicate the merged HyDFS file to successors
        try {
            JSONObject recreateFileMessage = new JSONObject();
            recreateFileMessage.put("type", "RecreateFile");
            recreateFileMessage.put("blockName", firstBlockName);
            byte[] fileContent = Files.readAllBytes(Paths.get("HyDFS" + nodeId + "/" + firstBlockName));
            recreateFileMessage.put("blockData", Base64.getEncoder().encodeToString(fileContent));
            Node successor1 = membership.get(ch.getSuccessor(nodeId));
            Node successor2 = membership.get(ch.getSuccessor2(nodeId));
            sendTCP(successor1.getIpAddress(), successor1.getPortTCP(), recreateFileMessage);
            sendTCP(successor2.getIpAddress(), successor2.getPortTCP(), recreateFileMessage);
        } catch (IOException e) {
            logger.warning("Failed to send \"RecreateFile\" requests to successors");
        }
        // Disseminate MergeFileResponse message
        JSONObject mergeAckMessage = new JSONObject();
        mergeAckMessage.put("type", "MergeFileResponse");
        mergeAckMessage.put("requesterNodeId", requesterNodeId);
        mergeAckMessage.put("hydfsFilename", hydfsFilename);
        mergeAckMessage.put("gossipCount", 0);
        gossip(mergeAckMessage, true);
        System.out.println("Completed merge at : " + clock.millis());
    }


    /*
    Handle "MergeFileFrom" message.
    Send "MergeFileFromResponse" with this node's blocks of the HyDFS file being merged.
     */
    private void handleMergeFileFrom(JSONObject message) {
        gossip(message, true);
        String hydfsFilename = message.getString("hydfsFilename");
        String firstBlockName = "1_" + hydfsFilename;
        Node requester = membership.get(ch.getServer(firstBlockName));
        if (this.nodeId == ch.getServer(firstBlockName)) return;
        for (int i = 2; i <= fileBlockMap.get(hydfsFilename); ++i) {
            if (localFiles.contains(i + "_" + hydfsFilename)) {
                try {
                    byte[] fileContent = Files.readAllBytes(
                            Paths.get("HyDFS" + nodeId + "/" + i + "_" + hydfsFilename)
                    );
                    JSONObject mergeFileMessage = new JSONObject();
                    mergeFileMessage.put("type", "MergeFileFromResponse");
                    mergeFileMessage.put("hydfsFilename", hydfsFilename);
                    mergeFileMessage.put("blockName", i + "_" + hydfsFilename);
                    mergeFileMessage.put("blockData", Base64.getEncoder().encodeToString(fileContent));
                    mergeFileMessage.put("blockId", i);
                    sendTCP(requester.getIpAddress(), requester.getPortTCP(), mergeFileMessage);
                    Thread.sleep(100);
                } catch (IOException e) {
                    logger.warning("Failed to read blocks of i_" + hydfsFilename);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }
        }
    }


    /*
    Handle "MergeFileResponse" message.
    Clean up file blocks and update block number for the merged HyDFS file.
     */
    private void handleMergeFileResponse(JSONObject message) {
        gossip(message, true);
        String hydfsFilename = message.getString("hydfsFilename");
        int requesterNodeId = message.getInt("requesterNodeId");
        if (fileBlockMap.get(hydfsFilename) == 1) return;
        Set<String> blocksToDelete = new HashSet<>();
        for (int i = 2; i <= fileBlockMap.get(hydfsFilename); i++) {
            String blockName = i + "_" + hydfsFilename;
            Path blockPath = Paths.get("HyDFS" + nodeId + "/" + blockName);
            if (Files.exists(blockPath)) blocksToDelete.add(blockName);
        }
        for (String block : blocksToDelete) {
            try {
                Files.deleteIfExists(Paths.get("HyDFS" + nodeId + "/" + block));
                localFiles.remove(block);
            } catch (IOException e) {
                logger.warning("Failed to delete block " + block);
            }
        }
        fileBlockMap.put(hydfsFilename, 1);
        if (this.nodeId == requesterNodeId)
            logger.info("Merge command for " + hydfsFilename + " completed successfully.");
    }


    /*
    Handle "MergeFileFromResponse" message.
    Store the received block and mark the block as received to proceed the merge process.
     */
    public void handleMergeFileFromResponse(JSONObject message) {
        String blockName = message.getString("blockName");
        int blockId = message.getInt("blockId");
        String hyDFSFileName = message.getString("hydfsFilename");
        byte[] blockData = Base64.getDecoder().decode(message.getString("blockData"));
        Path blockFilePath = Paths.get("HyDFS" + nodeId + "/" + blockName);
        if (!Files.exists(blockFilePath)) {
            try {
                Files.write(blockFilePath, blockData);
                unreceivedBlocks.get(hyDFSFileName).remove(blockId);
                logger.info("Stored" + blockName + " for file merge.");
            }catch (IOException e) {
                logger.warning("Failed to store block " + blockName + " for file merge.");
            }
        }
    }
    

    /*
    Handle "ModeUpdate" message.
    Update failure detection node.
     */
    private void handleModeUpdate(JSONObject message) {
        boolean suspicionMode = message.getString("suspicionMode").equals("enabled");
        failureDetectionMode = suspicionMode;
        lastPingTimes.clear();
        if (suspicionMode) System.out.println("Suspicion mode enabled");
        else System.out.println("Suspicion mode disabled");
        gossip(message, true);
    }


    /*
    Handle "RecreateFile" message.
    Re-create a specified file block locally on a node leave/failure.
    */
    private void handleRecreateFile(JSONObject message) {
        try {
            String blockName = message.getString("blockName");
            byte[] blockData = Base64.getDecoder().decode(message.getString("blockData"));
            String filePath = "HyDFS" + nodeId + "/" + blockName;
            Path path = Paths.get(filePath);
            Files.deleteIfExists(path);
            Files.write(path, blockData);
            localFiles.add(blockName);
            logger.info("Recreated block " + blockName + " and saved to local storage at " + filePath);
        } catch (IOException e) {
            logger.warning("Failed to recreate block " + message.getString("blockName") + ": " + e.getMessage());
        }

    }


    /*
    Re-replicate files when a node leaves or fails, called by HandleLeave, HandleFailure
    Check if this node stores any replicas previously stored by the dropped node.
    If it does, send "RecreateFile" request to the new locations to store the replicas.
     */
    private void reReplicationOnDrop(int droppedNodeId){
        // Loop through local files to create "RecreateFile" message for all files to be re-replicated
        List<JSONObject> recreateFileMessages = new ArrayList<>();
        Set<String> localFilesCopy = new HashSet<>(localFiles);
        for (String localFile : localFilesCopy) {
            try {
                Path localFilePath = Paths.get("HyDFS" + nodeId + "/" + localFile);
                int server1Id = ch.getServer(localFile);
                int server2Id = ch.getSuccessor(server1Id);
                int server3Id = ch.getSuccessor2(server1Id);
                if (server1Id == droppedNodeId || server2Id == droppedNodeId || server3Id == droppedNodeId) {
                    byte[] fileContent = Files.readAllBytes(localFilePath);
                    byte[] blockData = Arrays.copyOfRange(fileContent, 0, fileContent.length);
                    JSONObject recreateFileMessage = new JSONObject();
                    recreateFileMessage.put("type", "RecreateFile");
                    recreateFileMessage.put("blockName", localFile);
                    recreateFileMessage.put("blockData", Base64.getEncoder().encodeToString(blockData));
                    recreateFileMessages.add(recreateFileMessage);
                    Files.deleteIfExists(localFilePath);
                    localFiles.remove(localFile);
                }
            } catch (IOException e) {
                System.out.println("Exception occur in handleFailure re-replication: " + e.getMessage());
            }
        }
        ch.removeServer(droppedNodeId);
        // Send "RecreateFile" messages and file data to the new server to store the file
        for(JSONObject recreateFileMessage : recreateFileMessages){
            Node receiver1 = membership.get(
                    ch.getServer(recreateFileMessage.getString("blockName"))
            );
            Node receiver2 = membership.get(ch.getSuccessor(receiver1.getNodeId()));
            Node receiver3 = membership.get(ch.getSuccessor(receiver2.getNodeId()));
            for (Node member : Arrays.asList(receiver1, receiver2, receiver3)) {
                sendTCP(member.getIpAddress(), member.getPortTCP(), recreateFileMessage);
            }
        }
    }


    /*
    Called by appendMultiFiles function to record operation time length.
     */
    private void appendMultiFilesTimeCheck(String hydfsFilename, String nodeIds) {
        String[] nodeIdArray = nodeIds.replaceAll("\\s", "").split(",");
        int targetBlockCount = fileBlockMap.getOrDefault(hydfsFilename, 0) + nodeIdArray.length;
        long appendStartTime = clock.millis();
        System.out.println("Multi-Append started at: " + appendStartTime);
        while (fileBlockMap.getOrDefault(hydfsFilename, 0) < targetBlockCount) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        long appendEndTime = clock.millis();
        System.out.println("Multi-Append completed at: " + appendEndTime);
        System.out.println("Total time taken: " + (appendEndTime - appendStartTime) + " ms");
    }


    /*
    Called by mergeFile function to record operation time length.
     */
    private void mergeTimeCheck(String hydfsFilename) {
        long mergeStartTime = clock.millis();
        System.out.println("Merge started at: " + mergeStartTime);
        while (fileBlockMap.getOrDefault(hydfsFilename, 0) != 1) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        long mergeEndTime = clock.millis();
        System.out.println("Merge completed at: " + mergeEndTime);
        System.out.println("Total time taken: " + (mergeEndTime - mergeStartTime) + " ms");
    }


    /*
    Helper function to recursively delete directories.
     */
    private void deleteDirectoryRecursively(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }


    /*
     Common method used to send a message through TCP to the specified node.
     */
    private void sendTCP(String receiverIp, int receiverPort, JSONObject message){
        try (Socket socket = new Socket(receiverIp, receiverPort)) {
            socket.setSoTimeout(5000);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            writer.write(message.toString());
            writer.newLine();
            writer.flush();
            logger.info("Send " + message.getString("type") + " message to"
                    + receiverIp + ":" + receiverPort);
        } catch (IOException e) {
            logger.warning("Failed to send " + message.getString("type") + " message to "
                    + receiverIp + ":" + receiverPort);
        }
    }


    /*
     Common method used to send a message through UDP to the specified node.
     */
    private void sendUDP(String receiverIp, int receiverPort, JSONObject message) {
        try (DatagramSocket socket = new DatagramSocket()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message.toString());
            byte[] buffer = baos.toByteArray();
            InetAddress address = InetAddress.getByName(receiverIp);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, receiverPort);
            socket.send(packet);
        } catch (IOException e) {
            logger.warning("Failed to send " + message.getString("type") + " message to "
                    + receiverIp + ":" + receiverPort);
        }
    }


    /*
    Common method to initiate gossip dissemination to all nodes potentially alive.
     */
    private void gossip(JSONObject message, boolean isTCP) {
        // Check and add the gossip count
        int gossipCount = message.getInt("gossipCount");
        if(gossipCount > 2) return;
        message.put("gossipCount", gossipCount + 1);
        // Check current available members
        List<Node> availableMembers = new ArrayList<>();
        for (Node member : membership.values()) {
            if (member.getStatus().equals("alive") ||
                    failureDetectionMode && member.getStatus().equals("suspect")) {
                availableMembers.add(member);
            }
        }
        int availableNumber = Math.min(availableMembers.size(), (availableMembers.size() / 3 + 2));
        if (availableNumber < 1) {
            logger.warning("No other member to disseminate message");
            return;
        }
        // Send gossip message to random available numbers
        Collections.shuffle(availableMembers, new Random());
        List<Node> selectedMembers = availableMembers.subList(0, availableNumber);
        for(Node member: selectedMembers) {
            if (isTCP) sendTCP(member.getIpAddress(), member.getPortTCP(), message);
            else sendUDP(member.getIpAddress(), member.getPortUDP(), message);
        }
    }


}
