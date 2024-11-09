package main.java;


import java.io.*;
import java.net.ServerSocket;
import java.net.*;
import org.json.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
    final int nodeId;
    final String ipAddress;
    final int portTCP;
    final int portUDP;
    final Logger logger;
    final Clock clock;

    private ServerSocket tcpServerSocket;
    private ConsistentHashing ch;

    private MembershipManager membershipManager;
    private long predecessorLastPingTime;
    private int predecessorLastPingId;
    private long successorLastPingTime;
    private int successorLastPingId;
    private Map<Integer, Long> lastPingTimes;
    private Map<Integer, Integer> incarnationNumbers;
    private Map<String, Set<Integer>> unreceivedBlocks;
    private Boolean running;
    private Boolean serverMode; // Mode Flag: false = PingAck and true = PingAck+S

    // File list of this server
    private Set<String> localFiles;
    private Map<String, Integer> fileBlockMap;
    private LRUCache lruCache = new LRUCache(3);

    public Server(String[] args) throws IOException, NoSuchAlgorithmException {
        this.logger = Logger.getLogger("Server");
        this.clock = Clock.systemDefaultZone();

        this.nodeId = Integer.parseInt(args[0]);
        this.ipAddress = args[1];
        this.portTCP = Integer.parseInt(args[2]);
        this.portUDP = Integer.parseInt(args[3]);
        this.localFiles = new HashSet<>();
        this.fileBlockMap = new HashMap<>();

        this.running = true;
        this.serverMode = false;

        this.tcpServerSocket = new ServerSocket(portTCP);
        this.ch = new ConsistentHashing();
        this.membershipManager = new MembershipManager();
        membershipManager.addNode(new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        ch.addServer(nodeId);
        this.lastPingTimes = new HashMap<>();
        this.incarnationNumbers = new HashMap<>();
        incarnationNumbers.put(nodeId, 0);
        this.unreceivedBlocks = new HashMap<>();

        logger.setLevel(Level.OFF);

        File directory = new File("HyDFS" + nodeId);
        if(!directory.exists()) {
            boolean created = directory.mkdir();
            if(created) {
                logger.info("HyDFS directory created");
            }else{
                logger.info("Failed to create HyDFS directory");
            }
        }else{
            logger.info("HyDFS directory already exists");
        }
        File cacheDirectory = new File("Cache" + nodeId);
        if(!cacheDirectory.exists()){
            boolean created = cacheDirectory.mkdir();
            if(created) {
                logger.info("Cache directory created");
            }else{
                logger.info("Failed to create cache directory");
            }
        }else{
            logger.info("Cache directory already exists");
        }

    }

    public void tcpListen(){
        logger.info("Starting TCP Listen");
        while(running){
            try{
                Socket tcpSocket = tcpServerSocket.accept();
                tcpSocket.setSoTimeout(5000);
                BufferedReader reader = new BufferedReader(new InputStreamReader(tcpSocket.getInputStream()));
                String jsonString = reader.readLine();
                if (jsonString != null) {
                    JSONObject receivedMessage = new JSONObject(jsonString);
                    System.out.println(receivedMessage);

                    switch(receivedMessage.getString("type")) {
                        case "Join":
                            handleJoinRequest(receivedMessage);
                            break;
                        case "Join-Update":
                            handleJoinUpdate(receivedMessage);
                            break;
                        case "Leave":
                            handleLeave(receivedMessage);
                            break;
                        case "Membership-List":
                            handleMembershipList(receivedMessage);
                            break;
                        case "Failure":
                            handleFailure(receivedMessage);
                            break;
                        case "CreateFile":
                            handleCreateFile(receivedMessage);
                            break;
                        case "RecreateFile":
                            handleRecreateFile(receivedMessage);
                            break;
                        case "GetFile":
                            handleGetFile(receivedMessage);
                            break;
                        case "GetFromReplica":
                            handleGetFromReplica(receivedMessage);
                            break;
                        case "GetFileBlockResponse":
                            handleGetFileBlockResponse(receivedMessage);
                            break;
                        case "GetFromReplicaResponse":
                            handleGetFromReplicaResponse(receivedMessage);
                            break;
                        case "AppendFile":
                            handleAppendFile(receivedMessage);
                            break;
                        case "AppendFileRequest":
                            handleAppendMultiFiles(receivedMessage);
                            break;
                        case "Merge":
                            new Thread(() -> {
                                try {
                                    handleMerge(receivedMessage);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }).start();
                            break;
                        case "MergeFile":
                            handleMergeFile(receivedMessage);
                            break;
                        case "MergeAck":
                            handleMergeAck(receivedMessage);
                            break;
                        case "UpdateFile":
                            handleUpdateFile(receivedMessage);
                            break;
                        case "SuspicionModeUpdate":
                            handleModeUpdate(receivedMessage);
                            break;
                    }
                }

            }catch(IOException e){
                logger.info("Cannot read from TCP packet\n" + e.getMessage());
            }
        }
    }


    public void udpListen(){
        logger.info("Starting UDP Listen");
        try(DatagramSocket socket = new DatagramSocket(portUDP)){
            while(running){
                byte[] buffer = new byte[4096];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                ObjectInputStream ois = new ObjectInputStream(bais);
                // if(!running) break;
                JSONObject receivedMessage = new JSONObject((String) ois.readObject()) ;
                // System.out.println(receivedMessage);
                switch(receivedMessage.getString("type")) {
                    case "Ping":
                        handlePing(receivedMessage);
                        break;
                    case "PingAck":
                        handlePingAck(receivedMessage);
                        break;
                    case "Suspect":
                        handleSuspicion(receivedMessage);
                        break;
                    case "Alive":
                        handleAlive(receivedMessage);
                        break;
                }
            }
        }catch(IOException | ClassNotFoundException e){
            logger.info("Cannot read from UDP packet\n" + e.getMessage());
        }
    }

    public void switchMode(boolean suspicionMode) {
        this.serverMode = suspicionMode;
        if (suspicionMode) {
            System.out.println("Suspicion mode enabled");
        } else {
            System.out.println("Suspicion mode disabled");
        }

        // Gossip to all nodes
        JSONObject suspicionMessage = new JSONObject();
        suspicionMessage.put("type", "SuspicionModeUpdate");
        suspicionMessage.put("suspicionMode", suspicionMode ? "enabled" : "disabled");
        suspicionMessage.put("gossipCount", 0);

        gossip(suspicionMessage, true);
    }

    private void handleModeUpdate(JSONObject message) {
        boolean suspicionMode = message.getString("suspicionMode").equals("enabled");
        this.serverMode = suspicionMode;
        lastPingTimes.clear();
        if (suspicionMode) {
            System.out.println("Suspicion mode enabled");
        } else {
            System.out.println("Suspicion mode disabled");
        }
        gossip(message, true);
    }

    public void statusSus() {
        System.out.println(serverMode ? "Suspicion mode enabled" : "Suspicion mode disabled");
    }

    public void ping(){
        try {
            if (running) {
                JSONObject pingMessage = new JSONObject();
                pingMessage.put("type", "Ping");
                pingMessage.put("nodeId", this.nodeId);
                if (serverMode) {
                    List<Integer> availableMembers = new ArrayList<>();
                    for(int nodeId : membershipManager.getMembers().keySet()){
                        if (membershipManager.getNode(nodeId).getStatus().equals("alive")
                                || membershipManager.getNode(nodeId).getStatus().equals("suspect")) {
                            availableMembers.add(nodeId);
                        }
                    }
                    Node receiver = membershipManager.getNode(availableMembers.get(
                            (int)(Math.random() * availableMembers.size())
                    ));
                    lastPingTimes.put(receiver.getNodeId(), clock.millis());
                    sendUDP(receiver.getIpAddress(), receiver.getPortUDP(), pingMessage);
                }else {
                    for (int nodeId : Arrays.asList(ch.getSuccessor(nodeId), ch.getPredecessor(nodeId))) {
                        try (DatagramSocket socket = new DatagramSocket()) {
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(baos);
                            oos.writeObject(pingMessage.toString());
                            byte[] buffer = baos.toByteArray();
                            InetAddress address = InetAddress.getByName(membershipManager.getNode(nodeId).getIpAddress());
                            int port = membershipManager.getNode(nodeId).getPortUDP();
                            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, port);
                            socket.send(packet);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warning("Exception in ping" + e.getMessage());
        }

    }

    public void checkPing() {
        try {
            if(running) {
                long currentTime = clock.millis();
                if (!serverMode) {
                    if(currentTime - predecessorLastPingTime > 5000) {
                        System.out.println(currentTime + " " + predecessorLastPingTime);
                        JSONObject failureMessage = new JSONObject();
                        failureMessage.put("type", "Failure");
                        failureMessage.put("nodeId", this.predecessorLastPingId);
                        failureMessage.put("gossipCount", 0);
                        gossip(failureMessage, true);
                    }
                    if(currentTime - successorLastPingTime > 5000) {
                        System.out.println(currentTime + " " + successorLastPingTime);
                        JSONObject failureMessage = new JSONObject();
                        failureMessage.put("type", "Failure");
                        failureMessage.put("nodeId", this.successorLastPingId);
                        failureMessage.put("gossipCount", 0);
                        gossip(failureMessage, true);
                    }
                } else {
                    currentTime = clock.millis();
                    for (Map.Entry<Integer, Long> entry : lastPingTimes.entrySet()) {
                        int nodeId = entry.getKey();
                        Node member = membershipManager.getNode(nodeId);
                        System.out.println(member == null ? "member is null" : "member id: " + member.getNodeId());
                        long lastPingTime = entry.getValue();

                        System.out.println("Node ID: " + nodeId + "Last Ping Time: " + lastPingTime);

                        if (currentTime - lastPingTime > 3000 && member.getStatus().equals("alive")) {
                            JSONObject suspicionMessage = new JSONObject();
                            suspicionMessage.put("type", "Suspect");
                            suspicionMessage.put("nodeId", nodeId);
                            suspicionMessage.put("gossipCount", 0);
                            suspicionMessage.put("incarnation", incarnationNumbers.get(nodeId));
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
                }
            }
        } catch (Exception e) {
            logger.warning("Exception in checkPing" + e.getMessage());
        }
    }

    public void list_mem() {
        System.out.println("Current Membership List");
        for(int nodeId: membershipManager.getMembers().keySet()){
            System.out.println("Node ID = " + nodeId
                    + "; IP Address = " + membershipManager.getNode(nodeId).getIpAddress()
                    + "; Node Status = " + membershipManager.getNode(nodeId).getStatus());
        }
    }

    public void list_self() {
        System.out.println("NodeId = " + this.nodeId);
    }

    public void list_file_store(String filename) {
        System.out.println("File " + filename + " has " + fileBlockMap.get(filename) + " blocks.");
        for(int i = 1; i <= fileBlockMap.get(filename); ++i) {
            list_block_store(i + "_" + filename);
        }
    }

    public void list_block_store(String blockName) {
        System.out.println("The block " + blockName + " is stored at following nodes:");
        Node server1 = membershipManager.getNode(ch.getServer(blockName));
        Node server2 = membershipManager.getNode(ch.getSuccessor(server1.getNodeId()));
        Node server3 = membershipManager.getNode(ch.getSuccessor2(server1.getNodeId()));
        for(Node server: Arrays.asList(server1, server2, server3)){
            System.out.println("Node ID = " + server.getNodeId() + "; IP Address = " + server.getIpAddress()
                    + "; Node Status = " + server.getStatus() + "; Ring ID = " + ch.getRingId(server.getNodeId()));
        }
    }

    // 打出自己存的所有文件
    public void list_self_store() {
        System.out.println("Node Id = " + this.nodeId + "; Ring ID = " + ch.getRingId(this.nodeId));
        if (localFiles.isEmpty()) {
            System.out.println("No files currently stored on server " + nodeId);
        } else {
            System.out.println("Files currently stored on server " + nodeId + ":");
            for (String localFile: localFiles) {
                System.out.println(localFile);
            }
        }
    }

    public void list_mem_id() {
        System.out.println("Current Membership List and Ring IDs:");
        for(int nodeId: membershipManager.getMembers().keySet()){
            System.out.println("Node ID = " + nodeId
                    + "; IP Address = " + membershipManager.getNode(nodeId).getIpAddress()
                    + "; Node Status = " + membershipManager.getNode(nodeId).getStatus()
                    + "; Ring ID = " + ch.getRingId(nodeId));
        }
    }

    // Method to join the system by contacting other nodes
    public void join(String introIpAddress, int introPort) throws IOException, InterruptedException {
        running = true;
        membershipManager.clear();
        membershipManager.addNode(new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        JSONObject joinMessage = new JSONObject();
        joinMessage.put("type", "Join");
        joinMessage.put("nodeId", nodeId);
        joinMessage.put("ipAddress", ipAddress);
        joinMessage.put("portUDP", portUDP);
        joinMessage.put("portTCP", portTCP);

        sendTCP(introIpAddress, introPort, joinMessage);

        logger.info("Sent Join-Req Message");

        Thread.sleep(5000);
        if(membershipManager.getSize() > 1){
            logger.info("Join Succeeds");
        }else{
            logger.info("Join Failed for " + introIpAddress + ":" + introPort);
        }
    }

    public void leave() {
        membershipManager.getNode(nodeId).setStatus("leave");
        running = false;
        ch.removeServer(nodeId);
        JSONObject leaveMessage = new JSONObject();
        leaveMessage.put("type", "Leave");
        leaveMessage.put("nodeId", nodeId);
        leaveMessage.put("gossipCount", 0);
        gossip(leaveMessage, true);
    }

//    JSONObject createFileMessage = new JSONObject();
//        createFileMessage.put("type", "CreateFile");
//        createFileMessage.put("blockName", blockName);

//        byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
    //   byte[] blockData = Arrays.copyOfRange(fileContent, start, end);

//        createFileMessage.put("blockData", Base64.getEncoder().encodeToString(blockData));
//
//    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
//        writer.write(createFileMessage.toString());
//        writer.newLine();
//        writer.flush();
    public void createFile(String localFilename, String hydfsFilename) throws IOException {
        if(fileBlockMap.containsKey(hydfsFilename)){
            System.out.println("Filename already exists in HyDFS");
            return;
        }
        fileBlockMap.put(hydfsFilename, 1);
        int receiverId = ch.getServer( "1_" + hydfsFilename);

        // Add to file set
        localFiles.add("1_" + hydfsFilename);

        byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
        byte[] blockData = Arrays.copyOfRange(fileContent, 0, fileContent.length);

        for (int memberId : Arrays.asList(receiverId, ch.getSuccessor(receiverId), ch.getSuccessor2(receiverId))) {
            Node member = membershipManager.getNode(memberId);
            JSONObject createFileMessage = new JSONObject();
            createFileMessage.put("type", "CreateFile");
            createFileMessage.put("hydfsFilename", hydfsFilename);
            createFileMessage.put("blockNum", 1);
            createFileMessage.put("blockData", Base64.getEncoder().encodeToString(blockData));
            sendTCP(member.getIpAddress(), member.getPortTCP(), createFileMessage);
        }
        JSONObject updateFileMessage = new JSONObject();
        updateFileMessage.put("type", "UpdateFile");
        updateFileMessage.put("hydfsFilename", hydfsFilename);
        updateFileMessage.put("blockNum", 1);
        updateFileMessage.put("gossipCount", 0);
        gossip(updateFileMessage, true);

    }

    public void getFile(String hydfsFilename, String localFilename) {
        try {
            if(!fileBlockMap.containsKey(hydfsFilename)) {
                logger.warning("The file to be gotten does not exist in HyDFS");
                return;
            }else if(fileBlockMap.get(hydfsFilename) != lruCache.get(hydfsFilename)){
                int blockNum = fileBlockMap.get(hydfsFilename);
                unreceivedBlocks.put(hydfsFilename, new HashSet<>());
                for(int i = 1; i <= blockNum; ++i){
                    unreceivedBlocks.get(hydfsFilename).add(i);
                }
                int secondsPassed = 0;
                while(!unreceivedBlocks.get(hydfsFilename).isEmpty()){
                    if(secondsPassed % 3 == 0) {
                        for(int i: unreceivedBlocks.get(hydfsFilename)) {
                            Node receiver = membershipManager.getNode(ch.getServer(i + "_" + hydfsFilename));
                            if(secondsPassed % 9 == 3) {
                                receiver = membershipManager.getNode(ch.getSuccessor(receiver.getNodeId()));
                            } else if(secondsPassed % 9 == 6) {
                                receiver = membershipManager.getNode(ch.getSuccessor2(receiver.getNodeId()));
                            }
                            JSONObject getFileMessage = new JSONObject();
                            getFileMessage.put("type", "GetFile");
                            getFileMessage.put("hydfsFilename", hydfsFilename);
                            getFileMessage.put("blockId", i);
                            getFileMessage.put("blockFileName", i + "_" + hydfsFilename);
                            getFileMessage.put("nodeId", nodeId);
                            sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), getFileMessage);
                        }
                    }
                    secondsPassed += 1;
                    Thread.sleep(1000);
                }
                List<String> blockFiles = new ArrayList<>();
                for(int i = 1; i <= blockNum; ++i){
                    blockFiles.add("Cache" + nodeId + "/" + i + "_" + hydfsFilename);
                }
                Files.deleteIfExists(Paths.get("Cache" + nodeId + "/" + hydfsFilename));
                try (BufferedWriter writer = new BufferedWriter(new FileWriter("Cache" + nodeId + "/" + hydfsFilename))) {
                    for(String blockFile: blockFiles){
                        Path blockFilePath = Paths.get(blockFile);
                        writer.write(Files.readString(blockFilePath));
                        Files.deleteIfExists(blockFilePath);
                    }
                }
                String deletedFilename = lruCache.put(hydfsFilename, fileBlockMap.get(hydfsFilename));
                if(deletedFilename != null){
                    Files.deleteIfExists(Paths.get("Cache" + nodeId + "/" + deletedFilename));
                }
            }
            Files.copy(Paths.get("Cache" + nodeId + "/" + hydfsFilename),
                    Paths.get(localFilename), StandardCopyOption.REPLACE_EXISTING);
            logger.info("Succeed to get " + localFilename + " from HyDFS file " + hydfsFilename);

        }catch(IOException | InterruptedException e){
            logger.warning("An error occured while getting file " + hydfsFilename + e.getMessage());
        }
    }

    public void appendFile(String localFilename, String hydfsFilename) {
        int blockId = fileBlockMap.get(hydfsFilename) + 1;
        Node receiver = membershipManager.getNode(ch.getServer(blockId + "_" + hydfsFilename));
        try {
            byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
            JSONObject appendFileMessage = new JSONObject();
            appendFileMessage.put("type", "AppendFile");
            appendFileMessage.put("hydfsFilename", hydfsFilename);
            appendFileMessage.put("blockId", blockId);
            appendFileMessage.put("blockData", Base64.getEncoder().encodeToString(fileContent));
            sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), appendFileMessage);
        } catch (IOException e) {
            System.out.println("Failed to read from local file " + localFilename + e.getMessage());
        }

    }

    public void appendMultiFiles(String hydfsFilename, String nodeIds, String localFilenames) throws IOException {
        String[] nodeIdArray = nodeIds.replaceAll("\\s", "").split(",");
        String[] localFilenameArray = localFilenames.replaceAll("\\s", "").split(",");
        if (nodeIdArray.length != localFilenameArray.length) {
            System.out.println("Please specify equal number of node IDs and local filepath");
            return;
        }
        for (int i = 0; i < nodeIdArray.length; ++i) {
            JSONObject appendFileRequestMessage = new JSONObject();
            appendFileRequestMessage.put("type", "AppendFileRequest");
            appendFileRequestMessage.put("hydfsFilename", hydfsFilename);
            appendFileRequestMessage.put("localFilename", localFilenameArray[i]);
            Node receiver;
            try {
                receiver = membershipManager.getNode(Integer.parseInt(nodeIdArray[i]));
            } catch (NumberFormatException e) {
                System.out.println("Please specify node IDs as integers");
                return;
            }
            sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), appendFileRequestMessage);
        }
    }

    public void mergeFile(String hyDFSfilename) {
        JSONObject mergeRequest = new JSONObject();
        mergeRequest.put("type", "Merge");
        mergeRequest.put("hydfsFilename", hyDFSfilename);
        mergeRequest.put("requesterNodeId", this.nodeId);
        mergeRequest.put("gossipCount", 0);

        gossip(mergeRequest, true);
        logger.info("Broadcasted merge request for " + hyDFSfilename + " to all nodes.");
    }

    // vmAddress 应该是 ipaddr:port
    public void getFromReplica(String vmAddress, String hydfsFilename, String localFilename) {
        try {
            String[] addressParts = vmAddress.split(":");
            String ipAddress = addressParts[0];
            int port = Integer.parseInt(addressParts[1]);

            JSONObject getFileRequest = new JSONObject();
            getFileRequest.put("type", "GetFromReplica");
            getFileRequest.put("blockName", "1_" + hydfsFilename);
            getFileRequest.put("requesterNodeId", this.nodeId);
            getFileRequest.put("requesterIp", this.ipAddress);
            getFileRequest.put("requesterPort", this.portTCP);
            getFileRequest.put("localFilename", localFilename);

            sendTCP(ipAddress, port,getFileRequest);
            logger.info("Sent GetFromReplica request to replica at " + ipAddress + ":" + port);
        } catch (Exception e) {
            logger.warning("Error in getFromReplica: " + e.getMessage());
        }
    }

    public void handleGetFromReplica(JSONObject message) {
        try {
            String blockName = message.getString("blockName");
            String blockPath = "HyDFS" + nodeId + "/" + blockName;

            Path path = Paths.get(blockPath);
            if (!Files.exists(path)) {
                logger.warning("File " + blockName + " not found on replica.");
                return;
            }

            byte[] fileData = Files.readAllBytes(path);

            // Response message
            String localFilename = message.getString("localFilename");
            JSONObject responseMessage = new JSONObject();
            responseMessage.put("type", "GetFromReplicaResponse");
            responseMessage.put("blockName", blockName);
            responseMessage.put("fileData", Base64.getEncoder().encodeToString(fileData));
            responseMessage.put("localFilename", localFilename);

            String requesterIp = message.getString("requesterIp");
            int requesterPort = message.getInt("requesterPort");
            sendTCP(requesterIp, requesterPort, responseMessage);

            logger.info("Handled GetFromReplica request for " + blockName + " and sent data to " + requesterIp + ":" + requesterPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void handleGetFromReplicaResponse(JSONObject message) {
        try {
            String blockName = message.getString("blockName");
            byte[] fileData = Base64.getDecoder().decode(message.getString("fileData"));
            String localFilename = message.getString("localFilename");

            Path localFilePath = Paths.get(localFilename);

            Files.write(localFilePath, fileData);
            logger.info("Received " + blockName + " data and saved to " + localFilename);

        } catch (IOException e) {
            logger.warning("Error in handleGetFromReplicaResponse: " + e.getMessage());
        }
    }


    public void handleGetFile(JSONObject message) {
        try {
            String blockFileName = message.getString("blockFileName");
            int requesterNodeId = message.getInt("nodeId");
            String hydfsFilename = message.getString("hydfsFilename");
            int blockId = message.getInt("blockId");

            // Get node from membershipManager
            Node reqNode = membershipManager.getNode(requesterNodeId);
            if (reqNode == null) {
                logger.warning(requesterNodeId + ": Node not found in membership list of " + this.nodeId);
                return;
            }
            // Get Ip and TCP port
            String reqIpAddress = reqNode.getIpAddress();
            int reqPortTCP = reqNode.getPortTCP();

            // Locate and read the target block data
            String blockPath = "HyDFS" + nodeId + "/" + blockFileName;
            byte[] blockData = Files.readAllBytes(Paths.get(blockPath));

            // Send back response
            JSONObject responseMsg = new JSONObject();
            responseMsg.put("type", "GetFileBlockResponse");
            responseMsg.put("hydfsFilename", hydfsFilename);
            responseMsg.put("blockFileName", blockFileName);
            responseMsg.put("blockId", blockId);
            responseMsg.put("fileData", Base64.getEncoder().encodeToString(blockData));

            // Send through TCP
            sendTCP(reqIpAddress, reqPortTCP, responseMsg);
            logger.info("Sent block of file " + blockFileName + " to requester node " + requesterNodeId);

        } catch (IOException e) {
            logger.warning("Error while handling GetFileBlock request: " + e.getMessage());
        }
    }

    private void handleGetFileBlockResponse(JSONObject message) {
        try {
            // Retrieve the data and filename
            String blockFileName = message.getString("blockFileName");
            byte[] blockData = Base64.getDecoder().decode(message.getString("fileData"));

            // Save the block to a local path
            String blockPath = "Cache" + nodeId + "/" + blockFileName;
            Files.write(Paths.get(blockPath), blockData);

            String hydfsFilename = message.getString("hydfsFilename");
            int blockId = message.getInt("blockId");
            unreceivedBlocks.get(hydfsFilename).remove(blockId);

            logger.info("Received and cached block of file " + blockFileName + " to node " + this.nodeId);

        } catch (IOException e) {
            logger.warning("Error while handling GetFileBlock Response: " + e.getMessage());
        }
    }


    private void handleAppendFile(JSONObject message) {
        try {
            String hydfsFilename = message.getString("hydfsFilename");
            int currentBlockNum = fileBlockMap.get(hydfsFilename);
            int newBlockNum = currentBlockNum + 1;
            if(newBlockNum > message.getInt("blockId")) {
                Node receiver = membershipManager.getNode(ch.getServer(newBlockNum + "_" + hydfsFilename));
                message.put("blockId", newBlockNum);
                sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), message);
            }else{
                byte[] data = Base64.getDecoder().decode(message.getString("blockData"));
                // Construct the file path for new block
                String filePath = "HyDFS" + nodeId + "/" + newBlockNum + "_" + hydfsFilename;

                Files.write(Paths.get(filePath), data);

                fileBlockMap.put(hydfsFilename, newBlockNum);
                logger.info("Appended block " + newBlockNum + " to file " + hydfsFilename + " and saved to " + filePath);

                // Add to localFiles set
                localFiles.add(newBlockNum + "_" + hydfsFilename);

                // Create the file in current Node's successor and successor2
                JSONObject createFileMessage = new JSONObject();
                createFileMessage.put("type", "CreateFile");
                createFileMessage.put("hydfsFilename", hydfsFilename);
                createFileMessage.put("blockNum", newBlockNum);
                createFileMessage.put("blockData", Base64.getEncoder().encodeToString(data));
                Node successor = membershipManager.getNode(ch.getSuccessor(nodeId));
                Node successor2 = membershipManager.getNode(ch.getSuccessor2(nodeId));
                sendTCP(successor.getIpAddress(), successor.getPortTCP(), createFileMessage);
                sendTCP(successor2.getIpAddress(), successor2.getPortTCP(), createFileMessage);

                // Notify other member with new blockCount
                JSONObject updateFileMessage = new JSONObject();
                updateFileMessage.put("type", "UpdateFile");
                updateFileMessage.put("hydfsFilename", hydfsFilename);
                updateFileMessage.put("blockNum", newBlockNum);
                updateFileMessage.put("gossipCount", 0);
                gossip(updateFileMessage, true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void handleAppendMultiFiles(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFilename");
        String localFilename = message.getString("localFilename");
        appendFile(localFilename, hydfsFilename);
    }

    private void handleCreateFile(JSONObject message) {
        try {
            String hydfsFilename = message.getString("hydfsFilename");
            int blockNum = message.getInt("blockNum");
            byte[] data = Base64.getDecoder().decode(message.getString("blockData"));

            // save the block to local HyDfs
            String filePath = "HyDFS" + nodeId + "/" + blockNum + "_" + hydfsFilename ;
            Files.write(Paths.get(filePath), data);

            // Add to the localFiles set
            localFiles.add(blockNum + "_" + hydfsFilename);

            fileBlockMap.put(hydfsFilename, blockNum); // Initially, each file starts with one block
            logger.info("Block " + blockNum + " of file " + hydfsFilename + " saved to " + filePath);
        } catch (IOException e) {
            logger.warning("Failed to save block to local HyDFS directory.");
        }
    }

    private void handleUpdateFile(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFilename");
        int blockNum = message.getInt("blockNum");
        fileBlockMap.put(hydfsFilename, blockNum);
        gossip(message, true);
    }


    private void handlePing(JSONObject message) {
        if (serverMode){
            JSONObject pingAckMessage = new JSONObject();
            pingAckMessage.put("type", "PingAck");
            pingAckMessage.put("nodeId", nodeId);
            pingAckMessage.put("incarnation", incarnationNumbers.get(nodeId));
            Node receiver = membershipManager.getNode(message.getInt("nodeId"));
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


    private void handlePingAck(JSONObject message) {
        try {
            int senderNodeId = message.getInt("nodeId");
            int receivedIncarnation = message.getInt("incarnation");
            long currentTime = clock.millis();
            lastPingTimes.remove(senderNodeId);
            System.out.println("Current lastPingTimes after ack removal node#: " + senderNodeId + " "+ lastPingTimes);
            logger.info("Received PingAck from node: " + senderNodeId);

            if (serverMode) {
                Node senderNode = membershipManager.getNode(senderNodeId);
                if (senderNode != null && senderNode.getStatus().equals("suspect")) {
                    // Update incarnation number
                    int currentIncarnation = incarnationNumbers.getOrDefault(senderNodeId, 0);
                    if (receivedIncarnation > currentIncarnation) {
                        incarnationNumbers.put(senderNodeId, receivedIncarnation);
                        senderNode.setStatus("alive");
                        logger.info("Node " + senderNodeId + "status set to alive after received ping from it");

                        // Gossip the status change
                        JSONObject statusUpdateMessage = new JSONObject();
                        statusUpdateMessage.put("type", "Alive");
                        statusUpdateMessage.put("nodeId", senderNodeId);
                        statusUpdateMessage.put("incarnation", receivedIncarnation);

                        gossip(statusUpdateMessage, false);
                    }
                }
            }
        } catch (JSONException e) {
            logger.warning("Error parsing PingAck message: " + e.getMessage());
        }
    }

    // 处理被误判的node
    private void handleAlive(JSONObject message) {
        try {
            int aliveNodeId = message.getInt("nodeId");
            int receivedIncarnation = message.getInt("incarnation");

            int currentIncarnation = incarnationNumbers.getOrDefault(aliveNodeId, 0);
            if (receivedIncarnation > currentIncarnation) {
                membershipManager.getNode(aliveNodeId).setStatus("alive");
                incarnationNumbers.put(aliveNodeId, receivedIncarnation);
                lastPingTimes.remove(aliveNodeId);
                logger.info("Node " + aliveNodeId + "status set to alive");
                gossip(message, false);
            }
        } catch (JSONException e) {
            logger.warning("Error parsing StatusUpdate message: " + e.getMessage());
        }
    }

    private void handleSuspicion(JSONObject message) {
        int suspectNodeId = message.getInt("nodeId");
        int incarnationNumber = message.getInt("incarnation");
        if (suspectNodeId == nodeId) {
            if (incarnationNumber > incarnationNumbers.get(nodeId)) {
                incarnationNumbers.put(nodeId, incarnationNumber + 1);
                JSONObject aliveMessage = new JSONObject();
                aliveMessage.put("type", "Alive");
                aliveMessage.put("nodeId", nodeId);
                aliveMessage.put("incarnation", incarnationNumbers.get(nodeId));
                gossip(aliveMessage, false);
            }
        } else {
            membershipManager.getNode(suspectNodeId).setStatus("suspect");
            if (incarnationNumber > incarnationNumbers.get(suspectNodeId)){
                incarnationNumbers.put(suspectNodeId, incarnationNumber);
            }
            gossip(message, false);
        }
    }



    // Introducer handle join request
    private void handleJoinRequest(JSONObject message) {

        int joiningNodeId = message.getInt("nodeId");
        String joiningNodeIp = message.getString("ipAddress");
        int joiningNodePortUDP = message.getInt("portUDP");
        int joiningNodePortTCP = message.getInt("portTCP");
        Node joiningNode = new Node(joiningNodeId, joiningNodeIp, joiningNodePortUDP, joiningNodePortTCP);

        membershipManager.addNode(joiningNode);
        ch.addServer(joiningNodeId);
        if (!incarnationNumbers.containsKey(joiningNodeId)) {
            incarnationNumbers.put(joiningNodeId, 0);
        }
        logger.info("Node " + joiningNode.getNodeId() + " joined successfully");

        // 创建Join-Update消息
        JSONObject joinUpdateMessage = new JSONObject();
        joinUpdateMessage.put("type", "Join-Update");
        joinUpdateMessage.put("nodeId", joiningNodeId);
        joinUpdateMessage.put("ipAddress", joiningNodeIp);
        joinUpdateMessage.put("portUDP", joiningNodePortUDP);
        joinUpdateMessage.put("portTCP", joiningNodePortTCP);
        joinUpdateMessage.put("status", "alive");

        // Multicast to all members
        for (Node member : membershipManager.getMembers().values()) {
            if (member.getNodeId() != nodeId) {
                sendTCP(member.getIpAddress(), member.getPortTCP(), joinUpdateMessage);
            }
        }

        // Send back membership list to new joined node from introducer
        JSONObject memberUpdateMessage = new JSONObject();
        memberUpdateMessage.put("type", "Membership-List");

        JSONArray membersArray = new JSONArray();
        for (Node member : membershipManager.getMembers().values()) {
            JSONObject memberInfo = new JSONObject();
            memberInfo.put("nodeId", member.getNodeId());
            memberInfo.put("ipAddress", member.getIpAddress());
            memberInfo.put("portUDP", member.getPortUDP());
            memberInfo.put("portTCP", member.getPortTCP());
            memberInfo.put("status", member.getStatus());
            membersArray.put(memberInfo);
        }

        memberUpdateMessage.put("members", membersArray);
        sendTCP(joiningNodeIp, joiningNodePortTCP, memberUpdateMessage);
        logger.info("Send membership update message to new joined node" + joiningNodeId);
    }


    // 非introducer收到广播消息的join update
    private void handleJoinUpdate(JSONObject message) {
        int joiningNodeId = message.getInt("nodeId");
        String joiningNodeIp = message.getString("ipAddress");
        int joiningNodePortUDP = message.getInt("portUDP");
        int joiningNodePortTCP = message.getInt("portTCP");
        Node joiningNode = new Node(joiningNodeId, joiningNodeIp, joiningNodePortUDP, joiningNodePortTCP);

        membershipManager.addNode(joiningNode);
        ch.addServer(joiningNodeId);
        if (!incarnationNumbers.containsKey(joiningNodeId)) {
            incarnationNumbers.put(joiningNodeId, 0);
        }
        logger.info("Node " + joiningNode.getNodeId() + " joined successfully");
    }


    // MemberList 里面的node收到leave message 之后来handleLeave
    private void handleLeave(JSONObject message) throws IOException {
        int leftNodeId = message.getInt("nodeId");
        Node leavingNode = membershipManager.getNode(leftNodeId);

        if(ch.getRingId(leftNodeId) != -1) {
            // Loop through local files to create message for all files to be re-replicated
            List<JSONObject> recreateFileMessages = new ArrayList<>();
            Set<String> localFilesCopy = new HashSet<>(localFiles);
            for (String localFile : localFilesCopy) {
                Path localFilePath = Paths.get("HyDFS" + nodeId + "/" + localFile);
                System.out.println(localFilePath);
                int server1Id = ch.getServer(localFile);
                int server2Id = ch.getSuccessor(server1Id);
                int server3Id = ch.getSuccessor2(server1Id);
                if (server1Id == leftNodeId || server2Id == leftNodeId || server3Id == leftNodeId) {
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
            }
            // Check if the node exist
            if (leavingNode != null) {
                // Set leavingNode status to 'leave'
                leavingNode.setStatus("leave");
                logger.info("Node" + leftNodeId + " left successfully, set status to \"leave\"");
                // Update consistent hashing ring
                ch.removeServer(leftNodeId);
                gossip(message, true);
            } else {
                logger.warning("Node" + leftNodeId + " not found");
                return;
            }
            // Send re-create messages and file data to the new server to store the file
            for(JSONObject recreateFileMessage : recreateFileMessages){
                Node receiver1 = membershipManager.getNode(
                        ch.getServer(recreateFileMessage.getString("blockName"))
                );
                Node receiver2 = membershipManager.getNode(ch.getSuccessor(receiver1.getNodeId()));
                Node receiver3 = membershipManager.getNode(ch.getSuccessor(receiver2.getNodeId()));
                for (Node member : Arrays.asList(receiver1, receiver2, receiver3)) {
                    sendTCP(member.getIpAddress(), member.getPortTCP(), recreateFileMessage);
                }
            }
        }
    }


    private void handleFailure(JSONObject message) throws IOException {
        int failedNodeId = message.getInt("nodeId");

        if(ch.getRingId(failedNodeId) != -1){
            // Loop through local files to create message for all files to be re-replicated
            List<JSONObject> recreateFileMessages = new ArrayList<>();
            Set<String> localFilesCopy = new HashSet<>(localFiles);
            for (String localFile : localFilesCopy) {
                Path localFilePath = Paths.get("HyDFS" + nodeId + "/" + localFile);
                System.out.println(localFilePath.toString());
                int server1Id = ch.getServer(localFile);
                int server2Id = ch.getSuccessor(server1Id);
                int server3Id = ch.getSuccessor2(server1Id);
                if (server1Id == failedNodeId || server2Id == failedNodeId || server3Id == failedNodeId) {
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
            }
            // 在consistent hashing ring 和 membership manager当中删除掉这个节点
            ch.removeServer(failedNodeId);
            membershipManager.removeNode(failedNodeId);
            incarnationNumbers.remove(failedNodeId);
            lastPingTimes.remove(failedNodeId);
            logger.info("Node " + failedNodeId + " marked as failed node and removed from membership list and ring");
            // 如果被判断的是当前节点直接改running为 false
            if (this.nodeId == failedNodeId) {
                this.running = false;
            }
            gossip(message, true);
            // Send re-create messages and file data to the new server to store the file
            for(JSONObject recreateFileMessage : recreateFileMessages){
                Node receiver1 = membershipManager.getNode(
                        ch.getServer(recreateFileMessage.getString("blockName"))
                );
                Node receiver2 = membershipManager.getNode(ch.getSuccessor(receiver1.getNodeId()));
                Node receiver3 = membershipManager.getNode(ch.getSuccessor(receiver2.getNodeId()));
                for (Node member : Arrays.asList(receiver1, receiver2, receiver3)) {
                    sendTCP(member.getIpAddress(), member.getPortTCP(), recreateFileMessage);
                }
            }
        }
    }

    public void handleRecreateFile(JSONObject message) {
        try {
            String blockName = message.getString("blockName");
            byte[] blockData = Base64.getDecoder().decode(message.getString("blockData"));
            String filePath = "HyDFS" + nodeId + "/" + blockName;
            Path path = Paths.get(filePath);
            Files.deleteIfExists(path);
            Files.write(path, blockData);
            // Add to localFiles
            localFiles.add(blockName);
            logger.info("Recreated block " + blockName + " and saved to local storage at " + filePath);
        } catch (IOException e) {
            logger.warning("Failed to recreate block " + message.getString("blockName") + ": " + e.getMessage());
        }

    }

    private void handleMembershipList(JSONObject message) {
        logger.info("Node " + this.nodeId + " received Membership List");

        JSONArray membershipArray = message.getJSONArray("members");

        membershipManager.clear();

        for (int i = 0; i < membershipArray.length(); i++) {
            JSONObject memberInfo = membershipArray.getJSONObject(i);
            int memberId = memberInfo.getInt("nodeId");
            String memberIp = memberInfo.getString("ipAddress");
            int memberPortUDP = memberInfo.getInt("portUDP");
            int memberPortTCP = memberInfo.getInt("portTCP");
            String memberStatus = memberInfo.getString("status");

            // Create a new node object
            Node memberNode = new Node(memberId, memberIp, memberPortUDP, memberPortTCP, memberStatus);
            membershipManager.addNode(memberNode);
            ch.addServer(memberId);
            if (!incarnationNumbers.containsKey(memberId)) {
                incarnationNumbers.put(memberId, 0);
            }
        }

        logger.info("Update membership List completed from introducer");
    }


    private void handleMerge(JSONObject message) throws InterruptedException {
        String hydfsFilename = message.getString("hydfsFilename");
        String firstBlockName = "1_" + hydfsFilename;
        int requesterNodeId = message.getInt("requesterNodeId");
        int blockNum = fileBlockMap.get(hydfsFilename);
        // If the current node is the one storing block 1 for hydfsFilename
        if (ch.getServer(firstBlockName) == nodeId) {
            // Add all unreceived blocks to unreceivedBlocks[hydfsFilename]
            unreceivedBlocks.clear();
            unreceivedBlocks.put(hydfsFilename, new HashSet<>());
            for (int i = 1; i <= blockNum; ++i) {
                if (!localFiles.contains(i + "_" + hydfsFilename)) {
                    unreceivedBlocks.get(hydfsFilename).add(i);
                }
            }
            // Wait until all blocks are received
            while(!unreceivedBlocks.get(hydfsFilename).isEmpty()){
                Thread.sleep(1000);
            }
            // Append all blocks to 1_hydfsFilename and send RecreateFile requests to successors
            List<String> blockFiles = new ArrayList<>();
            for(int i = 2; i <= blockNum; ++i){
                blockFiles.add("HyDFS" + nodeId + "/" + i + "_" + hydfsFilename);
            }
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("HyDFS" + nodeId + "/" + firstBlockName, true))) {
                for(String blockFile: blockFiles){
                    Path blockFilePath = Paths.get(blockFile);
                    for(String line : Files.readAllLines(blockFilePath)){
                        System.out.println(line);
                        writer.write(line);
                        writer.newLine();
                    }
                    Files.deleteIfExists(blockFilePath);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                byte[] fileContent = Files.readAllBytes(Paths.get("HyDFS" + nodeId + "/" + firstBlockName));
                JSONObject recreateFileMessage = new JSONObject();
                recreateFileMessage.put("type", "RecreateFile");
                recreateFileMessage.put("blockName", firstBlockName);
                recreateFileMessage.put("blockData", Base64.getEncoder().encodeToString(fileContent));
                Node successor1 = membershipManager.getNode(ch.getSuccessor(nodeId));
                Node successor2 = membershipManager.getNode(ch.getSuccessor2(nodeId));
                sendTCP(successor1.getIpAddress(), successor1.getPortTCP(), recreateFileMessage);
                sendTCP(successor2.getIpAddress(), successor2.getPortTCP(), recreateFileMessage);
            }catch (IOException e) {
                throw new RuntimeException();
            }
            // Gossip MergeAck messages
            JSONObject mergeAckMessage = new JSONObject();
            mergeAckMessage.put("type", "MergeAck");
            mergeAckMessage.put("requesterNodeId", requesterNodeId);
            mergeAckMessage.put("hydfsFilename", hydfsFilename);
            mergeAckMessage.put("gossipCount", 0);
            gossip(mergeAckMessage, true);
        // If the current not does not store block 1 for hydfsFilename
        }else{
            try{
                for (int i = 2; i <= blockNum; ++i) {
                    if (localFiles.contains(i + "_" + hydfsFilename)) {
                        byte[] fileContent = Files.readAllBytes(Paths.get("HyDFS" + nodeId + "/" + i + "_" + hydfsFilename));
                        byte[] blockData = Arrays.copyOfRange(fileContent, 0, fileContent.length);
                        JSONObject mergeFileMessage = new JSONObject();
                        mergeFileMessage.put("type", "MergeFile");
                        mergeFileMessage.put("hydfsFilename", hydfsFilename);
                        mergeFileMessage.put("blockName", i + "_" + hydfsFilename);
                        mergeFileMessage.put("blockData", Base64.getEncoder().encodeToString(blockData));
                        mergeFileMessage.put("blockId", i);
                        Node receiver = membershipManager.getNode(ch.getServer(firstBlockName));
                        sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), mergeFileMessage);
                    }
                }
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        }
    }


    public void handleMergeAck(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFilename");
        int requesterNodeId = message.getInt("requesterNodeId");

        // 确认是不是block2及以上持有者
        Set<String> blocksToDelete = new HashSet<>();
        for (String localFile : localFiles) {
            if (!localFile.equals("1_" + hydfsFilename) && localFile.endsWith(hydfsFilename)) {
                blocksToDelete.add(localFile);
            }
        }
        fileBlockMap.put(hydfsFilename, 1);
        // 删除所有set当中的文件, 并且在localSet当中移除
        for (String block : blocksToDelete) {
            try {
                Files.deleteIfExists(Paths.get("HyDFS" + nodeId + "/" + block));
                localFiles.remove(block);
                logger.info("Deleted " + block + " after merge confirmation.");
            } catch (IOException e) {
                logger.warning("Failed to delete block " + block + ": " + e.getMessage());
            }
        }

        // Requester 打印确认信息
        if (this.nodeId == requesterNodeId) {
            System.out.println("Merge command for " + hydfsFilename + " completed successfully.");
        }
    }

    // 当block1持有者收到了block2+持有者发来的block信息的时候进行append处理
    public void handleMergeFile(JSONObject message) {
        String blockName = message.getString("blockName");
        int blockId = message.getInt("blockId");
        String hyDFSFileName = message.getString("hydfsFilename");
        byte[] blockData = Base64.getDecoder().decode(message.getString("blockData"));

        String blockFilePath = "HyDFS" + nodeId + "/" + blockName;
        Path path = Paths.get(blockFilePath);

        try {
            if (!Files.exists(path)) {
                Files.write(path, blockData);
                logger.info("Stored data for " + blockName + " under node ID: " + nodeId);

                unreceivedBlocks.get(hyDFSFileName).remove(blockId);
            } else {
                logger.info("Block " + blockName + " already exists locally, skipping storage.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    private void sendTCP(String receiverIp, int receiverPort, JSONObject message){
        try (Socket socket = new Socket(receiverIp, receiverPort)) {
            socket.setSoTimeout(5000);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            writer.write(message.toString());
            writer.newLine();
            writer.flush();

            logger.info("Send " + message.getString("type") + " message to" + receiverIp + ":" + receiverPort);
        } catch (IOException e) {
            logger.warning("Failed to send " + message.getString("type") + " message to " + receiverIp + ":" +
                    receiverPort);
        }
    }

    private void gossip(JSONObject message, boolean isTCP) {
        int gossipCount = message.getInt("gossipCount");
        if(gossipCount > 2){
            return;
        }
        message.put("gossipCount", gossipCount + 1);

        List<Node> availableMembers = new ArrayList<>();
        for (Node member : membershipManager.getMembers().values()) {
            if (member.getStatus().equals("alive") || serverMode && member.getStatus().equals("suspect")) {
                availableMembers.add(member);
            }
        }

        int availableNumber = Math.min(availableMembers.size(), (availableMembers.size() / 3 + 2));
        if (availableNumber < 1) {
            logger.warning("No other member to disseminate " + message.getString("type") + " message");
            return;
        }

        Collections.shuffle(availableMembers, new Random());
        List<Node> selectedMembers = availableMembers.subList(0, availableNumber);

        for(Node member: selectedMembers) {
            if (isTCP) {
                sendTCP(member.getIpAddress(), member.getPortTCP(), message);
            } else {
                sendUDP(member.getIpAddress(), member.getPortUDP(), message);
            }

        }
    }

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
            logger.warning("Failed to send " + message.getString("type") + " message to " + receiverIp + ":" +
                    receiverPort);
        }
    }

}
