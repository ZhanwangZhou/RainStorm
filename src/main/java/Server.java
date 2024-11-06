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
    private int predecessorlastPingId;
    private long successorLastPingTime;
    private int successorlastPingId;
    private Map<String, Set<Integer>> receivedBlocks;
    private Boolean running;
    private Boolean serverMode; // Mode Flag: false = PingAck and true = PingAck+S

    // File list of this server
    Map<String, Integer> fileBlockMap;
    LRUCache lruCache = new LRUCache(3);

    public Server(String[] args) throws IOException, NoSuchAlgorithmException {
        this.logger = Logger.getLogger("Server");
        this.clock = Clock.systemDefaultZone();

        this.nodeId = Integer.parseInt(args[0]);
        this.ipAddress = args[1];
        this.portTCP = Integer.parseInt(args[2]);
        this.portUDP = Integer.parseInt(args[3]);
        this.fileBlockMap = new HashMap<>();

        this.running = true;
        this.serverMode = false;

        this.tcpServerSocket = new ServerSocket(portTCP);
        this.ch = new ConsistentHashing();
        this.membershipManager = new MembershipManager();
        membershipManager.addNode(new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        ch.addServer(nodeId);
        this.receivedBlocks = new HashMap<>();

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
                        case "GetFile":
                            handleGetFile(receivedMessage);
                            break;
                        case "GetFileBlockResponse":
                            handleGetFileBlockResponse(receivedMessage);
                            break;
                        case "AppendFile":
                            handleAppendFile(receivedMessage);
                            break;
                        case "UpdateFile":
                            handleUpdateFile(receivedMessage);
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

                switch(receivedMessage.getString("type")) {
                    case "Ping":
                        handlePing(receivedMessage);
                        break;
                }
            }
        }catch(IOException | ClassNotFoundException e){
            logger.info("Cannot read from UDP packet\n" + e.getMessage());
        }
    }

    public void ping(){
        try {
            if(running) {
                for (int nodeId : Arrays.asList(ch.getSuccssor(nodeId), ch.getPredecessor(nodeId))) {
                    try (DatagramSocket socket = new DatagramSocket()) {
                        JSONObject pingMessage = new JSONObject();
                        pingMessage.put("type", "Ping");
                        pingMessage.put("nodeId", this.nodeId);
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
        } catch (Exception e) {
            logger.warning("Exception in ping");
            e.printStackTrace();
        }

    }

    public void checkPing() {
        try {
            if(running) {
                long currentTime = clock.millis();
                if(currentTime - predecessorLastPingTime > 5000) {
                    System.out.println(currentTime + " " + predecessorLastPingTime);
                    JSONObject failureMessage = new JSONObject();
                    failureMessage.put("type", "Failure");
                    failureMessage.put("nodeId", this.predecessorlastPingId);
                    failureMessage.put("gossipCount", 0);
                    gossipTCP(failureMessage);
                }
                if(currentTime - successorLastPingTime > 5000) {
                    System.out.println(currentTime + " " + successorLastPingTime);
                    JSONObject failureMessage = new JSONObject();
                    failureMessage.put("type", "Failure");
                    failureMessage.put("nodeId", this.successorlastPingId);
                    failureMessage.put("gossipCount", 0);
                    gossipTCP(failureMessage);
                }
            }
        } catch (Exception e) {
            logger.warning("Exception in checkPing");
            e.printStackTrace();
        }
    }

    public void list(){
        System.out.println("Current Membership List of Node: " + nodeId);
        for(int nodeId: membershipManager.getMembers().keySet()){
            System.out.println("Node ID = " + nodeId
                    + "; IP Address = " + membershipManager.getNode(nodeId).getIpAddress()
                    + "; Node Status = " + membershipManager.getNode(nodeId).getStatus());
        }
    }

    // Method to join the system by contacting other nodes
    public void join(String introIpAddress, int introPort) throws IOException, InterruptedException {
        running = true;
        membershipManager.clear();
        // pingedList.clear();
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
        gossipTCP(leaveMessage);
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

        byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
        byte[] blockData = Arrays.copyOfRange(fileContent, 0, fileContent.length);

        for (int memberId : Arrays.asList(receiverId, ch.getPredecessor(receiverId), ch.getSuccssor(receiverId))) {
            Node member = membershipManager.getNode(memberId);
            JSONObject createFileMessage = new JSONObject();
            createFileMessage.put("type", "CreateFile");
            createFileMessage.put("hydfsFileName", hydfsFilename);
            createFileMessage.put("blockNum", 1);
            createFileMessage.put("blockData", Base64.getEncoder().encodeToString(blockData));
            sendTCP(member.getIpAddress(), member.getPortTCP(), createFileMessage);
        }
        JSONObject updateFileMessage = new JSONObject();
        updateFileMessage.put("type", "UpdateFile");
        updateFileMessage.put("hydfsFileName", hydfsFilename);
        updateFileMessage.put("blockNum", 1);
        updateFileMessage.put("gossipCount", 0);
        gossipTCP(updateFileMessage);

    }

    public void getFile(String hydfsFilename, String localFilename) {
        try {
            if(!fileBlockMap.containsKey(hydfsFilename)) {
                logger.warning("The file to be gotten does not exist in HyDFS");
                return;
            }else if(fileBlockMap.get(hydfsFilename) != lruCache.get(hydfsFilename)){
                int blockNum = fileBlockMap.get(hydfsFilename);
                receivedBlocks.put(hydfsFilename, new HashSet<>());
                for(int i = 1; i <= blockNum; ++i){
                    receivedBlocks.get(hydfsFilename).add(i);
                }
                int secondsPassed = 0;
                while(!receivedBlocks.get(hydfsFilename).isEmpty()){
                    if(secondsPassed % 3 == 0) {
                        for(int i: receivedBlocks.get(hydfsFilename)) {
                            Node receiver = membershipManager.getNode(ch.getServer(i + "_" + hydfsFilename));
                            if(secondsPassed % 9 == 3) {
                                receiver = membershipManager.getNode(ch.getPredecessor(receiver.getNodeId()));
                            } else if(secondsPassed % 9 == 6) {
                                receiver = membershipManager.getNode(ch.getSuccssor(receiver.getNodeId()));
                            }
                            JSONObject getFileMessage = new JSONObject();
                            getFileMessage.put("type", "GetFile");
                            getFileMessage.put("hydfsFileName", hydfsFilename);
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
                        for(String line : Files.readAllLines(blockFilePath)){
                            System.out.println(line);
                            writer.write(line);
                            writer.newLine();
                        }
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

    public void appendFile(String localFilename, String hydfsFilename) throws IOException {
        int blockId = fileBlockMap.get(hydfsFilename) + 1;
        Node receiver = membershipManager.getNode(ch.getServer(blockId + "_" + hydfsFilename));
        byte[] fileContent = Files.readAllBytes(Paths.get(localFilename));
        byte[] blockData = Arrays.copyOfRange(fileContent, 0, fileContent.length);
        JSONObject appendFileMessage = new JSONObject();
        appendFileMessage.put("type", "AppendFile");
        appendFileMessage.put("hydfsFileName", hydfsFilename);
        appendFileMessage.put("blockId", blockId);
        appendFileMessage.put("blockData", Base64.getEncoder().encodeToString(blockData));
        sendTCP(receiver.getIpAddress(), receiver.getPortTCP(), appendFileMessage);
    }

    public void handleGetFile(JSONObject message) {
        try {
            String blockFileName = message.getString("blockFileName");
            int requesterNodeId = message.getInt("nodeId");
            String hydfsFilename = message.getString("hydfsFileName");
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
            responseMsg.put("hydfsFileName", hydfsFilename);
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

    public void handleGetFileBlockResponse(JSONObject message) {
        try {
            // Retrieve the data and filename
            String blockFileName = message.getString("blockFileName");
            byte[] blockData = Base64.getDecoder().decode(message.getString("fileData"));

            // Save the block to a local path
            String blockPath = "Cache" + nodeId + "/" + blockFileName;
            Files.write(Paths.get(blockPath), blockData);

            String hydfsFilename = message.getString("hydfsFileName");
            int blockId = message.getInt("blockId");
            receivedBlocks.get(hydfsFilename).remove(blockId);

            logger.info("Received and cached block of file " + blockFileName + " to node " + this.nodeId);

        } catch (IOException e) {
            logger.warning("Error while handling GetFileBlock Response: " + e.getMessage());
        }
    }


    public void handleAppendFile(JSONObject message) {
        try {
            String hydfsFilename = message.getString("hydfsFileName");
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

                JSONObject createFileMessage = new JSONObject();
                createFileMessage.put("type", "CreateFile");
                createFileMessage.put("hydfsFileName", hydfsFilename);
                createFileMessage.put("blockNum", newBlockNum);
                createFileMessage.put("blockData", Base64.getEncoder().encodeToString(data));
                Node predecessor = membershipManager.getNode(ch.getPredecessor(nodeId));
                Node successor = membershipManager.getNode(ch.getSuccssor(nodeId));
                sendTCP(predecessor.getIpAddress(), predecessor.getPortTCP(), createFileMessage);
                sendTCP(successor.getIpAddress(), successor.getPortTCP(), createFileMessage);

                // Notify other member with new blockCount
                JSONObject updateFileMessage = new JSONObject();
                updateFileMessage.put("type", "UpdateFile");
                updateFileMessage.put("hydfsFileName", hydfsFilename);
                updateFileMessage.put("blockNum", newBlockNum);
                updateFileMessage.put("gossipCount", 0);
                gossipTCP(updateFileMessage);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void handleCreateFile(JSONObject message) {
        try {
            String hydfsFilename = message.getString("hydfsFileName");
            int blockNum = message.getInt("blockNum");
            byte[] data = Base64.getDecoder().decode(message.getString("blockData"));

            // save the block to local HyDfs
            String filePath = "HyDFS" + nodeId + "/" + blockNum + "_" + hydfsFilename ;
            Files.write(Paths.get(filePath), data);

            fileBlockMap.put(hydfsFilename, blockNum); // Initially, each file starts with one block
            logger.info("Block " + blockNum + " of file " + hydfsFilename + " saved to " + filePath);
        } catch (IOException e) {
            logger.warning("Failed to save block to local HyDFS directory.");
        }
    }

    public void handleUpdateFile(JSONObject message) {
        String hydfsFilename = message.getString("hydfsFileName");
        int blockNum = message.getInt("blockNum");
        fileBlockMap.put(hydfsFilename, blockNum);
        gossipTCP(message);
    }


    private void handlePing(JSONObject message) {
        if (message.getInt("nodeId") == ch.getPredecessor(nodeId)) {
            predecessorLastPingTime = clock.millis();
            predecessorlastPingId = message.getInt("nodeId");
        }
        if (message.getInt("nodeId") == ch.getSuccssor(nodeId)) {
            successorLastPingTime = clock.millis();
            successorlastPingId = message.getInt("nodeId");
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
        logger.info("Node " + joiningNode.getNodeId() + " joined successfully");
    }


    // MemberList 里面的node收到leave message 之后来handleLeave
    private void handleLeave(JSONObject message) {
        int leaveNodeId = message.getInt("nodeId");
        Node leavingNode = membershipManager.getNode(leaveNodeId);

        // Check if the node exist
        if (leavingNode != null) {
            // Set leavingNode status to 'leave'
            leavingNode.setStatus("leave");
            logger.info("Node" + leaveNodeId + " left successfully, set status to \"leave\"");
            // Update consistent hashing ring
            ch.removeServer(leaveNodeId);
            gossipTCP(message);
        } else {
            logger.warning("Node" + leaveNodeId + " not found");
        }
    }

    private void handleFailure(JSONObject message) {
        int failedNodeId = message.getInt("nodeId");
        // 在consistent hashing ring 和 membership manager当中删除掉这个节点
        ch.removeServer(failedNodeId);
        membershipManager.removeNode(failedNodeId);
        logger.info("Node " + failedNodeId + " marked as failed node and removed from membership list and ring");

        // 如果被判断的是当前节点直接改running为 false
        if (this.nodeId == failedNodeId) {
            this.running = false;
        }
        gossipTCP(message);
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
        }

        logger.info("Update membership List completed from introducer");
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

    private void gossipTCP(JSONObject message) {
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

        for(Node member: availableMembers) {
            sendTCP(member.getIpAddress(), member.getPortTCP(), message);
        }
    }

}
