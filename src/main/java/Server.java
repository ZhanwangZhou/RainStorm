package main.java;

import netscape.javascript.JSObject;

import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.*;
import org.json.*;

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
    private Boolean running;

    public Server(String[] args) throws IOException, NoSuchAlgorithmException {
        this.logger = Logger.getLogger("Server");
        this.clock = Clock.systemDefaultZone();

        this.nodeId = Integer.parseInt(args[0]);
        this.ipAddress = args[1];
        this.portTCP = Integer.parseInt(args[2]);
        this.portUDP = Integer.parseInt(args[3]);

        this.running = true;

        this.tcpServerSocket = new ServerSocket(portTCP);
        this.ch = new ConsistentHashing();
        this.membershipManager = new MembershipManager();
        membershipManager.addNode(new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        ch.addServer(nodeId);

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
                    failureMessage.put("nodeId", ch.getPredecessor(this.predecessorlastPingId));
//                    for(Node member: membershipManager.getMembers().values()) {
//                        sendTCP(member.getIpAddress(), member.getPortTCP(), failureMessage);
//                    }
                    List<Node> membersSnapshot = new ArrayList<>(membershipManager.getMembers().values());
                    for (Node member : membersSnapshot) {
                        sendTCP(member.getIpAddress(), member.getPortTCP(), failureMessage);
                    }
                }
                if(currentTime - successorLastPingTime > 5000) {
                    System.out.println(currentTime + " " + successorLastPingTime);
                    JSONObject failureMessage = new JSONObject();
                    failureMessage.put("type", "Failure");
                    failureMessage.put("nodeId", ch.getSuccssor(this.successorlastPingId));
//                    for(Node member: membershipManager.getMembers().values()) {
//                        sendTCP(member.getIpAddress(), member.getPortTCP(), failureMessage);
//                    }
                    List<Node> membersSnapshot = new ArrayList<>(membershipManager.getMembers().values());
                    for (Node member : membersSnapshot) {
                        sendTCP(member.getIpAddress(), member.getPortTCP(), failureMessage);
                    }
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
        for(Node member : membershipManager.getMembers().values()){
            JSONObject leaveMessage = new JSONObject();
            leaveMessage.put("type", "Leave");
            leaveMessage.put("nodeId", nodeId);
            sendTCP(member.getIpAddress(), member.getPortTCP(), leaveMessage);
        }
        membershipManager.getNode(nodeId).setStatus("leave");
        ch.removeServer(nodeId);
        logger.info("Send Leave message to all members and leave");
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
        } else {
            logger.warning("Node" + leaveNodeId + " not found");
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
    }

}
