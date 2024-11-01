package main.java;

import netscape.javascript.JSObject;

import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.*;
import org.json.*;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class Server {
    final int nodeId;
    final String ipAddress;
    final int portTCP;
    final int portUDP;
    final Logger logger;

    private ServerSocket tcpServerSocket;
    private ConsistentHashing ch;

    private MembershipManager membershipManager;
    private int successorId;
    private int predecessorId;
    private Boolean running;

    public Server(String[] args) throws IOException, NoSuchAlgorithmException {
        this.logger = Logger.getLogger("Server");
        this.nodeId = Integer.parseInt(args[0]);
        this.ipAddress = args[1];
        this.portTCP = Integer.parseInt(args[2]);
        this.portUDP = Integer.parseInt(args[3]);

        this.running = true;

        this.tcpServerSocket = new ServerSocket(portTCP);
        this.ch = new ConsistentHashing();
        this.membershipManager = new MembershipManager();
        membershipManager.addNode(new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));

    }

    public void tcpListen(){
        logger.info("Starting TCP Listen");
        while(running){
            try{
                Socket tcpSocket = tcpServerSocket.accept();
                tcpSocket.setSoTimeout(5000);
                InputStream inputStream = tcpSocket.getInputStream();
                ObjectInputStream ois = new ObjectInputStream(inputStream);
                JSONObject receivedMessage = new JSONObject((String)ois.readObject());
                System.out.println(receivedMessage);
                switch(receivedMessage.getString("type")){
                    case "Join":
                        handleJoinRequest(receivedMessage);
                        break;
                }
            }catch(ClassNotFoundException | IOException e){
                logger.info("Cannot read from TCP packet\n" + e.getMessage());
            }
        }
    }


    public void udpListen(){
        logger.info("Starting UDP Listen");
        try(DatagramSocket socket = new DatagramSocket(portUDP)){
            while(running){
                logger.info("Start UDP Listen");
                byte[] buffer = new byte[4096];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                ObjectInputStream ois = new ObjectInputStream(bais);
                // if(!running) break;
                JSONObject receivedMessage = (JSONObject) ois.readObject();

                if(receivedMessage.getString("type").equals("Ping")){

                }
            }
        }catch(IOException | ClassNotFoundException e){
            logger.info("Cannot read from UDP packet\n" + e.getMessage());
        }
    }

    public void ping() {
        if(running){
            for(int nodeId: Arrays.asList(predecessorId, successorId)) {
                try(DatagramSocket socket = new DatagramSocket()) {
                    JSONObject pingMessage = new JSONObject();
                    pingMessage.put("type", "Ping");
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(pingMessage);
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

    public void list(){
        System.out.println("Current Membership List of Node: " + nodeId);
        for(int nodeId: membershipManager.getMembers().keySet()){
            System.out.println("Node ID = " + nodeId
                    + "; IP Address = " + membershipManager.getNode(nodeId).getIpAddress()
                    + "; Node Status = " + membershipManager.getNode(nodeId).getStatus());
        }
    }

    // Method to join the system by contacting other nodes
    public void join(String introIpAddress, int introPort) throws IOException {
        running = true;
        membershipManager.clear();
        // pingedList.clear();
        membershipManager.addNode(new Node(nodeId, ipAddress, portUDP, portTCP, "alive"));
        JSONObject message = new JSONObject();
        message.put("type", "Join");
        message.put("nodeId", nodeId);
        message.put("ipAddress", ipAddress);
        message.put("portUDP", portUDP);
        message.put("portTCP", portTCP);


        InetAddress introAddress = InetAddress.getByName(introIpAddress);
        try(Socket socket = new Socket(introAddress, introPort);){
            socket.setSoTimeout(5000);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(message.toString());
            oos.flush();

            socket.getOutputStream().write(bos.toByteArray());
            socket.getOutputStream().flush();

            logger.info("Sent Join-Req Message");

            Thread.sleep(5000);
            if(membershipManager.getSize() > 1){
                logger.info("Join Succeeds");
            }else{
                logger.info("Join Failed for " + introIpAddress + ":" + introPort);
            }
        }catch (IOException | InterruptedException e){
            logger.warning("Cannot connect to introducer " + introIpAddress + ":" + introPort + e.getMessage());
        }
    }

    public void handleJoinRequest(JSONObject message) {

        int joiningNodeId = message.getInt("nodeId");
        String joiningNodeIp = message.getString("ipAddress");
        int joiningNodePortUDP = message.getInt("portUDP");
        int joiningNodePortTCP = message.getInt("portTCP");
        Node joiningNode = new Node(joiningNodeId, joiningNodeIp, joiningNodePortUDP, joiningNodePortTCP);

        if (!membershipManager.containsNode(joiningNodeId)) {
            membershipManager.addNode(joiningNode);
            ch.addServer("Server" + joiningNode.getNodeId());
            logger.info("Node " + joiningNode.getNodeId() + " joined successfully");

            // 创建Join-Update消息
            JSONObject joinUpdateMessage = new JSONObject();
            joinUpdateMessage.put("type", "Join-Update");
            joinUpdateMessage.put("nodeId", joiningNodeId);
            joinUpdateMessage.put("portUDP", joiningNodePortUDP);
            joinUpdateMessage.put("portTCP", joiningNodePortTCP);
            joinUpdateMessage.put("status", "alive");

            // Multicast to all members
            for (Node member : membershipManager.getMembers().values()) {
                if (member.getNodeId() != nodeId) {
                    try (Socket socket = new Socket(member.getIpAddress(), member.getPortTCP())) {
                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        writer.write(joinUpdateMessage.toString());
                        writer.newLine();
                        writer.flush();

                        logger.info("Send Join-Update Message to" + member.getIpAddress() + ":" + member.getPortTCP());
                    } catch (IOException e) {
                        logger.warning("Failed to send Join-Update message to " + member.getIpAddress() + ":" + member.getPortTCP());
                    }
                }
            }
        } else {
            logger.info("Node" + joiningNode.getNodeId() + " already joined as a member");
        }

    }


    // TODO: Handle JoinUpdate
}
