package main.java.rainStorm;

import main.java.hydfs.Server;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Worker {

    public Server server;
    final int portTCP; // self node's  tcp port
    final int leaderPortTCP;
    final int leaderNodeId;
    private HashMap<Integer, JSONObject> stream1;
    final Queue<Integer> streamQueue1;
    final ServerSocket tcpServerSocket;

    private final Logger logger;
    final Clock clock;

    public Worker(String[] args, int portTCP, int leaderPortTCP, int leaderNodeId) throws IOException, NoSuchAlgorithmException {
        this.clock = Clock.systemDefaultZone();
        this.portTCP = portTCP;
        this.leaderPortTCP = leaderPortTCP;
        this.leaderNodeId = leaderNodeId;
        this.stream1 = new HashMap<>();
        this.streamQueue1 = new LinkedList<>();
        this.logger = Logger.getLogger("Worker");
        this.tcpServerSocket = new ServerSocket(this.portTCP);

        // Start threads to listen to TCP/UDP messages
        server = new Server(args);
        Thread tcpListen = new Thread(server::tcpListen);
        tcpListen.start();
        Thread udpListen = new Thread(server::udpListen);
        udpListen.start();
        // Start threads to periodically ping and check for failure detection
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1, 1, TimeUnit.SECONDS);
    }

    private void tcpListen() {
        try{
            while(server.running){
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
                switch (messageType) {
                    case "NewPartition":
                        handleNewPartition(receivedMessage);
                        break;
                }
            }
        }catch(IOException e){
            logger.info("Cannot read from TCP packet\n" + e.getMessage());
        }
    }

    private void executeStateful() {

    }

    private void executeStream1() {
        while(server.running) {
            if(!streamQueue1.isEmpty()) {
                int id = streamQueue1.poll();
                String value = stream1.get(id).getString("value");
                String op1 = stream1.get(id).getString("op1");
                String op2 = stream1.get(id).getString("op2");
                String destFile = stream1.get(id).getString("destFile");
                List<String> outputs = execute(value + " " + op1);

                JSONObject AckMessage = new JSONObject();
                AckMessage.put("id", id);
                if(outputs.isEmpty()) {
                    AckMessage.put("type", "Stream1EmptyAck");
                    sendTCP(server.membership.get(leaderNodeId).getIpAddress(), leaderPortTCP, AckMessage);
                    continue;
                }
                AckMessage.put("type", "Stream1Ack");
                AckMessage.put("op2", op2);
                AckMessage.put("key", outputs.get(0));
                AckMessage.put("value", outputs.get(1));

            }
        }
    }

    /**
     * Handles the new partition/work assigned from leader to this worker
     * Store the partition in the map and adds it to the processing Queue
     */
    private void handleNewPartition(JSONObject message) {
        int partitionId = message.getInt("partitionId");
        if (!stream1.containsKey(partitionId)) {
            stream1.put(partitionId, message);
            streamQueue1.add(partitionId);
            logger.info("Received new partition: ID=" + partitionId + ", Value="
                    + message.getString("partitionValue"));
        } else {
            logger.warning("Duplicate partition received: ID=" + partitionId);
        }
    }

    private List<String> execute(String args) {
        List<String> outputs = new ArrayList<>();
        try{
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(args);
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));
            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(proc.getErrorStream()));

            // Read the output from the command
            String temp;
            while ((temp = stdInput.readLine()) != null) {
                outputs.add(temp);
            }
            // Read any errors from the attempted command
            while ((temp = stdError.readLine()) != null) {
                logger.warning("Error while executing command" + args + ": " + temp);
            }

        }catch (IOException e) {
            logger.warning("Cannot read from command output of " + args + "\n" + e.getMessage());
        }
        return outputs;
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
}
