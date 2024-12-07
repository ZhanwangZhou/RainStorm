package main.java.rainStorm;

import main.java.hydfs.Server;
import org.json.JSONObject;
import org.apache.tools.ant.types.Commandline;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Worker {

    public Server server;
    final int portTCP; // self node's  tcp port
    private int leaderPortTCP;
    private String leaderIpAddress;
    private int leaderNodeId;
    private HashMap<Integer, JSONObject> stream1;
    final Queue<Integer> streamQueue1;
    private HashMap<Integer, JSONObject> stream2;
    final Queue<Integer> streamQueue2;
    final ServerSocket tcpServerSocket;

    private HashMap<String, String> stashedOutputs1; // new key -> new val
    private HashMap<Integer, String> stashedIds1; // id -> new key
    private HashMap<String, String> stashedOutputs2;
    private HashMap<Integer, String> stashedIds2;


    private final Logger logger;
    final Clock clock;
    private final Scanner scanner;

    public Worker(String[] args) throws IOException, NoSuchAlgorithmException {
        this.portTCP = Integer.parseInt(args[5]);
        this.clock = Clock.systemDefaultZone();
        this.stream1 = new HashMap<>();
        this.streamQueue1 = new LinkedList<>();
        this.stream2 = new HashMap<>();
        this.streamQueue2 = new LinkedList<>();
        this.logger = Logger.getLogger("Worker");
        logger.setLevel(Level.WARNING);
        this.scanner = new Scanner(System.in);
        this.tcpServerSocket = new ServerSocket(this.portTCP);

        this.stashedOutputs1 = new HashMap<>();
        this.stashedIds1 = new HashMap<>();
        this.stashedOutputs2 = new HashMap<>();
        this.stashedIds2 = new HashMap<>();

        // Start threads to listen to TCP/UDP messages
        server = new Server(Arrays.copyOfRange(args, 0, 5));
        // Initialize a directory to store rainStorm log files
        File directory = new File("rainStorm" + server.nodeId);
        if (Files.exists(Paths.get(directory.getAbsolutePath()))) {
            server.deleteDirectoryRecursively(Paths.get(directory.getAbsolutePath()));
        }
        boolean created = directory.mkdir();
        if(created) {
            logger.info("HyDFS directory created");
        }else{
            logger.info("Failed to create HyDFS directory");
        }

        Thread serverTcpListen = new Thread(server::tcpListen);
        serverTcpListen.start();
        Thread serverUdpListen = new Thread(server::udpListen);
        serverUdpListen.start();
        Thread workerListen = new Thread(this::tcpListen);
        workerListen.start();
        Thread workerExecuteStream1 = new Thread(this::executeStream1);
        workerExecuteStream1.start();
        Thread workerExecuteStream2 = new Thread(this::executeStream2);
        workerExecuteStream2.start();
        // Start threads to periodically ping and check for failure detection
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1, 1, TimeUnit.SECONDS);

        while(server.running){
            System.out.println("Enter command for Worker:");
            String[] command = scanner.nextLine().split(" ");
            switch(command[0]){
                case "join":
                    if (command.length == 3) {
                        leaderIpAddress = command[1].split(":")[0];
                        int port = Integer.parseInt(command[1].split(":")[1]);
                        leaderPortTCP = Integer.parseInt(command[2]);
                        server.join(leaderIpAddress, port);
                        JSONObject JoinMessage = new JSONObject();
                        JoinMessage.put("type", "WorkerJoin");
                        JoinMessage.put("id", server.nodeId);
                        JoinMessage.put("portTCP", this.portTCP);
                        sendTCP(leaderIpAddress, leaderPortTCP, JoinMessage);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("join <Leader Server IP Address>:<Leader Server Port> <Leader Port>");
                    }
                    break;
                case "create":
                    if (command.length == 3) {
                        server.createFile(command[1], command[2]);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("create <Local Filepath> <HyDFS Filename>");
                    }
                    break;
                case "queue":
                    System.out.println(streamQueue1);
                    System.out.println(streamQueue2);
                    break;
                case "quit":
                    serverTcpListen.interrupt();
                    serverUdpListen.interrupt();
                    workerListen.interrupt();
                    break;
            }
        }
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        if(args.length != 6) {
            System.out.println("Please specify the command as:");
            System.out.println("Worker <node_id> <ip_address> <hydfs_tcp_port> <hydfs_udp_port> " +
                    "<hydfs_cache_size> <worker_tcp_port>");
            return;
        }
        Worker worker = new Worker(args);
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
                    case "Stream":
                        handleStream(receivedMessage);
                        break;
                    case "WorkerJoinAck":
                        handleWorkerJoinAck(receivedMessage);
                        break;
                    case "Complete":
                        handleComplete(receivedMessage);
                        break;
                }
            }
        }catch(IOException e){
            logger.info("Cannot read from TCP packet\n" + e.getMessage());
        }
    }

    private List<String> executeStateful(int id, JSONObject info, int stage) {
        String key = info.getString("key");
        String value = info.getString("value");
        String op = info.getString("op");
        String destFile = info.getString("destFile");
        if (!server.fileBlockMap.containsKey(destFile)) {
            server.createEmptyFile(destFile);
        }
        String currentState = "";

        if (stage == 1 && stashedOutputs1.containsKey(key)) {
            currentState = stashedOutputs1.get(key);
        } else if (stage == 2 && stashedOutputs2.containsKey(key)){
            currentState = stashedOutputs2.get(key);
        } else {
            String localFilename = "rainStorm" + server.nodeId + "/" + destFile;
            server.getFile(destFile, localFilename);
            try(BufferedReader reader = new BufferedReader(new FileReader(localFilename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    int index1 = line.indexOf("<Key>=");
                    int index2 = line.indexOf("<Value>=");
                    String keyInLine = line.substring(index1 + 6, index2);
                    if (keyInLine.equals(key)) {
                        currentState = line.substring(index2 + 8);
                    }
                }
                Files.deleteIfExists(Paths.get(localFilename));
            } catch (IOException e) {
                System.out.println(localFilename);
                logger.warning("Failed to read log file " + e.getMessage());
            }
        }
        // key = key.indexOf(' ') == -1? key : "'" + key + "'";
        // value = value.indexOf(' ') == -1? value : "'" + value + "'";
        // currentState = currentState.indexOf(' ') == -1? currentState : "'" + currentState + "'";
        String[] cli = {op, key, value, currentState};
        return execute(cli);
    }

    private List<String> executeStateless(int id, JSONObject info) {
        String key = info.getString("key");
        String value = info.getString("value");
        String op = info.getString("op");
        String destFile = info.getString("destFile");
        if (!server.fileBlockMap.containsKey(destFile)) {
            server.createEmptyFile(destFile);
        }
        // key = key.indexOf(' ') == -1? key : "'" + key + "'";
        // value = value.indexOf(' ') == -1? value : "'" + value + "'";
        String[] cli = {op, key, value};
        return execute(cli);
    }

    private void executeStream1() {
        while (server.running) {
            if (streamQueue1.isEmpty() || stashedIds1.size() >= 20) {
                ackStashedChanges(1);
            } else {
                List<String> outputs;
                int id = streamQueue1.poll();
                JSONObject partition = stream1.get(id);
                if (partition.getBoolean("stateful")) {
                    outputs = executeStateful(id, stream1.get(id), 1);
                } else {
                    outputs = executeStateless(id, stream1.get(id));
                }
                if(outputs.isEmpty()) {
                    JSONObject ackMessage = new JSONObject();
                    ackMessage.put("type", "Stream1EmptyAck");
                    ackMessage.put("id", id);
                    sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
                } else if (outputs.size() == 2) {
                    String key = outputs.get(0);
                    String value = outputs.get(1);
                    stashedOutputs1.put(key, value);
                    stashedIds1.put(id, key);
                } else {
                    logger.warning("Unexpected output from stream1.");
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                logger.warning("stream 1 execution interrupted");
            }
        }
    }

    private void executeStream2() {
        while (server.running) {
            if (streamQueue2.isEmpty() || stashedIds2.size() >= 20) {
                ackStashedChanges(2);
            } else {
                List<String> outputs;
                int id = streamQueue2.poll();
                JSONObject partition = stream2.get(id);
                if (partition.getBoolean("stateful")) {
                    outputs = executeStateful(id, stream2.get(id), 2);
                } else {
                    outputs = executeStateless(id, stream2.get(id));
                }
                if (outputs.isEmpty()) {
                    JSONObject ackMessage = new JSONObject();
                    ackMessage.put("type", "Stream2EmptyAck");
                    ackMessage.put("id", id);
                    sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
                } else if (outputs.size() == 2) {
                    String key = outputs.get(0);
                    String value = outputs.get(1);
                    stashedOutputs2.put(key, value);
                    stashedIds2.put(id, key);
                } else {
                    logger.warning("Unexpected output from stream2.");
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                logger.warning("stream 1 execution interrupted");
            }
        }
    }

    private void ackStashedChanges(int stage) {
        HashMap<String, String> updates = new HashMap<>();
        List<JSONObject> ackMessages = new ArrayList<>();
        if (stage == 1) {
            for (int id: stashedIds1.keySet()) {
                String key = stashedIds1.get(id);
                String value = stashedOutputs1.get(key);
                String destFile = stream1.get(id).getString("destFile");
                if(!updates.containsKey(destFile)) updates.put(destFile, "");
                updates.put(destFile, updates.get(destFile) + "<Key>=" + key + "<Value>=" + value + "\n");
                JSONObject ackMessage = new JSONObject();
                ackMessage.put("type", "Stream1Ack");
                ackMessage.put("id", id);
                ackMessage.put("key", key);
                ackMessage.put("value", value);
                ackMessages.add(ackMessage);
            }
            for (String destFile: updates.keySet()) {
                server.appendString(updates.get(destFile), destFile);
            }
            for (JSONObject ackMessage: ackMessages) {
                sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
            }
            stashedOutputs1.clear();
            stashedIds1.clear();
        } else if (stage == 2) {
            for (int id: stashedIds2.keySet()) {
                String key = stashedIds2.get(id);
                String value = stashedOutputs2.get(key);
                String destFile = stream2.get(id).getString("destFile");
                if(!updates.containsKey(destFile)) updates.put(destFile, "");
                updates.put(destFile, updates.get(destFile) + "<Key>=" + key + "<Value>=" + value + "\n");
                JSONObject ackMessage = new JSONObject();
                ackMessage.put("type", "Stream2Ack");
                ackMessage.put("id", id);
                ackMessage.put("key", key);
                ackMessage.put("value", value);
                ackMessages.add(ackMessage);
            }
            for (String destFile: updates.keySet()) {
                server.appendString(updates.get(destFile), destFile);
            }
            for (JSONObject ackMessage: ackMessages) {
                sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
            }
            stashedOutputs2.clear();
            stashedIds2.clear();
        }
    }

    /**
     * Handles the new partition/work assigned from leader to this worker
     * Store the partition in the map and adds it to the processing Queue
     */
    private void handleStream(JSONObject message) {
        int partitionId = message.getInt("id");
        if (message.getInt("stage") == 1){
            if (!stream1.containsKey(partitionId)) {
                stream1.put(partitionId, message);
                streamQueue1.add(partitionId);
                logger.info("Received new partition: ID=" + partitionId + ", Value="
                        + message.getString("value"));
            } else {
                logger.info("Duplicate partition received: ID=" + partitionId);
            }
        } else if (message.getInt("stage") == 2){
            if (!stream2.containsKey(partitionId)) {
                stream2.put(partitionId, message);
                streamQueue2.add(partitionId);
                logger.info("Received new partition: ID=" + partitionId + ", Value="
                        + message.getString("value"));
            } else {
                logger.info("Duplicate partition received: ID=" + partitionId);
            }
        }
    }

    /**
     * Handles Join Acknowledgment from Leader
     * Logs a success message upon receiving the acknowledgment
     */
    private void handleWorkerJoinAck(JSONObject message) {
        this.leaderPortTCP = message.getInt("leaderPortTCP");
        logger.info("Join acknowledgment received for leader ");
        System.out.println("Successfully joined Leader as Worker");
    }

    private void handleComplete(JSONObject message) {
        stream1.clear();
        stashedIds1.clear();
        stashedOutputs1.clear();
        streamQueue1.clear();
        stream2.clear();
        stashedIds2.clear();
        stashedOutputs2.clear();
        streamQueue2.clear();
        logger.info("Received \"Complete\" message from the leader.");
        System.out.println("Complete all streams");
    }

    private List<String> execute(String[] args) {
        List<String> outputs = new ArrayList<>();
        try{
            Runtime rt = Runtime.getRuntime();
            String[] args0 = Commandline.translateCommandline(args[0]);
            String[] newArgs = new String[args.length - 1 + args0.length];
            for(int i = 0; i < args0.length; i++) {
                newArgs[i] = args0[i];
            }
            for(int i = 1; i < args.length; i++) {
                newArgs[i + args0.length - 1] = args[i];
            }
            Process proc = rt.exec(newArgs);
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
                logger.warning("Error while executing command" + Arrays.toString(args) + ": " + temp);
            }

        }catch (IOException e) {
            logger.warning("Cannot read from command output of " + Arrays.toString(args) + "\n" + e.getMessage());
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

