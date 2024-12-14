package main.java.rainStorm;

import main.java.hydfs.Server;
import org.json.JSONObject;
import org.apache.tools.ant.types.Commandline;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Worker {

    public Server server;
    public final int portTCP; // this worker's tcp port

    private int leaderPortTCP; // the leader's tcp port
    private String leaderIpAddress; // the leader's IP address

    private final HashMap<Integer, JSONObject> stream1; // hash map of stage-1 stream tuples
    private final Queue<Integer> streamQueue1; // queue of stage-1 stream tuples to be executed
    private final HashMap<Integer, JSONObject> stream2; // hash map of stage-2 stream tuples
    private final Queue<Integer> streamQueue2; // queue of stage-2 stream tuples to be executed
    private final HashMap<String, String> stashedOutputs1; // hash map storing stashed op1 output
    private final HashMap<Integer, String> stashedIds1; // hash map mapping stream tuple to op1 output
    private final HashMap<String, String> stashedOutputs2; // hash map storing stashed op2 output
    private final HashMap<Integer, String> stashedIds2; // hash map mapping stream tuple to op2 output

    private final Scanner scanner;
    private final Logger logger;

    private final Thread serverTcpListen;
    private final Thread serverUdpListen;
    private final Thread workerListen;
    private final Thread workerExecuteStream1;
    private final Thread workerExecuteStream2;
    private final ScheduledExecutorService scheduler;


    /**
     * Worker class constructor.
     * @param args Include Argument to initialize worker HyDFS server and worker's TCP port.
     */
    public Worker(String[] args) {
        this.server = new Server(Arrays.copyOfRange(args, 0, 5));
        this.portTCP = Integer.parseInt(args[5]);

        this.stream1 = new HashMap<>();
        this.streamQueue1 = new LinkedList<>();
        this.stream2 = new HashMap<>();
        this.streamQueue2 = new LinkedList<>();
        this.stashedOutputs1 = new HashMap<>();
        this.stashedIds1 = new HashMap<>();
        this.stashedOutputs2 = new HashMap<>();
        this.stashedIds2 = new HashMap<>();

        this.scanner = new Scanner(System.in);
        this.logger = Logger.getLogger("Worker");
        logger.setLevel(Level.WARNING);

        // Initialize a directory to store rainStorm log files
        File directory = new File("rainStorm" + server.nodeId);
        if (Files.exists(Paths.get(directory.getAbsolutePath()))) {
            server.deleteDirectoryRecursively(Paths.get(directory.getAbsolutePath()));
        }
        boolean created = directory.mkdir();
        if(created) {
            logger.info("RainStorm directory created");
        }else{
            logger.info("Failed to create RainStorm directory");
        }

        // Start threads to listen to TCP/UDP messages
        serverTcpListen = new Thread(server::tcpListen);
        serverTcpListen.start();
        serverUdpListen = new Thread(server::udpListen);
        serverUdpListen.start();
        workerListen = new Thread(this::tcpListen);
        workerListen.start();
        workerExecuteStream1 = new Thread(this::processStream1);
        workerExecuteStream1.start();
        workerExecuteStream2 = new Thread(this::processStream2);
        workerExecuteStream2.start();

        // Start threads to periodically ping and check for failure detection
        scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1, 1, TimeUnit.SECONDS);
    }


    /**
     * Worker main function
     * @param args Include Argument to initialize worker HyDFS server and worker's TCP port.
     */
    public static void main(String[] args) {
        if(args.length != 6) {
            System.out.println("Usage: java Worker <Node ID> <IP Address> <HyDFS TCP Port> "
                    + "<HyDFS UDP Port> <HyDFS Cache Size> <Worker TCP Port>");
            return;
        }
        Worker worker = new Worker(args);
        System.out.println("Succeed to launch worker");
        worker.readCommand();
    }


    /**
     * Continuously read command line user inputs.
     */
    public void readCommand() {
        while(server.running){
            System.out.println("Enter command for Worker:");
            String[] command = scanner.nextLine().split(" ");
            switch(command[0]){
                case "join":
                    if (command.length == 3) {
                        leaderIpAddress = command[1].split(":")[0];
                        int leaderHyDFSPortTCP = Integer.parseInt(command[1].split(":")[1]);
                        leaderPortTCP = Integer.parseInt(command[2]);
                        server.join(leaderIpAddress, leaderHyDFSPortTCP);
                        JSONObject JoinMessage = new JSONObject();
                        JoinMessage.put("type", "WorkerJoin");
                        JoinMessage.put("id", server.nodeId);
                        JoinMessage.put("portTCP", this.portTCP);
                        server.sendTCP(leaderIpAddress, leaderPortTCP, JoinMessage);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("join <Leader HyDFS IP Address>:<Leader HyDFS TCP Port> "
                                + "<Leader TCP Port>");
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
                    System.out.println("Stream Queue for Op1: " + streamQueue1);
                    System.out.println("Stream Queue for Op2: " + streamQueue2);
                    break;
                case "quit":
                    quit();
            }
        }
    }


    /**
     * Worker TCP listen.
     * Assign different types of incoming messages to their corresponding handle functions.
     */
    private void tcpListen() {
        try (ServerSocket tcpServerSocket = new ServerSocket(portTCP)) {
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
                switch (messageType) {
                    case "Stream":
                        handleStream(receivedMessage);
                        break;
                    case "WorkerJoinAck":
                        handleWorkerJoinAck(receivedMessage);
                        break;
                    case "Complete":
                        handleComplete();
                        break;
                    case "Quit":
                        quit();
                }
            }
        }catch(IOException e){
            logger.info("Cannot read from TCP packet\n" + e.getMessage());
        }
    }


    /**
     * Continuously process assigned stream tuples in streamQueue1.
     * Append to stashedOutputs1 if outputs are returned after processing.
     * Otherwise, immediately send acknowledgement to leader.
     */
    public void processStream1() {
        while (server.running) {
            if (streamQueue1.isEmpty() || stashedIds1.size() >= 50) {
                ackStashedChanges(1);
            } else {
                List<String> outputs;
                int id = streamQueue1.poll();
                if (stream1.get(id).getBoolean("stateful")) {
                    outputs = processStateful(stream1.get(id), 1);
                } else {
                    outputs = processStateless(stream1.get(id));
                }
                if(outputs.isEmpty()) {
                    JSONObject ackMessage = new JSONObject();
                    ackMessage.put("type", "Stream1EmptyAck");
                    ackMessage.put("id", id);
                    server.sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
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


    /**
     * Continuously process assigned stream tuples in streamQueue2.
     * Append to stashedOutputs2 if outputs are returned after processing.
     * Otherwise, immediately send acknowledgement to leader.
     */
    public void processStream2() {
        while (server.running) {
            if (streamQueue2.isEmpty() || stashedIds2.size() >= 50) {
                ackStashedChanges(2);
            } else {
                List<String> outputs;
                int id = streamQueue2.poll();
                if (stream2.get(id).getBoolean("stateful")) {
                    outputs = processStateful(stream2.get(id), 2);
                } else {
                    outputs = processStateless(stream2.get(id));
                }
                if (outputs.isEmpty()) {
                    JSONObject ackMessage = new JSONObject();
                    ackMessage.put("type", "Stream2EmptyAck");
                    ackMessage.put("id", id);
                    server.sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
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


    /**
     * Kill all threads and exit the program of the current worker
     */
    public void quit() {
        serverTcpListen.interrupt();
        serverUdpListen.interrupt();
        workerListen.interrupt();
        workerExecuteStream1.interrupt();
        workerExecuteStream2.interrupt();
        scheduler.shutdownNow();
        System.exit(0);
    }


    /**
     * Handle "Stream" message.
     * Store the stream tuple in the map and adds it to the processing Queue.
     * @param message Message received at the worker's TCP port.
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
     * Handle "WorkerJoinAck" message.
     * @param message Message received at the worker's TCP port.
     */
    private void handleWorkerJoinAck(JSONObject message) {
        this.leaderPortTCP = message.getInt("leaderPortTCP");
        logger.info("Join acknowledgment received for leader ");
    }


    /**
     * Handle "Complete" message.
     * Reset class variables.
     */
    private void handleComplete() {
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


    /**
     * Process the stream tuple by running the stateful operation.
     * Fetch current state from stashedOutputs or HyDFS logs.
     * @param info JSONObject of the stream tuple.
     * @param stage The stage of operation to execute.
     * @return Outputs of the operation execution on the stream tuple
     */
    private List<String> processStateful(JSONObject info, int stage) {
        String key = info.getString("key");
        String value = info.getString("value");
        String op = info.getString("op");
        String destFile = info.getString("destFile");
        if (!server.hasFile(destFile)) { server.createEmptyFile(destFile); }
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
        String[] cli = {op, key, value, currentState};
        return executeOperation(cli);
    }


    /**
     * Process the stream tuple by running the stateless operation.
     * @param info JSONObject of the stream tuple.
     * @return Outputs of the operation execution on the stream tuple
     */
    private List<String> processStateless(JSONObject info) {
        String destFile = info.getString("destFile");
        if (!server.hasFile(destFile)) { server.createEmptyFile(destFile); }
        String[] cli = {info.getString("op"), info.getString("key"), info.getString("value")};
        return executeOperation(cli);
    }


    /**
     * Acknowledge all stashed outputs of the processed stream tuples.
     * Update all outputs to HyDFS destination file.
     * @param stage Stage of the operation executed on the stream tuple.
     */
    private void ackStashedChanges(int stage) {
        HashMap<String, String> updates = new HashMap<>();
        List<JSONObject> ackMessages = new ArrayList<>();
        HashMap<Integer, String> stashedIds = stage == 1? stashedIds1 : stashedIds2;
        HashMap<String, String> stashedOutputs = stage == 1? stashedOutputs1 : stashedOutputs2;
        HashMap<Integer, JSONObject> stream = stage == 1? stream1 : stream2;
        for (int id: stashedIds.keySet()) {
            String key = stashedIds.get(id);
            String value = stashedOutputs.get(key);
            String destFile = stream.get(id).getString("destFile");
            if(!updates.containsKey(destFile)) updates.put(destFile, "");
            updates.put(destFile, updates.get(destFile) + "<Key>=" + key + "<Value>=" + value + "\n");
            JSONObject ackMessage = new JSONObject();
            if (stage == 1) ackMessage.put("type", "Stream1Ack");
            if (stage == 2) ackMessage.put("type", "Stream2Ack");
            ackMessage.put("id", id);
            ackMessage.put("key", key);
            ackMessage.put("value", value);
            ackMessages.add(ackMessage);
        }
        for (String destFile: updates.keySet()) {
            server.appendString(updates.get(destFile), destFile);
        }
        for (JSONObject ackMessage: ackMessages) {
            server.sendTCP(leaderIpAddress, leaderPortTCP, ackMessage);
        }
        stashedIds.clear();
    }


    /**
     * Call the operation command and read the output.
     * @param args Argument required to execute the operation.
     * @return List of Strings containing the key and value.
     */
    private List<String> executeOperation(String[] args) {
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
            String line;
            while ((line = stdInput.readLine()) != null) {
                outputs.add(line);
            }
            while ((line = stdError.readLine()) != null) {
                logger.warning("Error while executing command" + Arrays.toString(args) + ": " + line);
            }
        }catch (IOException e) {
            logger.warning("Cannot read from command output of " + Arrays.toString(args));
        }
        return outputs;
    }

}

