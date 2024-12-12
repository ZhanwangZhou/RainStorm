package main.java.rainStorm;

import main.java.hydfs.Node;
import main.java.hydfs.Server;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

public class Leader {
    public Server server;
    final int portTCP; // self node's tcp port
    final HashMap<Integer, Integer> membership; // membership lists store all worker tcp ports

    private final List<Integer> op1Workers;
    private final List<Integer> op2Workers;
    private List<Integer> availableWorkers;

    private final Logger logger;
    private Scanner scanner;

    final Clock clock;
    private ConcurrentHashMap<Integer, Long> timeTable;  // key = uniqueId1, value = createdTime
    private HashMap<Integer, KeyValue> partitions;  // key=uniqueId1, value=KeyValue={filename:lineNumber, line}
    private int uniqueId = 0;  // Global uniqueId for Op1 and Op2
    private int appUniqueId = 0; // Gloabl uniqueId for RainStorm app

    private int stage1Counter = 0;  // Count total number of stage1 tasks
    private int stage2Counter = 0;  // Count total number of stage2 tasks

    private HashMap<String, String> stage2Results = new HashMap<>();

    public String hydfsSrcFile;
    public String hydfsDestFilename;
    public String op1;
    public boolean op1Stateful;
    public String op2;
    public boolean op2Stateful;
    int numTasks;
    boolean available; // if there is any RainStorm task being processed
    boolean readingSrcFile; // if the leader is reading src file

    private HashMap<Integer, Integer> completionCount;  // key = partitionId; value = count


    final ServerSocket tcpServerSocket;

    public Leader(String[] args, int portTCP) throws IOException, NoSuchAlgorithmException {
        this.clock = Clock.systemDefaultZone();
        this.logger = Logger.getLogger("Leader");
        logger.setLevel(Level.WARNING);
        this.scanner = new Scanner(System.in);
        this.portTCP = portTCP;
        this.membership = new HashMap<>();
        this.partitions = new HashMap<>();
        this.timeTable = new ConcurrentHashMap<>();
        this.tcpServerSocket = new ServerSocket(this.portTCP);
        this.op1Workers = new ArrayList<>();
        this.op2Workers = new ArrayList<>();
        this.availableWorkers = new ArrayList<>();
        this.available = true;
        this.readingSrcFile = false;
        this.completionCount = new HashMap<>();


        // Start threads to listen to TCP/UDP messages
        server = new Server(args);
        // Create "rainStorm" + server.nodeId directory if not exist
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
        Thread leaderListen = new Thread(this::tcpListen);
        leaderListen.start();

        // Start threads to periodically ping and check for failure detection
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::checkTimeTable, 0, 2, TimeUnit.SECONDS);



        while (server.running) {
            System.out.println("Enter command for Leader:");
            String[] command = scanner.nextLine().split(" ");
            switch(command[0]){
                case "RainStorm":
                    if(command.length == 4) {
                        initRainStorm(command[1], command[2], Integer.parseInt(command[3]));
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("RainStorm <hydfs_src_file> <hydfs_dest_filename> <num_tasks>");
                    }
                    break;
                case "printPartitions":
                    System.out.println(partitions);
                    break;
                case "list_workers":  // list all current workers
                    listWorkers();
                    break;
                case "list_server_mem":  // list all server members
                    server.listMem();
                    break;
                case "ls":  // list file location
                    if (command.length == 2) {
                        listFileLocation(command[1]);
                    } else {
                        System.out.println("Usage: ls <filename>");
                    }
                    break;
                case "create":
                    if (command.length == 3) {
                        try {
                            server.createFile(command[1], command[2]);
                            System.out.println("File created in HyDFS: " + command[2]);
                        } catch (Exception e) {
                            System.out.println("Error creating file: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("create <Local Filepath> <HyDFS Filename>");
                    }
                    break;
                case "get":
                    if (command.length == 3) {
                        new Thread(() -> {
                            try {
                                server.getFile(command[1], command[2]);
                                System.out.println("File retrieved to local path: " + command[2]);
                            } catch (Exception e) {
                                System.out.println("Error retrieving file: " + e.getMessage());
                            }
                        }).start();
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("get <HyDFS Filename> <Local Filepath>");
                    }
                    break;
                case "append":
                    if (command.length == 3) {
                        server.appendFile(command[1], command[2]);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("append <Local Filepath> <HyDFS Filename>");
                    }
                    break;
                case "workers":
                    System.out.println(op1Workers);
                    System.out.println(op2Workers);
                    break;
            }
        }
    }

    public void tcpListen(){
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
                switch(messageType){
                    case "WorkerJoin":
                        handleWorkerJoin(receivedMessage);
                        break;
                    case "Stream1Ack":
                    case "Stream2Ack":
                    case "Stream1EmptyAck":
                        handleAckFromWorker(receivedMessage);
                        break;
                }
            }
        }catch(IOException e){
            logger.info("Cannot read from TCP packet\n" + e.getMessage());
        }
    }

    private void initRainStorm(String hydfsSrcFile, String hydfsDestFilename, int numTasks) {
        if(!available) {
            System.out.println("Previous RainStorm application is still being processed");
            return;
        }
        // Clear cache
        available = false;
        availableWorkers.clear();
        op1Workers.clear();
        op2Workers.clear();
        stage2Results.clear();
        partitions.clear();
        timeTable.clear();
        uniqueId = 0;
        stage1Counter = 0;
        stage2Counter = 0;
        completionCount.clear();

        this.hydfsSrcFile = hydfsSrcFile;
        this.hydfsDestFilename = hydfsDestFilename;
        this.numTasks = numTasks;
        initializeAllAvailableMembers();
        System.out.println("Enter command for op1_exe");
        this.op1 = scanner.nextLine();
        System.out.println("Is op1_exe stateful? <true/false>");
        op1Stateful = Boolean.parseBoolean(scanner.nextLine());
        System.out.println("Enter command for op2_exe");
        this.op2 = scanner.nextLine();
        System.out.println("Is op2_exe stateful? <true/false>");
        op2Stateful = Boolean.parseBoolean(scanner.nextLine());
        readingSrcFile = true;
        generateAndPartitionOp1(hydfsSrcFile);
        readingSrcFile = false;
    }

    private void initializeAllAvailableMembers() {
        for (Map.Entry<Integer, Node> member : server.membership.entrySet()) {
            int memberId = member.getKey();
            if (memberId != server.nodeId) {
                availableWorkers.add(memberId);  // memberId = serverId
            }
        }

        int numWorkers = numTasks;
        // Add workers to task1
        for (int i = 0; i < Math.min(numWorkers, availableWorkers.size()); i++) {
            op1Workers.add(availableWorkers.get(i));
        }

        // Add workers for task2
        for (int i = numWorkers; i < availableWorkers.size(); i++) {
            if (op2Workers.size() < numWorkers) {
                op2Workers.add(availableWorkers.get(i));
            }
        }

        // If not enough worker for task2
        if (op2Workers.size() < numWorkers) {
            for (int worker : op1Workers) {
                if (op2Workers.size() < numWorkers) {
                    op2Workers.add(worker);
                } else {
                    break;
                }
            }
        }
        logger.info("Workers assigned to op1: " + op1Workers);
        logger.info("Workers assigned to op2: " + op2Workers);
    }

    public void generateAndPartitionOp1(String sourceFilename) {
        String localFilePath = "rainStorm" + server.nodeId + "/" + sourceFilename;
        server.getFile(sourceFilename, localFilePath);
        try(BufferedReader reader = new BufferedReader(new FileReader(localFilePath))) {
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                if (lineNumber == 1) {
                    lineNumber++;
                    continue;
                }
                String key = sourceFilename + ":" + lineNumber;
                int workerIndex = Math.abs(key.hashCode()) % op1Workers.size();
                KeyValue kv = new KeyValue(key, line, 1, "App" + appUniqueId +
                        "Op1Worker" + workerIndex + ".log");

                // Assign a uniqueId1 and call send partition
                int partitionId = uniqueId++;
                partitions.put(partitionId, kv);
                timeTable.put(partitionId, clock.millis());
                sendStream(partitionId);
                lineNumber++;
            }
            logger.info("Successfully read and send all kvs from source: " + sourceFilename);
        } catch (IOException e) {
            logger.warning("Failed to read file " + sourceFilename + " to generate KVs");
        }
    }


    private void sendStream(int partitionId) {
        String key = partitions.get(partitionId).getKey();
        int stage = partitions.get(partitionId).getStage();
        List<Integer> workers = (stage == 1) ? op1Workers : op2Workers;
        int workerIndex = Math.abs(key.hashCode()) % workers.size();
        int workerId = workers.get(workerIndex);

        while (!server.membership.containsKey(workerId)) {
            reassign(workerId);
            workerId = workers.get(workerIndex);
        };
        String workerIp = server.membership.get(workerId).getIpAddress();
        int workerPort = this.membership.get(workerId);

        // Create a JSON message for the partition
        JSONObject partitionMessage = new JSONObject();
        partitionMessage.put("type", "Stream");
        partitionMessage.put("id", partitionId);
        partitionMessage.put("key", partitions.get(partitionId).getKey());
        partitionMessage.put("value", partitions.get(partitionId).getValue());
        partitionMessage.put("stage", partitions.get(partitionId).getStage());
        if(partitions.get(partitionId).getStage() == 1) {
            partitionMessage.put("op", op1);
            partitionMessage.put("stateful", op1Stateful);
        }else if(partitions.get(partitionId).getStage() == 2) {
            partitionMessage.put("op", op2);
            partitionMessage.put("stateful", op2Stateful);
        }
        partitionMessage.put("destFile", partitions.get(partitionId).getDestFile());

        sendTCP(workerIp, workerPort, partitionMessage);
        logger.info("Partition " + partitionId + " with key '" + partitions.get(partitionId).getKey() + "' sent to worker " + workerId);
    }

    private void reassign(int failedWorkerId) {
        logger.warning("Reassigning failed Worker " + failedWorkerId);
        membership.remove(failedWorkerId);
        availableWorkers.remove((Integer) failedWorkerId);
        int index1 = op1Workers.indexOf(failedWorkerId);
        int index2 = op2Workers.indexOf(failedWorkerId);
        if (index1 != -1 ) {
            int replacedWorkerId = findReplacementWorker(1);
            if(replacedWorkerId != -1) op1Workers.set(index1, replacedWorkerId);
            logger.info("Replaced failed Op1 worker " + failedWorkerId + " with new worker " + replacedWorkerId);
        }
        if (index2 != -1 ) {
            int replacedWorkerId = findReplacementWorker(2);
            if(replacedWorkerId != -1) op2Workers.set(index2, replacedWorkerId);
            logger.info("Replaced failed Op2 worker " + failedWorkerId + " with new worker " + replacedWorkerId);
        }
    }


    /*
     This method is going to check if assigned task timeout
     */
    private void checkTimeTable() {
        long currentTime = clock.millis();
        // Get all timeOut partitions
        for (Map.Entry<Integer, Long> entry : timeTable.entrySet()) {
            int partitionId = entry.getKey();
            long assignedTime = entry.getValue();
            if (currentTime - assignedTime >= 10000) {
                // System.out.println("Resend stream for partitionID = " + partitionId);
                sendStream(partitionId);
                timeTable.put(partitionId, clock.millis());
            }
        }
    }


    /*
     Helper method to find a replacement worker.
     Looks for a worker in the membership list not in op1Workers or op2Workers.
    */
    private int findReplacementWorker(int stage) {
        List<Integer> failedWorkers = stage == 1? op1Workers : op2Workers;
        List<Integer> otherWorkers = stage == 1? op2Workers : op1Workers;
        for (int memberId : server.membership.keySet()) {
            if (server.nodeId != memberId && !op1Workers.contains(memberId) && !op2Workers.contains(memberId)) {
                return memberId;
            }
        }
        for (int workerId : otherWorkers) {
            if (!failedWorkers.contains(workerId)) {
                return workerId;
            }
        }
        logger.warning("Failed to find a replacement worker for stage " + stage);
        return -1;
    }

    /*
     Handle received acknowledge from completed task and remove corresponding task from timeTable
     */
    private void handleAckFromWorker(JSONObject message) {
        while(readingSrcFile) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        int partitionId = message.getInt("id");
        String type = message.getString("type");
        // add to completion count for debugging
        completionCount.put(partitionId, completionCount.getOrDefault(partitionId, 0) + 1);
        if (completionCount.get(partitionId) > 1) {
            System.out.println("!过程发现重复Ack: partitionId = " + partitionId + " Count = " + completionCount.get(partitionId));
            return;
        }

        // System.out.println("Leader从Worker收到了partition ID = " + partitionId);
        // Remove corresponding partition id from timeTable
        if (timeTable.containsKey(partitionId)) {
            timeTable.remove(partitionId);
            logger.info("Task complete for partition " + partitionId + ". Removed from timeout table.");
        }

        if ("Stream1Ack".equals(type)) {  // Not an empty Ack, continue to next stage
            stage1Counter++;
            String newKey = message.getString("key");
            String newValue = message.getString("value");

            // Generate Stage2 task and add to timeTable
            // Send Stage2 task to appropriate worker
            int workerIndex = Math.abs(newKey.hashCode()) % op2Workers.size();
            int newPartitionId = uniqueId++;
            KeyValue kv = new KeyValue(newKey, newValue, 2, "App" + appUniqueId + "Op2Worker" + workerIndex + ".log");
            partitions.put(newPartitionId, kv);
            timeTable.put(newPartitionId, clock.millis());

            sendStream(newPartitionId);

        } else if ("Stream1EmptyAck".equals(type)) {  // An empty Ack from stage1
            stage1Counter++;
        } else if ("Stream2Ack".equals(type)) {  // Stage2 Completed
            stage2Counter++;

            // Write result into stage2Result hashmap
            String finalKey = message.getString("key");
            String finalValue = message.getString("value");
            stage2Results.put(finalKey, finalValue);
            logger.info("Received Stage2 Ack: Key=" + finalKey + ", Value=" + finalValue);
        } else if ("Stream2EmptyAck".equals(type)) {
            stage2Counter++;
        }

        if ((stage1Counter + stage2Counter) % 50 == 0 || stage1Counter + stage2Counter >= uniqueId) {
            System.out.println("Received # Ack = " + (stage1Counter + stage2Counter) + " / " + uniqueId);
        }

        if (stage1Counter + stage2Counter == uniqueId) {
            writeStage2ResultsToFile("rainStorm" + server.nodeId + "/" + hydfsDestFilename);
            logger.info("All RainStorm tasks have been completed.");
            notifyAllWorkersComplete();
            available = true;
            appUniqueId += 1;
            System.out.println("RainStorm processing completed successfully.");
            for (Map.Entry<Integer, Integer> entry : completionCount.entrySet()) {
                if (entry.getValue() > 1) {
                    System.out.println("!发现重复Ack: partitionId = " + entry.getKey() + " Count = " + entry.getValue());
                }
            }

        }
    }

    private void notifyAllWorkersComplete() {
        JSONObject message = new JSONObject();
        message.put("type", "Complete");

        for (Map.Entry<Integer, Integer> entry : this.membership.entrySet()) {
            int workerId = entry.getKey();
            String workerIp = server.membership.get(workerId).getIpAddress();
            int workerPort = entry.getValue();
            sendTCP(workerIp, workerPort, message);
            logger.info("Notified Worker " + workerId + " that all tasks are complete.");
        }
    }

    private void writeStage2ResultsToFile(String filename) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))){
            logger.info("Writing Stage2 Results to local " + filename);
            for (Map.Entry<String, String> entry : stage2Results.entrySet()) {
                writer.write(entry.getKey() + ", " + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            logger.warning("Error occurred when writing Stage2 Results to " + filename);
        }
        server.createFile(filename, hydfsDestFilename);
        logger.info("Writing Stage2 Results to hydfs " + filename);
    }

    private void handleWorkerJoin(JSONObject message) {
        try {
            int workerId = message.getInt("id");
            int workerTCPPort = message.getInt("portTCP");

            if (this.membership.containsKey(workerId)) {
                logger.warning("Worker " + workerId + " already exist as a member");
                return;
            }

            // Add worker to membership list
            this.membership.put(workerId, workerTCPPort);
            logger.info("Worker " + workerId + " joined successfully with TCP port " + workerTCPPort);

            // Reply Ack to joined worker
            JSONObject response = new JSONObject();
            response.put("type", "WorkerJoinAck");
            response.put("leaderPortTCP", this.portTCP);
            String workerIP = server.membership.get(workerId).getIpAddress();
            sendTCP(workerIP, workerTCPPort, response);

        } catch (Exception e) {
            logger.warning("Error handling Worker join: " + e.getMessage());
        }
    }

    /**
     * List all Worker members in the framework
     */
    private void listWorkers() {
        System.out.println("Worker Membership List:");
        if (this.membership.isEmpty()) {
            System.out.println("No Workers have joined yet.");
            return;
        }

        for (Map.Entry<Integer, Integer> entry : this.membership.entrySet()) {
            System.out.println("Worker ID: " + entry.getKey() + ", TCP Port: " + entry.getValue());
        }
    }

    public void listFileLocation(String filename) {
        if (server.fileBlockMap.containsKey(filename)) {
            server.listFileLocation(filename);
        } else {
            System.out.println("File " + filename + " does not exist in HyDFS.");
        }
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

    public static void main(String[] args) {
        try {
            if (args.length < 6) {
                System.out.println("Usage: java leader <nodeId> <IpAddress> <PortTCP> <PortUDP> <CacheSize> <LeaderTCPPort>");
                return;
            }

            int leaderTCPPort = Integer.parseInt(args[5]);
            String[] serverArgs = Arrays.copyOfRange(args, 0, 5);
            Leader leader = new Leader(serverArgs, leaderTCPPort);

            System.out.println("Leader started successfully on TCP port " + leaderTCPPort);
            System.out.println("Server started on FileSystem TCP port " + args[2]);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
