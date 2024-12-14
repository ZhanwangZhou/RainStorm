package main.java.rainStorm;

import main.java.hydfs.Node;
import main.java.hydfs.Server;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;


public class Leader {

    public Server server;
    public final int portTCP; // the leader's tcp port

    private final HashMap<Integer, Integer> membership; // membership of all workers with port as key
    private final List<Integer> op1Workers; // list of op1 worker ids
    private final List<Integer> op2Workers; // list of op2 worker ids

    private int uniqueId = 0;  // global uniqueId for each stream tuple
    private int appUniqueId = 0; // global uniqueId for each RainStorm app
    private int stage1Counter = 0;  // total number of acknowledged stage1 tasks
    private int stage2Counter = 0;  // total number of acknowledged stage2 tasks
    private final HashMap<Integer, StreamTuple> streamTuples;  // hash map of all stream tuples with id as key
    private final Queue<JSONObject> streamQueue;
    private final HashMap<String, String> finalResults = new HashMap<>(); // final result key-value pairs
    private final ConcurrentHashMap<Integer, Long> timeTable;  // last sent time of each stream tuple

    private String hydfsSrcFilename;
    private String hydfsDestFilename;
    private String op1;
    private boolean op1Stateful;
    private String op2;
    private boolean op2Stateful;

    private boolean available; // if there is any RainStorm task being processed
    private boolean readingFromSrc; // if source file is being read

    private final Clock clock;
    private final Scanner scanner;
    private final Logger logger;

    private final Thread serverTcpListen;
    private final Thread serverUdpListen;
    private final Thread leaderListen;
    private final Thread leaderProcessStream;
    private final ScheduledExecutorService scheduler;


    /**
     * Leader class constructor.
     * @param args Include Argument to initialize leader HyDFS server and leader's TCP port.
     */
    public Leader(String[] args) {
        this.server = new Server(Arrays.copyOfRange(args, 0, 5));
        this.portTCP = Integer.parseInt(args[5]);
        this.membership = new HashMap<>();

        this.op1Workers = new ArrayList<>();
        this.op2Workers = new ArrayList<>();

        this.streamTuples = new HashMap<>();
        this.streamQueue = new LinkedList<>();
        this.timeTable = new ConcurrentHashMap<>();

        this.available = true;
        this.readingFromSrc = false;

        this.clock = Clock.systemDefaultZone();
        this.scanner = new Scanner(System.in);
        this.logger = Logger.getLogger("Leader");
        logger.setLevel(Level.WARNING);

        // Create "rainStorm" + server.nodeId directory if not exist
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
        leaderListen = new Thread(this::tcpListen);
        leaderListen.start();
        leaderProcessStream = new Thread(this::processStream);
        leaderProcessStream.start();

        // Start threads to periodically ping and check for failure detection
        scheduler = Executors.newScheduledThreadPool(3);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::checkTimeTable, 0, 2, TimeUnit.SECONDS);
    }


    /**
     * Leader main function.
     * @param args Include Argument to initialize leader HyDFS server and leader's TCP port.
     */
    public static void main(String[] args) {
        if (args.length != 6) {
            System.out.println("Usage: java Leader <Node ID> <IP Address> <HyDFS TCP Port> "
                    + "<HyDFS UDP Port> <HyDFS Cache Size> <Leader TCP Port>");
            return;
        }
        Leader leader = new Leader(args);
        System.out.println("Succeed to launch leader");
        leader.readCommand();
    }


    /**
     * Continuously read command line user inputs.
     */
    public void readCommand() {
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
                case "append":
                    if (command.length == 3) {
                        server.appendFile(command[1], command[2]);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("append <Local Filepath> <HyDFS Filename>");
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
                case "get":
                    if (command.length == 3) {
                        new Thread(() -> server.getFile(command[1], command[2])).start();
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("get <HyDFS Filename> <Local Filepath>");
                    }
                    break;
                case "ls":
                    if (command.length == 2) {
                        server.listFileLocation(command[1]);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("ls <HyDFS Filename>");
                    }
                    break;
                case "list_mem":
                    server.listMem();
                    break;
                case "list_workers":
                    listWorkers();
                    break;
                case "workers":
                    System.out.println(op1Workers);
                    System.out.println(op2Workers);
                    break;
                case "quit":
                    quit();
            }
        }
    }


    /**
     * Leader TCP listen.
     * Assign different types of incoming messages to their corresponding handle functions.
     */
    public void tcpListen(){
        try (ServerSocket tcpServerSocket = new ServerSocket(portTCP)) {
            while(server.running) {
                Socket tcpSocket = tcpServerSocket.accept();
                tcpSocket.setSoTimeout(5000);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(tcpSocket.getInputStream())
                );
                String jsonString = reader.readLine();
                if (jsonString == null) continue;
                JSONObject receivedMessage = new JSONObject(jsonString);
                String messageType = receivedMessage.getString("type");
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


    /**
     * Continuously track sent time of all stream tuples. Resent if timeout.
     */
    public void checkTimeTable() {
        long currentTime = clock.millis();
        for (Map.Entry<Integer, Long> entry : timeTable.entrySet()) {
            int partitionId = entry.getKey();
            long assignedTime = entry.getValue();
            if (currentTime - assignedTime >= 10000) {
                sendStream(partitionId);
                timeTable.put(partitionId, clock.millis());
            }
        }
    }


    /**
     * Continuously process data stored in streamQueue.
     * Read and store acknowledged results.
     * Create new stream tuples and send to workers.
     */
    public void processStream() {
        while(server.running) {
            if(!streamQueue.isEmpty()) {
                JSONObject currentData = streamQueue.poll();
                if (currentData.getString("type").equals("NewData")) {
                    int lineNumber = currentData.getInt("lineNumber");
                    String line = currentData.getString("line");
                    String key = hydfsSrcFilename + ":" + lineNumber;
                    int workerIndex = Math.abs(key.hashCode()) % op1Workers.size();
                    String destFile = "App" + appUniqueId + "Op1Worker" + workerIndex + ".log";
                    streamTuples.put(uniqueId, new StreamTuple(key, line, 1, destFile));
                    timeTable.put(uniqueId, clock.millis());
                    sendStream(uniqueId);
                    uniqueId++;
                } else if (currentData.getString("type").equals("Stream1Ack")) {
                    String key = currentData.getString("key");
                    String value = currentData.getString("value");
                    int workerIndex = Math.abs(key.hashCode()) % op2Workers.size();
                    String destFile = "App" + appUniqueId + "Op2Worker" + workerIndex + ".log";
                    streamTuples.put(uniqueId, new StreamTuple(key, value, 2, destFile));
                    timeTable.put(uniqueId, clock.millis());
                    sendStream(uniqueId);
                    stage1Counter++;
                    uniqueId++;
                } else if (currentData.getString("type").equals("Stream1EmptyAck")) {
                    stage1Counter++;
                } else if (currentData.getString("type").equals("Stream2Ack")) {
                    finalResults.put(currentData.getString("key"), currentData.getString("value"));
                    stage2Counter++;
                } else if (currentData.getString("type").equals("Stream2EmptyAck")) {
                    stage2Counter++;
                }
                int totalAcked = stage1Counter + stage2Counter;
                if (totalAcked > 0 && totalAcked % 50 == 0
                        && !currentData.getString("type").equals("NewData")) {
                    System.out.println("Received # Ack = " + totalAcked + " / " + uniqueId);
                }
                if (totalAcked > 0 && !readingFromSrc && totalAcked == uniqueId) {
                    completeStream();
                }
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    /**
     * Initiate a new RainStorm application.
     * @param hydfsSrcFile HyDFS filename of RainStorm application source file.
     * @param hydfsDestFilename HyDFS filename of RainStorm application destination file.
     * @param numTasks The number of tasks to run per stage of the application
     */
    public void initRainStorm(String hydfsSrcFile, String hydfsDestFilename, int numTasks) {
        if(!available) {
            System.out.println("Previous RainStorm application is still being processed");
            return;
        }
        available = false;

        // Read command line inputs for app operations
        this.hydfsSrcFilename = hydfsSrcFile;
        this.hydfsDestFilename = hydfsDestFilename;
        System.out.println("Enter command for op1_exe");
        this.op1 = scanner.nextLine();
        System.out.println("Is op1_exe stateful? <true/false>");
        op1Stateful = Boolean.parseBoolean(scanner.nextLine());
        System.out.println("Enter command for op2_exe");
        this.op2 = scanner.nextLine();
        System.out.println("Is op2_exe stateful? <true/false>");
        op2Stateful = Boolean.parseBoolean(scanner.nextLine());

        // Assign workers to each operation
        op1Workers.clear();
        op2Workers.clear();
        List<Integer> availableWorkers = new ArrayList<>();
        for (int memberId : server.getMembership().keySet()) {
            if (memberId != server.nodeId) {
                availableWorkers.add(memberId);
            }
        }
        int numWorkers = Math.min(numTasks, availableWorkers.size());
        for (int i = 0; i < numWorkers; i++) {
            op1Workers.add(availableWorkers.get(i));
        }
        logger.info("Workers assigned to op1: " + op1Workers);
        for (int i = numWorkers; i < numWorkers * 2; i ++) {
            op2Workers.add(availableWorkers.get(i % availableWorkers.size()));
        }
        logger.info("Workers assigned to op2: " + op2Workers);

        // Start a thread to read from the source file
        new Thread(this::readFromSrcFile).start();
    }


    /**
     * List all Worker members
     */
    public void listWorkers() {
        System.out.println("Worker Membership List:");
        if (this.membership.isEmpty()) {
            System.out.println("No Workers have joined yet.");
        } else {
            for (Map.Entry<Integer, Integer> entry : this.membership.entrySet()) {
                String ops = "";
                if(op1Workers.contains(entry.getKey())) ops += " Op1";
                if(op2Workers.contains(entry.getKey())) ops += " Op2";
                if (ops.isEmpty()) {
                    System.out.println("Worker ID: " + entry.getKey() + ", TCP Port: " + entry.getValue());
                } else {
                    System.out.println("Worker ID: " + entry.getKey() + ", TCP Port: " + entry.getValue()
                            + " Processing" + ops);
                }
            }
        }
    }


    /**
     * Send "Quit" messages to stop all workers.
     * Kill all threads and exit the program.
     */
    public void quit() {
        JSONObject quitMessage = new JSONObject();
        quitMessage.put("type", "Quit");
        for (Integer memberId : this.membership.keySet()) {
            Node receiver = server.getMember(memberId);
            server.sendTCP(receiver.getIpAddress(), this.membership.get(memberId), quitMessage);
        }
        serverTcpListen.interrupt();
        serverUdpListen.interrupt();
        leaderListen.interrupt();
        leaderProcessStream.interrupt();
        scheduler.shutdownNow();
        System.exit(0);
    }


    /**
     * Handle "WorkerJoin" message.
     * Add the worker to membership and send "WorkerJoinAck" message to the worker.
     * @param message Message received at the leader's TCP port
     */
    private void handleWorkerJoin(JSONObject message) {
        int workerId = message.getInt("id");
        int workerTCPPort = message.getInt("portTCP");
        this.membership.put(workerId, workerTCPPort);
        JSONObject response = new JSONObject();
        response.put("type", "WorkerJoinAck");
        response.put("leaderPortTCP", this.portTCP);
        String workerIP = server.getMembership().get(workerId).getIpAddress();
        server.sendTCP(workerIP, workerTCPPort, response);
        logger.info("Worker " + workerId + " joined successfully with TCP port " + workerTCPPort);
    }


    /**
     * Handle message of acknowledged stream tuple
     * Add the message to streamQueue and remove the stream tuple from timeTable
     * @param message Message received at the leader's TCP port
     */
    private void handleAckFromWorker(JSONObject message) {
        int streamTupleId = message.getInt("id");
        if (timeTable.containsKey(streamTupleId)) {
            timeTable.remove(streamTupleId);
            logger.info("Received acknowledgement for stream tuple " + streamTupleId);
        }
        streamQueue.add(message);
    }


    /**
     * Continuously read content from source file and append to streamQueue
     */
    private void readFromSrcFile() {
        readingFromSrc = true;
        String localFilePath = "rainStorm" + server.nodeId + "/" + hydfsSrcFilename;
        server.getFile(hydfsSrcFilename, localFilePath);
        try(BufferedReader reader = new BufferedReader(new FileReader(localFilePath))) {
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                JSONObject newData = new JSONObject();
                newData.put("type", "NewData");
                newData.put("lineNumber", lineNumber);
                newData.put("line", line);
                streamQueue.add(newData);
                lineNumber++;
                Thread.sleep(20);
            }
        } catch (IOException e) {
            logger.warning("Failed to read file " + hydfsSrcFilename);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        readingFromSrc = false;
        logger.info("Successfully read all contents from source: " + hydfsSrcFilename);
    }


    /**
     * Create message for the stream tuple and send the message to the assigned worker
     * @param streamTupleId Unique ID of the stream tuple
     */
    private void sendStream(int streamTupleId) {
        StreamTuple streamTuple = streamTuples.get(streamTupleId);
        String key = streamTuple.getKey();
        int stage = streamTuple.getStage();

        // Create message for the stream tuple
        JSONObject streamMessage = new JSONObject();
        streamMessage.put("type", "Stream");
        streamMessage.put("id", streamTupleId);
        streamMessage.put("key", key);
        streamMessage.put("value", streamTuple.getValue());
        streamMessage.put("stage", stage);
        if(stage == 1) {
            streamMessage.put("op", op1);
            streamMessage.put("stateful", op1Stateful);
        }else if(stage == 2) {
            streamMessage.put("op", op2);
            streamMessage.put("stateful", op2Stateful);
        }
        streamMessage.put("destFile", streamTuple.getDestFile());

        // Determine which worker to process the stream tuple
        List<Integer> workers = (stage == 1) ? op1Workers : op2Workers;
        int workerIndex = Math.abs(key.hashCode()) % workers.size();
        int workerId = workers.get(workerIndex);

        // Reassign worker if the target worker does not exist
        while (!server.getMembership().containsKey(workerId)) {
            reassign(workerId);
            workerId = workers.get(workerIndex);
        }
        server.sendTCP(server.getMember(workerId).getIpAddress(), membership.get(workerId), streamMessage);
        logger.info("Sent stream tuple " + streamTupleId + " to worker " + workerId);
    }


    /**
     * Called on the completion of a RainStorm application
     * Inform workers, write final results, and reset variables.
     */
    private void completeStream() {
        logger.info("All RainStorm data have been processed.");

        // Inform all workers of the completion
        JSONObject message = new JSONObject();
        message.put("type", "Complete");
        for (Map.Entry<Integer, Integer> entry : this.membership.entrySet()) {
            server.sendTCP(server.getMember(entry.getKey()).getIpAddress(), entry.getValue(), message);
        }

        // Write final results to both local and HyDFS
        String localDestFilePath = "rainStorm" + server.nodeId + "/" + hydfsDestFilename;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(localDestFilePath))){
            for (Map.Entry<String, String> entry : finalResults.entrySet()) {
                writer.write(entry.getKey() + ", " + entry.getValue());
                writer.newLine();
            }
            logger.info("Succeed to write final results to local " + localDestFilePath);
        } catch (IOException e) {
            logger.warning("Failed to write final results to local: " + e.getMessage());
        }
        server.createFile(localDestFilePath, hydfsDestFilename);
        logger.info("Succeed to write final results to HyDFS " + hydfsDestFilename);

        // Reset variables
        uniqueId = 0;
        stage1Counter = 0;
        stage2Counter = 0;
        appUniqueId++;
        finalResults.clear();
        streamTuples.clear();
        timeTable.clear();
        available = true;
        System.out.println("RainStorm complete successfully");
    }


    /**
     * Reassign by replacing the failed worker with an alive worker for each stage
     * @param failedWorkerId Server node ID of the failed worker
     */
    private void reassign(int failedWorkerId) {
        logger.warning("Reassigning failed Worker " + failedWorkerId);
        membership.remove(failedWorkerId);
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


    /**
     * Helper method to find a new worker for a stage.
     * @param stage The stage of the operation. Either 1 or 2.
     * @return Server node ID of the new worker. -1 if no available worker.
     */
    private int findReplacementWorker(int stage) {
        List<Integer> failedWorkers = stage == 1? op1Workers : op2Workers;
        List<Integer> otherWorkers = stage == 1? op2Workers : op1Workers;
        for (int memberId : server.getMembership().keySet()) {
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

}
