package main.java.rainStorm;

import main.java.hydfs.ConsistentHashing;
import main.java.hydfs.Node;
import main.java.hydfs.Server;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.*;

public class Leader {
    public Server server;
    final int portTCP; // self node's tcp port
    final HashMap<Integer, Integer> membership; // membership lists store all worker tcp ports

    private final List<Integer> op1Workers;
    private final List<Integer> op2Workers;
    private List<Integer> availableWorkers;

    private final Logger logger = Logger.getLogger(Leader.class.getName());

    final Clock clock;
    private HashMap<Integer, Long> timeTable;  // key = uniqueId1, value = createdTime
    private HashMap<Integer, KeyValue> partitions;  // key=uniqueId1, value=KeyValue={filename:lineNumber, line}
    private int uniqueId1 = 0;  // Global uniqueId1 for Op1
    private int uniqueId2 = 0; // Global uniqueId2 for Op2

    public String hydfsSrcFile;
    public String hydfsDestFilename;
    int numTasks;


    final ServerSocket tcpServerSocket;

    public Leader(String[] args, int portTCP) throws IOException, NoSuchAlgorithmException {
        this.clock = Clock.systemDefaultZone();
        this.portTCP = portTCP;
        this.membership = new HashMap<>();
        this.partitions = new HashMap<>();
        this.timeTable = new HashMap<>();
        this.tcpServerSocket = new ServerSocket(this.portTCP);
        this.op1Workers = new ArrayList<>();
        this.op2Workers = new ArrayList<>();
        this.availableWorkers = new ArrayList<>();

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
        scheduler.scheduleAtFixedRate(this::checkTimeTable, 0, 3, TimeUnit.SECONDS);


        Scanner scanner = new Scanner(System.in);
        while (server.running) {
            System.out.println("Enter command for Leader:");
            String[] command = scanner.nextLine().split(" ");
            if(command.length == 4 && command[0].equals("RainStorm")) {
                hydfsSrcFile = command[1];
                hydfsDestFilename = command[2];
                numTasks = Integer.parseInt(command[3]);
                initializeAllAvailableMembers();
                System.out.println("Enter command for op1_exe");
                String op1 = scanner.nextLine();
                System.out.println("Enter command for op2_exe");
                String op2 = scanner.nextLine();

            } else {
                System.out.println("Please specify the command as:");
                System.out.println("RainStorm <hydfs_src_file> <hydfs_dest_filename> <num_tasks");
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
            }
        }catch(IOException e){
            logger.info("Cannot read from TCP packet\n" + e.getMessage());
        }
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
        try(BufferedReader reader = new BufferedReader(new FileReader(sourceFilename))) {
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                String key = sourceFilename + ":" + lineNumber;
                String value = line;
                KeyValue kv = new KeyValue(key, value);

                // Assign a uniqueId1 and call send partition
                int partitionId = uniqueId1++;
                partitions.put(partitionId, kv);
                timeTable.put(partitionId, clock.millis());
                sendPartitionByKey(partitionId, key, kv, op1Workers);
            }
            logger.info("Successfully read and send all kvs from source: " + sourceFilename);
        } catch (IOException e) {
            logger.warning("Failed to read file " + sourceFilename + " to generate KVs");
        }
    }


    private void sendPartitionByKey(int partitionId, String key, KeyValue partition, List<Integer> targetWorkers) {
        if (targetWorkers.isEmpty()) {
            logger.warning("No workers available to assign partition " + partitionId);
            return;
        }

        int workerIndex = Math.abs(key.hashCode()) % targetWorkers.size();
        int workerId = targetWorkers.get(workerIndex);
        String workerIp = server.membership.get(workerId).getIpAddress();
        int workerPort = this.membership.get(workerId);

        // Create a JSON message for the partition
        JSONObject partitionMessage = new JSONObject();
        partitionMessage.put("type", "Stream1");
        partitionMessage.put("id", partitionId);
        partitionMessage.put("key", partition.getKey());
        partitionMessage.put("value", partition.getValue());
        partitionMessage.put("op1", "#TODO");
        partitionMessage.put("op2", "#TODO");
        partitionMessage.put("destFile", "#TODO");

        sendTCP(workerIp, workerPort, partitionMessage);
        logger.info("Partition " + partitionId + " with key '" + key + "' sent to worker " + workerId);
    }


//    /*
//     Convert each line of sourceFile to be a list of KeyValue pair for stream processing
//     */
//    public List<KeyValue> generateKV(String sourceFilename) {
//        List<KeyValue> keyValueList = new ArrayList<>();
//        try(BufferedReader reader = new BufferedReader(new FileReader(sourceFilename))) {
//            String line;
//            int lineNumber = 1;
//            while ((line = reader.readLine()) != null) {
//                String key = sourceFilename + ":" + lineNumber;
//                String value = line;
//                keyValueList.add(new KeyValue(key, value));
//            }
//            logger.info("Successfully read " + sourceFilename + " from " + sourceFilename + " and generated KVs");
//        } catch (IOException e) {
//            logger.warning("Failed to read file " + sourceFilename + " to generate KVs");
//        }
//        return keyValueList;
//    }

//    /*
//     Assigned each generated KV pair an uniqueId and call sendPartition to send to worker
//     */
//    public void partitioning(List<KeyValue> keyValueList){
//        // Loop through all generated KVs and assign an uniqueId to it
//        for(KeyValue kv : keyValueList){
//            partitions.put(uniqueId++, kv);
//        }
//
//        logger.info("UniqueId assignment completed. Total partitions: " + partitions.size());
//        for (Map.Entry<Integer, KeyValue> entry : partitions.entrySet()) {
//            sendPartition(entry.getKey(), entry.getValue());
//        }
//        logger.info("Partitioning completed. Total partitions: " + partitions.size());
//    }
//    public void sendPartitionForOp1(int partitionId, KeyValue partition) {
//        List<Integer> allworkers = new ArrayList<>();
//        for (Map.Entry<Integer, Node> entry : server.membership.entrySet()) {
//            int nodeId = entry.getKey();
//            if (nodeId != this.server.nodeId) {
//                allworkers.add(nodeId);
//            }
//        }
//
//        if (allworkers.isEmpty()) {
//            logger.warning("No workers available to assign partition " + partitionId + " for op1");
//            return;
//        }
//
//        int size  = Math.max(allworkers.size(), 3); // TODO: hardcoded num_tasks for now
//
//
//        int workerIndex = partitionId % size;
//        int workerId = allworkers.get(workerIndex);
//        op1UsedWorkers.add(workerId);
//        sendPartitionToWorker(workerId, partitionId, partition);
//    }


//    public void sendPartitionForOp2(int partitionId, KeyValue partition) {
//        HashSet<Integer> allWorkers = new HashSet<>();
//        for (Map.Entry<Integer, Node> entry : server.membership.entrySet()) {
//            int nodeId = entry.getKey();
//            if (nodeId != this.server.nodeId) {
//                allWorkers.add(nodeId);
//            }
//        }
//
//        List<Integer> unusedWorkers = new ArrayList<>(allWorkers);
//        unusedWorkers.removeAll(op1UsedWorkers);
//
//        List<Integer> selectedWorkers = new ArrayList<>(unusedWorkers);
//        if (selectedWorkers.size() < 3) {  // TODO: hardcoded num_tasks for now
//            List<Integer> usedWorkers = new ArrayList<>(op1UsedWorkers);
//            int neededWorkers = 3 - selectedWorkers.size();
//            selectedWorkers.addAll(usedWorkers.subList(0, Math.min(neededWorkers, usedWorkers.size())));
//        }
//
//        if (selectedWorkers.isEmpty()) {
//            logger.warning("No workers available to assign partition " + partitionId + " for op2");
//            return;
//        }
//
//        int workerIndex = partitionId % selectedWorkers.size();
//        int workerId = selectedWorkers.get(workerIndex);
//
//        sendPartitionToWorker(workerId, partitionId, partition);
//    }


//    /*
//     Send partitioned (key=uniqueId, KV={filename:lineNumber, line}) to worker
//     Port: New TCP port specified for each worker
//     */
//    public void sendPartition(int partitionId, KeyValue partition) {
//        JSONObject partitionMessage = new JSONObject();
//        partitionMessage.put("type", "NewPartition");
//        partitionMessage.put("partitionId", partitionId);
//        partitionMessage.put("partitionKey", partition.getKey());
//        partitionMessage.put("partitionValue", partition.getValue());
//        List<Integer> selectedWorkers = getWorkersForTasks();
//        if (selectedWorkers.isEmpty()) {
//            logger.warning("No workers available to assign partition " + partitionId);
//            return;
//        }
//
//        int workerIndex = partitionId % selectedWorkers.size();
//        int workerId = selectedWorkers.get(workerIndex);
//        String workerIp = server.membership.get(workerId).getIpAddress();
//
//        sendTCP(workerIp, this.membership.get(workerId), partitionMessage);
//    }

//    private List<Integer> getWorkersForTasks() {
//        List<Integer> workerIds = new ArrayList<>();
//        for (Map.Entry<Integer, Integer> entry : membership.entrySet()) {
//            int memberId = entry.getKey();
//            // int memberPort = entry.getValue();
//            if (memberId != this.server.nodeId) {  // ignore the leader node
//                workerIds.add(memberId);
//            }
//        }
//
//        // 只保留最多 num_tasks 个 Worker => hardcode = 3 for now
//        int numTasks = 3;
//        return workerIds.subList(0, numTasks);
//    }

    /*
     This method is going to check if assigned task timeout
     */
    private void checkTimeTable() {

        long currentTime = clock.millis();
        for(Integer key: timeTable.keySet()){
            if(currentTime - timeTable.get(key) >= 5000){
                timeTable.remove(key);
                // Check if failure occur
                String hashKey = partitions.get(key).getKey();
                int failedWorkerIndex = Math.abs(hashKey.hashCode()) % op1Workers.size();
                int failedWorkerId = op1Workers.get(failedWorkerIndex);

                // If the failed worker is no longer in membership
                if (!server.membership.containsKey(failedWorkerId)) {
                    logger.warning("Worker " + failedWorkerId + " failed. Reassigning task for partition " + key);

                    int newWorkerId = findReplacementWorker();

                    if (newWorkerId != -1) {
                        // Replace failed worker with new worker
                        op1Workers.set(failedWorkerIndex, newWorkerId);
                        logger.info("Replaced failed worker " + failedWorkerId + " with new worker " + newWorkerId);
                    } else {
                        // Remove the failed worker from op1Workers
                        logger.warning("No replacement worker found. Removing failed worker " + failedWorkerId);
                        op1Workers.remove(failedWorkerIndex);
                        availableWorkers.remove(failedWorkerIndex);
                    }
                }
                // Resend the partition task
                sendPartitionByKey(key, partitions.get(key).getKey(), partitions.get(key), op1Workers);
                timeTable.put(key, clock.millis());
            }
        }
    }

    /*
     Helper method to find a replacement worker.
     Looks for a worker in the membership list not in op1Workers or op2Workers.
    */
    private int findReplacementWorker() {
        for (Map.Entry<Integer, Node> member : server.membership.entrySet()) {
            int memberId = member.getKey();
            if (!op1Workers.contains(memberId) && !op2Workers.contains(memberId)) {
                return memberId;
            }
        }
        return -1;
    }

    /*
     Handle received acknowledge from completed task and remove corresponding task from timeTable
     */
    private void handleAckFromWorker(JSONObject message) {
        try {
            int key = message.getInt("key");  // UniqueId of completed task
            if (timeTable.containsKey(key)) {
                timeTable.remove(key);
                logger.info("Task complete for partition " + key + ". Removed from timeout table.");
            } else {
                logger.warning("Received Task Complete for unknown partition " + key);
            }
        } catch (Exception e) {
            logger.warning("Error occurred when handling Ack from worker");
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
}
