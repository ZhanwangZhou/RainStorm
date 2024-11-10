package main.java;

import javax.sound.midi.SysexMessage;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.*;

// javac -d out -cp "lib/json-20240303.jar" $(find src/main/java -name "*.java")
// java -cp "out:lib/json-20240303.jar" main.java.Main 1 127.0.0.1 8080 9090

public class Main {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
        Server server = new Server(args);
        Thread tcpListen = new Thread(server::tcpListen);
        tcpListen.start();
        Thread udpListen = new Thread(server::udpListen);
        udpListen.start();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1000, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1000, 1000, TimeUnit.MILLISECONDS);


        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while(running){
            System.out.println("Enter command for node#" + server.nodeId + ": ");
            String[] command = scanner.nextLine().split(" ");
            switch (command[0]){
                case "join":
                    if (command.length == 2) {
                        String address = command[1].split(":")[0];
                        int port = Integer.parseInt(command[1].split(":")[1]);
                        server.join(address, port);
                    } else {
                        System.out.println("Please specify the command as: join <IP Address>:<Port>");
                    }
                    break;
                case "leave":
                    server.leave();
                    break;
                case "list_mem":
                    server.list_mem();
                    break;
                case "list_self":
                    server.list_self();
                    break;
                case "enable_sus":
                    server.switchMode(true);
                    break;
                case "disable_sus":
                    server.switchMode(false);
                    break;
                case "status_sus":
                    server.statusSus();
                    break;
                case "create":
                    if (command.length == 3) {
                        server.createFile(command[1], command[2]);
                    } else {
                        System.out.println("Please specify the command as: create <Local Filepath> <HyDFS Filename>");
                    }
                    break;
                case "get":
                    if (command.length == 3) {
                        new Thread(() -> server.getFile(command[1], command[2])).start();
                    } else {
                        System.out.println("Please specify the command as: get <HyDFS Filename> <Local Filepath>");
                    }
                    break;
                case "getfromreplica":
                    if (command.length == 4) {
                        server.getFromReplica(command[1], command[2], command[3]);
                    } else {
                        System.out.println("Please specify command as: getfromreplica <VM Address>"
                                + " <HyDFS Filename> <Local Filepath>");
                    }
                    break;
                case "append":
                    if (command.length == 3) {
                        server.appendFile(command[1], command[2]);
                    } else {
                        System.out.println("Please specify the command as: append <Local Filepath> <HyDFS Filename>");
                    }
                    break;
                case "multiappend":
                    System.out.println("Enter HyDFS filename (<hydfsFilename>):");
                    String hydfsFilename = scanner.nextLine();
                    System.out.println("Enter list of nodef IDs to append (<nodeId1>,<nodeId2>,...,<nodeId3>)");
                    String nodeIds = scanner.nextLine();
                    System.out.println("Enter list of local filepath (<localFilePath1>,<localFilePath2>,...,<localFilePath3>)");
                    String localFilenames = scanner.nextLine();
                    server.appendMultiFiles(hydfsFilename, nodeIds, localFilenames);
                    break;
                case "merge":
                    if (command.length == 2) {
                        server.mergeFile(command[1]);
                    } else {
                        System.out.print("Please specify the command as: merge <HyDFS Filename>");
                    }
                    break;
                case "ls":
                    if (command.length == 2) {
                        server.list_file_store(command[1]);
                    } else {
                        System.out.println("Please specify the command as: ls <HyDFS Filename>");
                    }
                    break;
                case "store":
                    server.list_self_store();
                    break;
                case "list_mem_id":
                    server.list_mem_id();
                    break;
                case "quit":
                    running = false;
                    break;
                case "multicreate":
                    if (command.length == 4) {
                        server.multiCreate(Integer.parseInt(command[1]), command[2], command[3]);
                    }
                    break;
                default:
                    System.out.println("Invalid command");
            }
        }
    }
}
