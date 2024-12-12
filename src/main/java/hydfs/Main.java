package main.java.hydfs;

import java.util.Scanner;
import java.util.concurrent.*;


/*
HyDFS client-side UI.
Initialize a new node for HyDFS with TCP/UDP monitor and failure detection.
Read in command line inputs and send requests to servers within HyDFS.
 */
public class Main {
    public static void main(String[] args) {
        // Start threads to listen to TCP/UDP messages
        Server server = new Server(args);
        Thread tcpListen = new Thread(server::tcpListen);
        tcpListen.start();
        Thread udpListen = new Thread(server::udpListen);
        udpListen.start();
        // Start threads to periodically ping and check for failure detection
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1, 1, TimeUnit.SECONDS);
        // Read command line input and call corresponding client-side function.
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
                        System.out.println("Please specify the command as:");
                        System.out.println("join <IP Address>:<Port>");
                    }
                    break;
                case "leave":
                    server.leave();
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
                case "getfromreplica":
                    if (command.length == 4) {
                        server.getFromReplica(command[1], command[2], command[3]);
                    } else {
                        System.out.println("Please specify command as:");
                        System.out.println("getfromreplica <IP Address>:<Port>"
                                + " <HyDFS Filename> <Local Filepath>");
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
                case "multiappend":
                    System.out.println("Enter HyDFS filename (<hydfsFilename>):");
                    String hydfsFilename = scanner.nextLine();
                    System.out.println("Enter list of nodef IDs to append "
                            + "(<nodeId1>,<nodeId2>,...,<nodeId3>)");
                    String nodeIds = scanner.nextLine();
                    System.out.println("Enter list of local filepath "
                            + "(<localFilePath1>,<localFilePath2>,...,<localFilePath3>)");
                    String localFilenames = scanner.nextLine();
                    server.appendMultiFiles(hydfsFilename, nodeIds, localFilenames);
                    break;
                case "merge":
                    if (command.length == 2) {
                        server.mergeFile(command[1]);
                    } else {
                        System.out.print("Please specify the command as:");
                        System.out.println("merge <HyDFS Filename>");
                    }
                    break;
                case "store":
                    server.listSelfStorage();
                    break;
                case "ls":
                    if (command.length == 2) {
                        server.listFileLocation(command[1]);
                    } else {
                        System.out.println("Please specify the command as:");
                        System.out.println("ls <HyDFS Filename>");
                    }
                    break;
                case "list_self":
                    server.listSelf();
                    break;
                case "list_mem":
                    server.listMem();
                    break;
                case "list_mem_ids":
                    server.listMemRingIds();
                    break;
                case "status_sus":
                    server.statusSus();
                    break;
                case "enable_sus":
                    server.switchMode(true);
                    break;
                case "disable_sus":
                    server.switchMode(false);
                    break;
                case "quit":
                    running = false;
                    break;
                default:
                    System.out.println("Invalid command");
            }
        }
        tcpListen.interrupt();
        udpListen.interrupt();
        scheduler.shutdownNow();
        System.exit(0);
    }
}
