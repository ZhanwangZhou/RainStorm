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
        new Thread(server::tcpListen).start();
        new Thread(server::udpListen).start();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(server::ping, 0, 1000, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(server::checkPing, 1000, 1000, TimeUnit.MILLISECONDS);


        Scanner scanner = new Scanner(System.in);
        while(true){
            System.out.println("Enter command for node#" + server.nodeId + ": ");
            String command = scanner.nextLine();
            String localFilePath;
            String hydfsFilename;
            switch (command.split(" ")[0]){
                case "join":
                    System.out.println("Enter introducer IP address and port:");
                    String addressAndPorts = scanner.nextLine();
                    String address = addressAndPorts.split(":")[0];
                    int port = Integer.parseInt(addressAndPorts.split(":")[1]);
                    server.join(address, port);
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
                case "create":
                    System.out.println("Enter local filepath");
                    localFilePath = scanner.nextLine();
                    System.out.println("Enter HyDFS filename");
                    hydfsFilename = scanner.nextLine();
                    server.createFile(localFilePath, hydfsFilename);
                    break;
                case "get":
                    System.out.println("Enter HyDFS filename");
                    hydfsFilename = scanner.nextLine();
                    System.out.println("Enter local filepath");
                    localFilePath = scanner.nextLine();
                    new Thread(() -> server.getFile(hydfsFilename, localFilePath)).start();
                    break;
                case "append":
                    System.out.println("Enter local filepath");
                    localFilePath = scanner.nextLine();
                    System.out.println("Enter HyDFS filename");
                    hydfsFilename = scanner.nextLine();
                    server.appendFile(localFilePath, hydfsFilename);
                    break;
                case "ls":
                    server.list_file_store(command.split(" ")[1]);
                    break;
                case "store":
                    server.list_self_store();
                    break;
                case "list_mem_id":
                    server.list_mem_id();
                    break;
                default:
                    System.out.println("Invalid command");
            }
        }
    }
}
