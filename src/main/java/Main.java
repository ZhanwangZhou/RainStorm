package main.java;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        Server server = new Server(args);
        new Thread(server::tcpListen).start();

        Scanner scanner = new Scanner(System.in);
        while(true){
            System.out.println("Enter command for node#" + server.nodeId + ": ");
            String command = scanner.nextLine();
            switch (command){
                case "join":
                    System.out.println("Enter introducer IP address and port:");
                    String addressAndPorts = scanner.nextLine();
                    String address = addressAndPorts.split(":")[0];
                    int port = Integer.parseInt(addressAndPorts.split(":")[1]);
                    server.join(address, port);
                    break;
                case "list_mem":
                    server.list();
                    break;
            }
        }
    }
}
