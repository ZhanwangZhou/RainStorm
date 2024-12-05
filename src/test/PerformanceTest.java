package test;

import main.java.hydfs.Server;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class PerformanceTest {
    public Server server;
    public Thread tcpListen;
    public Thread udpListen;
    public ScheduledExecutorService scheduler;

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        String[] newargs = Arrays.copyOfRange(args, 0, args.length - 1);
        PerformanceTest test = new PerformanceTest(newargs);
        test.join();
        switch(args[args.length - 1]){
            case "overheads":
                Scanner scanner = new Scanner(System.in);
                System.out.println("Input number of files to create: ");
                int fileNumber = scanner.nextInt();
                test.multiCreate(fileNumber, "archive/per4/", "per4", "overheads");
                break;
            case "mergeTest":
                for (int clientNumber: Arrays.asList(1, 2, 5, 10)) {
                    System.out.println("----------------------------------------------------");
                    System.out.println("Test merge performance with multi-append from " + clientNumber + " clients");
                    System.out.println("4K per file:");
                    test.testMerge(50, clientNumber, "archive/per4/", "per4",
                            "test4K" + clientNumber + "client.txt");
                    System.out.println("40K per file:");
                    test.testMerge(50, clientNumber, "archive/per40/", "per40",
                            "test40K" + clientNumber + "client.txt");
                }
                break;
        }
    }

    public PerformanceTest(String[] args) throws IOException, NoSuchAlgorithmException {
        this.server = new Server(args);
        this.tcpListen = new Thread(server::tcpListen);
        this.tcpListen.start();
        this.udpListen = new Thread(server::udpListen);
        this.udpListen.start();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.scheduler.scheduleAtFixedRate(server::ping, 0, 1000, TimeUnit.MILLISECONDS);
        this.scheduler.scheduleAtFixedRate(server::checkPing, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void testMerge(int fileNumber, int clientNumber, String filepath, String filenameBase,
                          String hydfsFilename) {
        server.createFile(filepath + filenameBase + "_1.txt", hydfsFilename);
        StringBuilder localFilenames = new StringBuilder();
        for(int i = 2; i <= fileNumber + 1; i++)
            localFilenames.append(filepath).append(filenameBase).append("_").append(i).append(".txt,");
        localFilenames.deleteCharAt(localFilenames.length() - 1);

        StringBuilder nodeIds = new StringBuilder();
        List<Integer> clients = new ArrayList<>();
        for(int i = 1; i <= 10; i++)
            clients.add(i);
        Collections.shuffle(clients);
        for(int i = 0; i < clientNumber; i++)
            nodeIds.append(clients.get(i)).append(",");
        nodeIds.deleteCharAt(nodeIds.length() - 1);

        server.appendMultiFilesAndMerge(hydfsFilename, nodeIds.toString(), localFilenames.toString());
    }

    public void multiCreate(int fileNumber, String filepath, String filenameBase, String hydfsFilename) {
        for(int i = 1; i <= fileNumber; i++) {
            System.out.println(i);
            server.createFile(filepath + filenameBase + "_" + i + ".txt",
                    hydfsFilename + "_" + i + ".txt");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void join() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter introducer IP address and port (<IP Address>:<Port>)");
        String addressAndPorts = scanner.nextLine();
        String address = addressAndPorts.split(":")[0];
        int port = Integer.parseInt(addressAndPorts.split(":")[1]);
        server.join(address, port);
    }

}
