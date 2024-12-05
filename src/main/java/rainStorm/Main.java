package main.java.rainStorm;

import main.java.hydfs.Server;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.*;


/*
HyDFS client-side UI.
Initialize a new node for HyDFS with TCP/UDP monitor and failure detection.
Read in command line inputs and send requests to servers within HyDFS.
 */
public class Main {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        // Read command line input and call corresponding client-side function.
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while(running) {
            System.out.println("Enter command for :");
            String[] command = scanner.nextLine().split(" ");
        }
    }
}

