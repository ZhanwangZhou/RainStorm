package main.java.rainStorm;


import org.apache.tools.ant.types.Commandline;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;


/*
HyDFS client-side UI.
Initialize a new node for HyDFS with TCP/UDP monitor and failure detection.
Read in command line inputs and send requests to servers within HyDFS.
 */
public class Main {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        List<String> outputs = new ArrayList<>();
        try{
            Runtime rt = Runtime.getRuntime();
            // String escapedValue = value.replaceAll("\"", "\"\"");
            String pattern = "\"Warning\"";
            String value = "-9828558.24604536,4879517. 36898661,29,Stop,\"30\"\"\", ,Unpunched Telespar,2010,Warning, ,R1-1,Champaign,29,,AERIAL,E,,29,";
            // pattern = pattern.replaceAll("\"", "FUCK 425");
            // value = value.replaceAll("\"", "FUCK 425");
            System.out.println(pattern);
            System.out.println(value);
            String args1 = "java bolts/App1Op1 8 \"" + pattern + "\" \"" + value + "\"";
            String[] args2 = Commandline.translateCommandline(args1);
            Process proc = rt.exec(args2);
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));
            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(proc.getErrorStream()));

            // Read the output from the command
            String temp;
            while ((temp = stdInput.readLine()) != null) {
                outputs.add(temp);
            }
            while ((temp = stdError.readLine()) != null) {
                System.out.println(temp);
            }
            System.out.println("SIZE = " + outputs.size());
            for(String output: outputs){
                System.out.println(output);
            }

        }catch (IOException e) {
            System.out.println(e.getMessage());
        }

        /*// Read command line input and call corresponding client-side function.
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while(running) {
            System.out.println("Enter command for :");
            String[] command = scanner.nextLine().split(" ");
        }*/
    }
}

