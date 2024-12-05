package main.java.hydfs;

import java.io.Serializable;


/*
Store basic info for a single device in HyDFS.
 */
public class Node implements Serializable {
    private int nodeId;
    private String ipAddress;
    private int portUDP;
    private int portTCP;
    private String status;

    public Node(int nodeId, String ipAddress, int portUDP, int portTCP, String status) {
        this.nodeId = nodeId;
        this.ipAddress = ipAddress;
        this.portUDP = portUDP;
        this.portTCP = portTCP;
        this.status = status;
    }

    public Node(int nodeId, String ipAddress, int portUDP, int portTCP) {
        this.nodeId = nodeId;
        this.ipAddress = ipAddress;
        this.portUDP = portUDP;
        this.portTCP = portTCP;
        this.status = "alive";
    }

    public String getIpAddress(){
        return ipAddress;
    }

    public int getPortUDP(){
        return portUDP;
    }

    public int getPortTCP(){
        return portTCP;
    }

    public int getNodeId(){
        return nodeId;
    }

    public String getStatus(){
        return status;
    }

    public void setStatus(String newStatus) {
        this.status = newStatus;
    }
}

