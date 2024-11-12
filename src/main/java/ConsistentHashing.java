package main.java;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;


/*
Ring data structure holding all nodes in HyDFS.
Use consistent hashing to determine the ring ID of each node and
the node where each file is stored.
 */
public class ConsistentHashing {
    private TreeMap<Long, Integer> ring;
    private MessageDigest md;

    public ConsistentHashing() throws NoSuchAlgorithmException {
        this.ring = new TreeMap<>();
        this.md = MessageDigest.getInstance("MD5");
    }

    public void addServer(int server) {
        long hash = generateHash(String.valueOf(server));
        ring.put(hash, server);
    }

    public void removeServer(int server) {
        long hash = generateHash(String.valueOf(server));
        ring.remove(hash);
    }

    public Integer getServer(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = generateHash(key);
        if (!ring.containsKey(hash)) {
            SortedMap<Long, Integer> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    public int getSuccessor(int server) {
        long hash = generateHash(String.valueOf(server));
        SortedMap<Long, Integer> tailMap = ring.tailMap(hash + 1);
        return tailMap.isEmpty() ?  ring.get(ring.firstKey()) : ring.get(tailMap.firstKey());
    }

    public int getPredecessor(int server) {
        long hash = generateHash(String.valueOf(server));
        SortedMap<Long, Integer> headMap = ring.headMap(hash);
        return headMap.isEmpty() ?  ring.get(ring.lastKey()) : ring.get(headMap.lastKey());
    }

    public int getSuccessor2(int server) {
        int successor1 = getSuccessor(server);
        return getSuccessor(successor1);
    }

    public int getRingId(int server) {
        ArrayList<Integer> valueSet = new ArrayList<>(this.ring.values());
        return valueSet.indexOf(server);
    }

    private long generateHash(String key) {
        md.reset();
        md.update(key.getBytes());
        byte[] digest = md.digest();
        return ((long) (digest[3] & 0xFF) << 24) |
                ((long) (digest[2] & 0xFF) << 16) |
                ((long) (digest[1] & 0xFF) << 8) |
                ((long) (digest[0] & 0xFF));
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ConsistentHashing ch = new ConsistentHashing();
        ch.addServer(1);
        ch.addServer(2);
        ch.addServer(3);
        ch.addServer(4);
        ch.addServer(5);

        System.out.println("successor of 1: " + ch.getSuccessor(1));
        System.out.println("successor of 4: " + ch.getSuccessor(4));
        System.out.println("successor of 3: " + ch.getSuccessor(3));
        System.out.println("successor of 2: " + ch.getSuccessor(2));

        System.out.println("predecessor of 1: " + ch.getPredecessor(1));
        System.out.println("secondSuccessor of 1: " + ch.getSuccessor2(1));

        System.out.println("ring Id of server 1: " + ch.getRingId(1));
        System.out.println("ring Id of server 2: " + ch.getRingId(2));
        System.out.println("ring Id of server 3: " + ch.getRingId(3));
        System.out.println("ring Id of server 4: " + ch.getRingId(4));

        System.out.println(ch.ring);
        System.out.println("key1: is present on server: " + ch.getServer("key1"));
        System.out.println("key111: is present on server: " + ch.getServer("key111"));
        System.out.println("key67890: is present on server: " + ch.getServer("key67890"));
        System.out.println("filename_1: is present on server: " + ch.getServer("filename_1"));
        System.out.println("filename_2: is present on server: " + ch.getServer("filename_2"));


        ch.removeServer(1);
        System.out.println("After removing server1");

        System.out.println("key1: is present on server: " + ch.getServer("key1"));
        System.out.println("key111: is present on server: " + ch.getServer("key111"));
        System.out.println("key67890: is present on server: " + ch.getServer("key67890"));
        System.out.println("filename_1: is present on server: " + ch.getServer("filename_1"));
        System.out.println("filename_2: is present on server: " + ch.getServer("filename_2"));

    }
}
