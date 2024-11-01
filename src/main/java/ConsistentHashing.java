package main.java;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {
    private TreeMap<Long, String> ring;
    private MessageDigest md;

    public ConsistentHashing() throws NoSuchAlgorithmException {
        this.ring = new TreeMap<>();
        this.md = MessageDigest.getInstance("MD5");
    }

    public void addServer(String server) {
        long hash = generateHash(server);
        ring.put(hash, server);
    }

    public void removeServer(String server) {
        long hash = generateHash(server);
        ring.remove(hash);
    }

    public String getServer(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = generateHash(key);
        if (!ring.containsKey(hash)) {
            SortedMap<Long, String> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
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
        ch.addServer("server1");
        ch.addServer("server2");
        ch.addServer("server3");
        ch.addServer("server4");
        ch.addServer("server5");

        System.out.println(ch.ring);
        System.out.println("key1: is present on server: " + ch.getServer("key1"));
        System.out.println("key111: is present on server: " + ch.getServer("key111"));
        System.out.println("key67890: is present on server: " + ch.getServer("key67890"));

        ch.removeServer("server2");
        System.out.println("After removing server2");

        System.out.println("key1: is present on server: " + ch.getServer("key1"));
        System.out.println("key111: is present on server: " + ch.getServer("key111"));
        System.out.println("key67890: is present on server: " + ch.getServer("key67890"));
    }
}
