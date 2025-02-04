package main.java.hydfs;
import java.util.HashMap;


/**
 * LRU Cache used to store recently read HyDFS files.
 */
class LRUCache {
    HashMap<String, Node> map = new HashMap<>();
    int capacity;
    Node head, tail;

    public LRUCache(int _capacity) {
        capacity = _capacity;
        head = new Node("headDummy", -1);
        tail = new Node("tailDummy", -1);
        head.next = tail;
        tail.pre = head;
    }

    public int get(String key) {
        if (map.containsKey(key)) {
            Node node = map.get(key);
            cutNode(node);
            insertHead(node);
            return node.val;
        }
        return -1;
    }

    public String put(String key, int value) {
        if (map.containsKey(key)) {
            Node node = map.get(key);
            node.val = value;
            cutNode(node);
            insertHead(node);
            return null;
        } else {
            Node node = new Node(key, value);
            map.put(key, node);
            insertHead(node);
            if (map.size() > capacity) {
                Node delNode = cutTail();
                map.remove(delNode.key);
                return delNode.key;
            }
            return null;
        }
    }

    private void cutNode(Node node) {
        node.pre.next = node.next;
        node.next.pre = node.pre;
        node.pre = null;
        node.next = null;
    }

    private Node cutTail() {
        Node last = tail.pre;
        last.pre.next = tail;
        tail.pre = last.pre;
        last.pre = null;
        last.next = null;
        return last;
    }

    private void insertHead(Node node) {
        Node next = head.next;
        node.next = next;
        node.pre = head;
        next.pre = node;
        head.next = node;
    }

    static class Node {
        String key;
        int val;
        Node next;
        Node pre;
        public Node(String _key, int _val) {
            key = _key;
            val = _val;
        }
    }
}