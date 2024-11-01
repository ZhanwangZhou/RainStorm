package main.java;

import java.io.Serializable;

public class Message implements Serializable {
    private String type;
    private String content;
    private Node senderNode;

    public Message(String type, String content, Node senderNode) {
        this.type = type;
        this.content = content;
        this.senderNode = senderNode;
    }

    public String getType() {
        return type;
    }

    public String getContent() {
        return getContent();
    }

    public Node getSenderNode() {
        return senderNode;
    }

    @Override
    public String toString() {
        return "Message{" +
                "type='" + type + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
