package main.java;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class MembershipManager {
    private HashMap<Integer, Node> membership;
    private Logger logger;

    public MembershipManager() {
        this.membership = new HashMap<>();
        this.logger = Logger.getLogger("MembershipManager");
    }

    public void addNode(Node node) {
        if (!membership.containsKey(node.getNodeId())) {
            membership.put(node.getNodeId(), node);
            logger.info("Node added: " + node.getNodeId());
        } else {
            logger.warning("Adding failed since node already exists: " + node.getNodeId());
        }
    }

    public void removeNode(int nodeId) {
        if (membership.containsKey(nodeId)) {
            membership.remove(nodeId);
            logger.info("Node removed: " + nodeId);
        } else {
            logger.warning("Removing failed since node does not exist: " + nodeId);
        }
    }

    public void clear(){
        membership.clear();
        logger.info("Cleared MembershipManager");
    }

    public int getSize(){
        return membership.size();
    }

    public Node getNode(int nodeId) {
        return membership.get(nodeId);
    }

    public boolean containsNode(int nodeId) {
        return membership.containsKey(nodeId);
    }

    public Map<Integer, Node> getMembers() {
        return membership;
    }

    public void printMembership() {
        logger.info("Current membership:");
        membership.forEach((id, node) -> logger.info(id + ": " + node));
    }
}
