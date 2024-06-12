package org.zrj.rpc;

import java.util.HashMap;
import java.util.Map;

public class Register implements RegisterInterface {
    private final Map<String, Node> nodes;

    public Register() {
        nodes = new HashMap<>();
    }

    @Override
    public void register(Node n) {
        nodes.put(n.getId(), n);
    }

    public Node getNode(String nodeId) {
        if (!nodes.containsKey(nodeId)) {
            throw new RuntimeException(String.format("node %s is not registered", nodeId));
        }
        return nodes.get(nodeId);
    }
}
