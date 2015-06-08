package edu.buffalo.cse.cse486586.simpledht;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Created by nikhil on 3/25/15.
 */
public class NodeDetails implements Serializable {

    String successor;
    String predecessor;
    String id;
    String sPort;
    String pPort;
    String msgType = "1";
    String nodeType;
    final String myPort;

    public NodeDetails(String id, String port){
        this.successor = "0";
        this.predecessor = "0";
        this.id = id;
        this.myPort = port;
        this.nodeType = "";
    }
    /*@Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
        successor = (String)input.readObject();
        predecessor = (String)input.readObject();
        id = (String)input.readObject();
        sPort = (String)input.readObject();
        pPort = (String)input.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException {
        output.writeObject(successor);
        output.writeObject(predecessor);
        output.writeObject(id);
        output.writeObject(sPort);
        output.writeObject(pPort);
    }*/

    @Override
    public String toString(){
        return msgType;
    }

    public void copy(NodeDetails nds){
        this.successor = nds.successor;
        this.predecessor = nds.predecessor;
        this.id = nds.id;
        this.sPort = nds.sPort;
        this.pPort = nds.pPort;

        return;
    }
}
