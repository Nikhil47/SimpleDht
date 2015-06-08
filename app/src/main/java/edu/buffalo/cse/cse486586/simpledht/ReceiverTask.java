package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by nikhil on 3/25/15.
 * Handle New join requests
 * Maintain successor Predecessor values here
 * If New joining ID greater than own ID forward request to successor if present
 */
public class ReceiverTask implements Runnable {

    Socket sock;
    SimpleDhtProvider sdp;
    ObjectInputStream read;

    public ReceiverTask(Socket sock, SimpleDhtProvider sdp){
        this.sock = sock;
        this.sdp = sdp;
    }

    @Override
    public void run() {
        try {

            read = new ObjectInputStream(sock.getInputStream());
            Log.d("Debug:", "OIS Crossed");
            //while (true) {

                Object inp = read.readObject();
                Log.d("Debug:", "Crossed");

                switch (Integer.parseInt(inp.toString())) {
                    case 1: {

                        NodeDetails details = (NodeDetails) inp;
                        Log.d("Debug:", details.id);
                        //If below condition is true then this is the first object
                        if (details.id.equals("5554")) {
                            sdp.details.predecessor = TiredOfClasses.genHash(sdp.portStr);
                            sdp.details.successor = TiredOfClasses.genHash(sdp.portStr);
                            sdp.details.sPort = sdp.myPort;
                            sdp.details.pPort = sdp.myPort;
                            sdp.details.nodeType = "head";

                            //out.writeObject(details);
                            sdp.details.msgType = "2";
                            sdp.sockConn = new SocketConnection(sdp.details);
                            Log.d("Debug:", "this is the first object");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }
                        //if the id is greater than myid, myID is greater equal to successor id
                        //Appending to the head of the linked list
                        else if(TiredOfClasses.genHash(details.id).compareTo(TiredOfClasses.genHash(sdp.portStr)) > 0
                                && TiredOfClasses.genHash(sdp.details.id).compareTo(sdp.details.successor) >= 0
                                /*&& sdp.details.successor.compareTo(TiredOfClasses.genHash("11108")) == 0
                                && sdp.details.predecessor.compareTo(TiredOfClasses.genHash("11108")) == 0*/){

                            //Update new node's predecessor to self

                            //Cursor c = sdp.queryDelete(TiredOfClasses.genHash(details.id));

                            if(TiredOfClasses.genHash(sdp.details.id).compareTo(sdp.details.successor) == 0) {
                                details.predecessor = sdp.details.predecessor;
                                details.pPort = sdp.details.pPort;

                                sdp.details.predecessor = TiredOfClasses.genHash(details.id);
                                sdp.details.pPort = details.myPort;
                            }
                            else{
                                details.predecessor = TiredOfClasses.genHash(sdp.details.id);
                                details.pPort = sdp.myPort;
                            }

                            //Update new node's successor to my id
                            details.successor = sdp.details.successor;
                            sdp.details.successor = TiredOfClasses.genHash(details.id);
                            details.sPort = sdp.details.sPort;
                            sdp.details.sPort = details.myPort;

                            //Update forward connections to the next link and this link will be used by the successor link for backward communications
                            sdp.sockConn.updateConnections(sdp.details);
                            details.msgType = "2";
                            sdp.sockConn.writeForward(details);

                            Log.d("Debug:", "if the id is greater than successor and current node is its own successor");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }
                        //If new node ID is less than my id and I am my successor
                        else if(TiredOfClasses.genHash(details.id).compareTo(TiredOfClasses.genHash(sdp.portStr)) < 0
                                && TiredOfClasses.genHash(sdp.details.id).compareTo(sdp.details.successor) == 0){
                            details.msgType = "3";
                            details.nodeType = "head";
                            sdp.details.nodeType = "";

                            details.predecessor = sdp.details.predecessor;
                            sdp.details.predecessor = TiredOfClasses.genHash(details.id);

                            details.pPort = sdp.details.pPort;
                            sdp.details.pPort = details.myPort;

                            details.sPort = sdp.myPort;
                            details.successor = TiredOfClasses.genHash(sdp.portStr);

                            sdp.details.successor = TiredOfClasses.genHash(details.id);
                            sdp.details.sPort = details.myPort;


                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeBackward(details);

                            Log.d("Debug:", "If new id is less than me and I am my successor");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }
                        //If new node id is greater than myID and greater than my successor, and i am smaller than my successor
                        else if((TiredOfClasses.genHash(details.id).compareTo(TiredOfClasses.genHash(sdp.portStr)) > 0
                                && sdp.details.successor.compareTo(TiredOfClasses.genHash(details.id)) < 0
                                && TiredOfClasses.genHash(sdp.details.id).compareTo(sdp.details.successor) < 0)
                                ){

                            //Then do nothing, just forward the request to next node
                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeForward(details);
                            Log.d("Debug:", "If new node id is greater than myID and greater than my successor, moving on");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }
                        //If new node id is greater than myID but less than my successor, and i am smaller than my successor
                        else if(TiredOfClasses.genHash(details.id).compareTo(TiredOfClasses.genHash(sdp.portStr)) > 0
                                && sdp.details.successor.compareTo(TiredOfClasses.genHash(details.id)) > 0
                                && TiredOfClasses.genHash(sdp.details.id).compareTo(sdp.details.successor) < 0){

                            //Then this node has to be inserted
                            details.successor = sdp.details.successor;
                            sdp.details.successor = TiredOfClasses.genHash(details.id);
                            details.sPort = sdp.details.sPort;
                            sdp.details.sPort = details.myPort;

                            details.predecessor = TiredOfClasses.genHash(sdp.details.id);
                            details.pPort = sdp.details.myPort;

                            sdp.sockConn.updateConnections(sdp.details);
                            details.msgType = "2";
                            sdp.sockConn.writeForward(details);

                            Log.d("Debug:", "If new node id is greater than myID but less than my successor");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }
                        //If new node ID is less than my ID and my ID is less than predecessor, and new node id is smaller than predecessor
                        /*else if(TiredOfClasses.genHash(details.id).compareTo(TiredOfClasses.genHash(sdp.portStr)) < 0
                                && TiredOfClasses.genHash(details.id).compareTo(sdp.details.predecessor) < 0
                                && TiredOfClasses.genHash(sdp.details.id).compareTo(sdp.details.predecessor) < 0){
                            details.msgType = "3";
                            details.predecessor = sdp.details.predecessor;
                            sdp.details.predecessor = TiredOfClasses.genHash(details.id);

                            details.pPort = sdp.details.pPort;
                            sdp.details.pPort = details.myPort;

                            details.sPort = sdp.myPort;
                            details.successor = TiredOfClasses.genHash(sdp.portStr);

                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeBackward(details);

                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }*/

                        else if(TiredOfClasses.genHash(details.id).compareTo(sdp.hashedSelfId) < 0
                                && !sdp.details.nodeType.equals("head")){
                            Log.d("Debug:", "Forwarding to previous nodes");
                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeBackward(details);

                            return;
                        }
                        //If current node is head and new id is smaller. Then insert and make it head.
                        else if(sdp.details.nodeType.equals("head")
                                && sdp.hashedSelfId.compareTo(TiredOfClasses.genHash(details.id)) > 0){
                            sdp.details.nodeType = "";
                            details.nodeType = "head";

                            details.msgType = "3";
                            details.predecessor = sdp.details.predecessor;
                            sdp.details.predecessor = TiredOfClasses.genHash(details.id);

                            details.pPort = sdp.details.pPort;
                            sdp.details.pPort = details.myPort;

                            details.sPort = sdp.myPort;
                            details.successor = TiredOfClasses.genHash(sdp.portStr);

                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeBackward(details);

                            Log.d("Debug:", "Make New id Head");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }

                        else if(TiredOfClasses.genHash(details.id).compareTo(sdp.hashedSelfId) < 0
                                && TiredOfClasses.genHash(details.id).compareTo(sdp.details.predecessor) > 0){
                            details.msgType = "3";
                            details.predecessor = sdp.details.predecessor;
                            sdp.details.predecessor = TiredOfClasses.genHash(details.id);

                            details.pPort = sdp.details.pPort;
                            sdp.details.pPort = details.myPort;

                            details.sPort = sdp.myPort;
                            details.successor = TiredOfClasses.genHash(sdp.portStr);

                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeBackward(details);

                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }

                        else{
                            //Then this node has to be inserted
                            details.successor = sdp.details.successor;
                            sdp.details.successor = TiredOfClasses.genHash(details.id);
                            details.sPort = sdp.details.sPort;
                            sdp.details.sPort = details.myPort;

                            details.predecessor = TiredOfClasses.genHash(sdp.details.id);
                            details.pPort = sdp.details.myPort;

                            sdp.sockConn.updateConnections(sdp.details);
                            details.msgType = "2";
                            sdp.sockConn.writeForward(details);

                            Log.d("Debug:", "If new node id is greater than myID but less than my successor");
                            Log.d("Debug:", "Successor" + sdp.details.sPort);
                            Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                            return;
                        }

                        //Log.d("Debug:", "Nothing Chosen");
                        //break;
                    }

                    case 2:{
                        sdp.details = (NodeDetails) inp;
                        sdp.sockConn = new SocketConnection(sdp.details);
                        Log.d("Debug:", "Successor" + sdp.details.sPort);
                        Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                        //Update next node's predecessor value
                        NodeDetails beacon = new NodeDetails(sdp.portStr, sdp.myPort);
                        beacon.msgType = "4";
                        sdp.sockConn.writeForward(beacon);
                        return;
                    }

                    case 3:{
                        sdp.details = (NodeDetails) inp;
                        //sdp.details.copy(nds);
                        /*sdp.details.sPort = nds.id;
                        sdp.details.successor = TiredOfClasses.genHash(nds.id);*/

                        sdp.sockConn = new SocketConnection(sdp.details);

                        NodeDetails beacon = new NodeDetails(sdp.portStr, sdp.myPort);
                        beacon.msgType = "5";
                        sdp.sockConn.writeBackward(beacon);

                        Log.d("Debug:", "Successor" + sdp.details.sPort);
                        Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                        return;
                    }

                    case 4:{
                        NodeDetails nds = (NodeDetails) inp;

                        //Cursor c = sdp.queryDelete(TiredOfClasses.genHash(nds.id));

                        sdp.details.predecessor = TiredOfClasses.genHash(nds.id);
                        sdp.details.pPort = nds.myPort;

                        /*if(c != null) {
                            c.moveToFirst();
                            while (!c.isLast()) {

                                ContentValues values = new ContentValues();
                                values.put("key", c.getString(0));
                                values.put("value", c.getString(1));

                                sdp.sockConn.updateConnections(sdp.details);
                                sdp.sockConn.writeBackward(new LoadCarrier(values));
                                c.moveToNext();
                            }
                        }*/

                        Log.d("Debug:", "Successor" + sdp.details.sPort);
                        Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                        return;
                    }

                    case 5:{
                        NodeDetails nds = (NodeDetails) inp;

                        sdp.details.successor = TiredOfClasses.genHash(nds.id);
                        sdp.details.sPort = nds.myPort;

                        Log.d("Debug:", "Successor" + sdp.details.sPort);
                        Log.d("Debug:", "Predecessor" + sdp.details.pPort);
                        return;
                    }

                    //Read from the HashMap of LoadCarrier to ContentValues so that insert method can be used
                    case 56:{
                        LoadCarrier lc = (LoadCarrier) inp;
                        ContentValues insert = new ContentValues();

                        Iterator it = lc.c_.entrySet().iterator();
                        while (it.hasNext()) {

                            Map.Entry pair = (Map.Entry)it.next();
                            insert.put("key", (String)pair.getKey());
                            insert.put("value", (String)pair.getValue());

                            it.remove();
                        }

                        sdp.insert(TiredOfClasses.uri, insert);

                        return;
                    }

                    case 57:{
                        LoadCarrier lc = (LoadCarrier) inp;

                        if(sdp.details.id.equals(lc.initiator)){
                            sdp.map = lc.c_;

                            for(Thread t : Thread.getAllStackTraces().keySet()){
                                if(t.getId() == lc.threadId) {
                                    t.interrupt();
                                    Log.d("Debug:", "Generating interrupt on " + t.getName());
                                }
                            }

                            return;
                        }
                        else{
                            Cursor c = sdp.query(TiredOfClasses.uri, null, "\"@\"", null, null);

                            /*ArrayList<ContentValues> retVal = new ArrayList<ContentValues>();
                            ContentValues map;
                            if(c.moveToFirst()) {
                                do {
                                    map = new ContentValues();
                                    DatabaseUtils.cursorRowToContentValues(c, map);
                                    retVal.add(map);
                                } while(c.moveToNext());
                            }
                            c.close();

                            ArrayList<ContentValues> ret = new ArrayList<ContentValues>();
                            ret.addAll(retVal);
                            ret.addAll(lc.c_);*/

                            if(c != null) {
                                if (c.moveToFirst()) {
                                    do {
                                        lc.c_.put(c.getString(0), c.getString(1));
                                    } while (c.moveToNext());
                                }
                            }

                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeForward(lc);
                            return;
                        }
                    }

                    case 58:{
                        LoadCarrier lc = (LoadCarrier) inp;

                        if(lc.initiator.equals(sdp.details.id))
                            return;

                        else if(lc.cmd.equals("*")) {
                            sdp.delete(TiredOfClasses.uri, null, null);
                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeForward(lc);

                            return;
                        }

                        else{
                            sdp.delete(TiredOfClasses.uri, lc.cmd, null);
                            return;
                        }
                    }

                    case 59:{
                        LoadCarrier lc = (LoadCarrier) inp;
                        Log.d("Debug:", "Got packet" + lc.toString());
                        if(lc.initiator.equals(sdp.details.id)){
                            sdp.map = lc.c_;

                            for(Thread t : Thread.getAllStackTraces().keySet()){
                                if(t.getId() == lc.threadId) {
                                    t.interrupt();
                                    Log.d("Debug:", "Generating interrupt on " + t.getName());
                                }
                            }
                            return;
                        }

                        Cursor ret = sdp.newQuery(lc.cmd);
                        if(ret == null) {
                            Log.d("Debug:", "Packing and sending from null braces");
                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeForward(lc);
                            return;
                        }
                        else{
                            if(ret.moveToFirst()){
                                do {
                                    lc.c_.put(ret.getString(0), ret.getString(1));
                                }while(ret.moveToNext());
                            }
                            Log.d("Debug:", "Packing and sending");
                            sdp.sockConn.updateConnections(sdp.details);
                            sdp.sockConn.writeForward(lc);
                            return;
                        }
                    }

                    case 99:{
                        EndLine el = (EndLine) inp;

                        if(el.command.equals("query"))
                            sdp.query(TiredOfClasses.uri, null, el.message, null, null);
                    }
                }
            }
    //    }
    catch(IOException ioe){
            Log.d(ReceiverTask.class.getSimpleName(), "Socket Exception: IO error");
        //ioe.printStackTrace();
        }
        catch(ClassNotFoundException cnfe){
            Log.d(ReceiverTask.class.getSimpleName(), "Read Exception: Class not found");
        }
    }
}