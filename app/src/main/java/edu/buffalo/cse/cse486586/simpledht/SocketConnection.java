package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by nikhil on 3/27/15.
 */
public class SocketConnection{

    Socket forward = null, backward = null;
    ObjectOutputStream out, backOut;
    NodeDetails debugDetails;

    public SocketConnection(NodeDetails nds) {
        //Create sockets here for successor and predecessor
        this.debugDetails = nds;
        updateConnections(nds);
    }

    public void updateConnections(NodeDetails nds){
        //close previous sockets
        //update according to new NodeDetails object
        try {
            if(forward != null && !forward.isClosed())
                forward.close();
            forward = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(nds.sPort));

            out = new ObjectOutputStream(forward.getOutputStream());

            if(backward != null && !backward.isClosed())
                backward.close();
            backward = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(nds.pPort));

            backOut = new ObjectOutputStream(backward.getOutputStream());

        }catch(UnknownHostException ukhe){ Log.d(SocketConnection.class.getSimpleName(), "Update Socket Exception: Unknown Host"); }
        catch(IOException ioe){ Log.d(SocketConnection.class.getSimpleName(), "Update Socket Exception: IO Error"); }
    }

    public void writeForward(Object nds){
        try {
            Log.d("Debug:", "Writing forward to " + debugDetails.sPort);
            out.writeObject(nds);

        }catch(IOException ioe){ Log.d(SocketConnection.class.getSimpleName(), "writeForward Exception"); ioe.printStackTrace(); }
    }

    public void readForward(){
        //Will return ContentValues from here
    }

    public void writeBackward(Object nds){
        try {
            Log.d("Debug:", "Writing backwards to " + debugDetails.pPort);
            backOut.writeObject(nds);

        }catch(IOException ioe){ Log.d(SocketConnection.class.getSimpleName(), "writeForward Exception"); }
    }

    public void readBackward(){
        //read data from the predecessor node here
    }
}