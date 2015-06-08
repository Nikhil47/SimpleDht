package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by nikhil on 3/25/15.
 */
public class ServerTask implements Runnable {

        Socket socks;
        SimpleDhtProvider sdp;

        public ServerTask(SimpleDhtProvider sdp){
            this.sdp = sdp;
        }

        @Override
        public void run() {
            ServerSocket acceptSkt;

            try {
                acceptSkt = new ServerSocket(10000);
                Log.d("Debug:", "Accept Sockets1");
                while (true) {
                    socks = acceptSkt.accept();
                    Log.d("Debug:", "Accept Sockets");
                    new Thread(new ReceiverTask(socks, sdp)).start();
                }
            }catch(IOException ioe){ Log.d(ServerTask.class.getSimpleName(), "Error in accepting sockets"); }
        }
}
