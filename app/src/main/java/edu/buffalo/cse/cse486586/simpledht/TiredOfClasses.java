package edu.buffalo.cse.cse486586.simpledht;

import android.net.Uri;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * Created by nikhil on 3/28/15.
 */
public class TiredOfClasses implements Runnable {
    NodeDetails nds;
    static Uri uri;
    SimpleDhtProvider sdp;

    public TiredOfClasses(SimpleDhtProvider sdp){
        this.nds = sdp.details;
        this.sdp = sdp;
        TiredOfClasses.uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    public static String genHash(String input) {
        MessageDigest sha1 = null;
        try {
             sha1 = MessageDigest.getInstance("SHA-1");
        }catch (NoSuchAlgorithmException NSAE){}

        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void run() {
        try{

Thread.sleep(10);
            Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt("11108"));

            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            out.writeObject(nds);
        }
        catch(UnknownHostException uhe){ Log.d(TiredOfClasses.class.getSimpleName(), "Socket Exception: Unknown Host"); }
        catch(IOException ieo){ Log.d(TiredOfClasses.class.getSimpleName(), "Socket Exception: IO Error");}
        catch(Exception e){}
    }
}
