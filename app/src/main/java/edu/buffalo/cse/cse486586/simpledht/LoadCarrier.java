package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;

import java.io.Serializable;import java.lang.String;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;

/**
 * Created by nikhil on 3/29/15.
 */
public class LoadCarrier implements Serializable {
    public ContentValues cv;

    public HashMap<String, String> c_;
    public String initiator;    //Contains the process id that initiated this request
    public long threadId;

    public String cmd;

    public String msgType;

    public LoadCarrier(HashMap<String, String> c){
        this.c_ = c;
        this.msgType = "56";
    }

    public LoadCarrier(HashMap<String, String> c, String init, long id){
        this.c_ = c;
        this.initiator = init;
        this.threadId = id;
        this.msgType = "57";
    }

    public LoadCarrier(String cmd, String init){
        this.cmd = cmd;
        this.initiator = init;
        this.msgType = "58";
    }

    public LoadCarrier(String cmd, String init, long id){
        this.c_ = new HashMap<String, String>();
        this.cmd = cmd;
        this.initiator = init;
        this.threadId = id;
        this.msgType = "59";
    }

    public String toString(){
        return msgType;
    }
}