package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;import java.lang.String;

/**
 * Created by nikhil on 3/29/15.
 */
public class EndLine implements Serializable {
    String message;
    String command;

    public EndLine(String message, String command){
        this.message = message;
        this.command = command;
    }

    public String toString(){
        return "99";
    }
}
