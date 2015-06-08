package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleDhtProvider extends ContentProvider {

    String portStr, myPort, hashedSelfId;
    NodeDetails details;
    SocketConnection sockConn;
    long providerThread;

    private DatabaseHelper dbh = new DatabaseHelper(getContext());
    private SQLiteQueryBuilder sqb = new SQLiteQueryBuilder();
    private SQLiteDatabase sqd;
    public Cursor merged;
    public ArrayList<ContentValues> c_;
    public HashMap<String, String> map;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        String key = TiredOfClasses.genHash(selection);
        sqd = dbh.getDB();

        if(selection.equals("\"*\"")) {
            selection = null;
            this.sockConn.updateConnections(details);
            sockConn.writeForward(new LoadCarrier("*", details.id));

            sqd.delete(DatabaseHelper.TABLE, selection, null);
        }

        else if(selection.equals("\"@\"")) {
            selection = null;
            sqd.delete(DatabaseHelper.TABLE, selection, null);
        }

        /*else {
            String key = selection;

            Boolean cornerCondition = (hashedSelfId.compareTo(this.details.predecessor) < 0);

            if((!cornerCondition && key.compareTo(hashedSelfId) <= 0 && key.compareTo(this.details.predecessor) > 0)
                    || (cornerCondition && ((key.compareTo(hashedSelfId) > 0) && key.compareTo(this.details.predecessor) > 0)
                        || (key.compareTo(hashedSelfId) < 0 && key.compareTo(this.details.predecessor) < 0))
                    || (hashedSelfId.compareTo(this.details.predecessor) == 0)){

                selection = dbh.KEY + " = '" + key + "'";
            }
            else{
                sockConn = new SocketConnection(details);
                sockConn.updateConnections(details);
                sockConn.writeForward(new LoadCarrier(selection, details.id));
                return 0;
            }
        }*/

        else {

            Boolean cornerCondition = (hashedSelfId.compareTo(this.details.predecessor) < 0);

            if (hashedSelfId.compareTo(this.details.predecessor) == 0) {

                selection = dbh.KEY + " = '" + selection + "'";
                sqd.delete(DatabaseHelper.TABLE, selection, null);

            } else {
                if (cornerCondition) {
                    if ((key.compareTo(hashedSelfId) > 0 && key.compareTo(this.details.predecessor) > 0)
                            || (key.compareTo(hashedSelfId) < 0 && key.compareTo(this.details.predecessor) < 0)) {

                        selection = dbh.KEY + " = '" + selection + "'";
                        sqd.delete(DatabaseHelper.TABLE, selection, null);

                    } else {
                        sockConn = new SocketConnection(details);
                        //sockConn.updateConnections(details);
                        sockConn.writeForward(new LoadCarrier(selection, details.id));
                    }
                } else {
                    if (key.compareTo(hashedSelfId) <= 0 && key.compareTo(this.details.predecessor) > 0) {

                        selection = dbh.KEY + " = '" + selection + "'";
                        sqd.delete(DatabaseHelper.TABLE, selection, null);

                    } else {
                        sockConn = new SocketConnection(details);
                        //sockConn.updateConnections(details);
                        sockConn.writeForward(new LoadCarrier(selection, details.id));
                    }
                }
            }
        }

        //sqb.setTables(dbh.TABLE);

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = TiredOfClasses.genHash(values.getAsString("key"));
        String value = values.getAsString("value");

        Boolean cornerCondition = (hashedSelfId.compareTo(this.details.predecessor) < 0);
        Log.d("Debug: ", "CornerCondition:" + cornerCondition + " at " + details.id + " because of " + hashedSelfId.compareTo(this.details.predecessor));

        if(hashedSelfId.compareTo(this.details.predecessor) == 0){
            sqd = dbh.getDB();

            sqb.setTables(DatabaseHelper.TABLE);
            sqd.insert(DatabaseHelper.TABLE, null, values);
        }
        else{
            if(cornerCondition){
                if((key.compareTo(hashedSelfId) > 0 && key.compareTo(this.details.predecessor) > 0)
                        || (key.compareTo(hashedSelfId) < 0 && key.compareTo(this.details.predecessor) < 0)){

                    sqd = dbh.getDB();

                    sqb.setTables(DatabaseHelper.TABLE);
                    sqd.insert(DatabaseHelper.TABLE, null, values);
                }
                else{
                    sockConn = new SocketConnection(details);
                    //sockConn.updateConnections(details);
                    HashMap<String, String> t = new HashMap<String, String>();
                    t.put(values.getAsString("key"), values.getAsString("value"));
                    sockConn.writeForward(new LoadCarrier(t));
                }
            }
            else{
                if(key.compareTo(hashedSelfId) <= 0 && key.compareTo(this.details.predecessor) > 0){
                    sqd = dbh.getDB();

                    sqb.setTables(DatabaseHelper.TABLE);
                    sqd.insert(DatabaseHelper.TABLE, null, values);
                }
                else{
                    sockConn = new SocketConnection(details);
                    //sockConn.updateConnections(details);
                    HashMap<String, String> t = new HashMap<String, String>();
                    t.put(values.getAsString("key"), values.getAsString("value"));
                    sockConn.writeForward(new LoadCarrier(t));
                }
            }
        }

        /*if((!cornerCondition && key.compareTo(hashedSelfId) <= 0 && key.compareTo(this.details.predecessor) > 0)
                || (cornerCondition && ((key.compareTo(hashedSelfId) > 0) && key.compareTo(this.details.predecessor) > 0)
                    || (key.compareTo(hashedSelfId) < 0 && key.compareTo(this.details.predecessor) < 0))
                || (hashedSelfId.compareTo(this.details.predecessor) == 0)) {

            Log.e("Debug: ", "Insert: " + values.getAsString("key") + " HashKey: " + key + " P: " + details.predecessor + " Self: " + hashedSelfId + " S: " + details.successor);

            String selection = DatabaseHelper.KEY + " = '" + values.getAsString("key") + "'";

            sqd = dbh.getDB();

            sqb.setTables(DatabaseHelper.TABLE);
            sqd.insert(DatabaseHelper.TABLE, null, values);
        }
        else{
            sockConn = new SocketConnection(details);
            //sockConn.updateConnections(details);
            HashMap<String, String> t = new HashMap<String, String>();
            t.put(values.getAsString("key"), values.getAsString("value"));
            sockConn.writeForward(new LoadCarrier(t));
        }*/
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        c_ = null;

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        details = new NodeDetails(portStr, myPort);
        details.sPort = myPort;
        details.pPort = myPort;
        details.predecessor = TiredOfClasses.genHash(portStr);
        details.successor = TiredOfClasses.genHash(portStr);

        providerThread = Thread.currentThread().getId();

        hashedSelfId = TiredOfClasses.genHash(this.details.id);

        new Thread(new ServerTask(this)).start(); //Start Server

        dbh = new DatabaseHelper(getContext());

        new Thread((new TiredOfClasses(this))).start(); //to AVD 5554

        Log.d("Debug:", "OnCreate ended");

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        String key = TiredOfClasses.genHash(selection);
        String clause = "";
        Log.d("Debug:", selection);

        if(selection.equals("\"*\"")) {
            clause = null;
            sqd = dbh.getDB();

            sqb.setTables(dbh.TABLE);
            merged = sqb.query(sqd, null, clause, null, null, null, null);
        }

        else if(selection.equals("\"@\"")) {
            clause = null;
            sqd = dbh.getDB();

            sqb.setTables(dbh.TABLE);
            merged = sqb.query(sqd, null, clause, null, null, null, null);
        }

        else {
            Boolean cornerCondition = (hashedSelfId.compareTo(this.details.predecessor) < 0);
            Log.d("Debug: ", "CornerCondition:" + cornerCondition + " at " + details.id + " because of " + hashedSelfId.compareTo(this.details.predecessor));

            if(hashedSelfId.compareTo(this.details.predecessor) == 0){
                clause = DatabaseHelper.KEY + " = '" + selection + "'";
                sqd = dbh.getDB();

                sqb.setTables(dbh.TABLE);
                merged = sqb.query(sqd, null, clause, null, null, null, null);

                return merged;
            }
            else {
                if (cornerCondition) {
                    if ((key.compareTo(hashedSelfId) > 0 && key.compareTo(this.details.predecessor) > 0)
                            || (key.compareTo(hashedSelfId) < 0 && key.compareTo(this.details.predecessor) < 0)) {

                        clause = DatabaseHelper.KEY + " = '" + selection + "'";
                        sqd = dbh.getDB();

                        sqb.setTables(dbh.TABLE);
                        merged = sqb.query(sqd, null, clause, null, null, null, null);
                    } else {
                        sockConn = new SocketConnection(details);
                        sockConn.writeForward(new LoadCarrier(selection, details.id, Thread.currentThread().getId()));

                        try {
                            while (!Thread.currentThread().isInterrupted())
                                Thread.sleep(10);
                        } catch (InterruptedException ie) {
                            Log.d("Debug:", "Interrupted");
                        }

                        String[] columns = {"key", "value"};
                        MatrixCursor mc = new MatrixCursor(columns);
                        Iterator it = map.entrySet().iterator();
                        while (it.hasNext()) {
                            Log.d("Debug:", "Adding");
                            Map.Entry pair = (Map.Entry) it.next();
                            String[] cols = {(String) pair.getKey(), (String) pair.getValue()};
                            mc.addRow(cols);
                            it.remove();
                        } //from stackOverflow

                        merged = mc;
                    }
                } else {
                    if (key.compareTo(hashedSelfId) <= 0 && key.compareTo(this.details.predecessor) > 0) {

                        clause = DatabaseHelper.KEY + " = '" + selection + "'";
                        sqd = dbh.getDB();

                        sqb.setTables(dbh.TABLE);
                        merged = sqb.query(sqd, null, clause, null, null, null, null);
                    } else {
                        sockConn = new SocketConnection(details);
                        sockConn.writeForward(new LoadCarrier(selection, details.id, Thread.currentThread().getId()));

                        try {
                            while (!Thread.currentThread().isInterrupted())
                                Thread.sleep(10);
                        } catch (InterruptedException ie) {
                            Log.d("Debug:", "Interrupted");
                        }

                        String[] columns = {"key", "value"};
                        MatrixCursor mc = new MatrixCursor(columns);
                        Iterator it = map.entrySet().iterator();
                        while (it.hasNext()) {
                            Log.d("Debug:", "Adding");
                            Map.Entry pair = (Map.Entry) it.next();
                            String[] cols = {(String) pair.getKey(), (String) pair.getValue()};
                            mc.addRow(cols);
                            it.remove();
                        } //from stackOverflow

                        merged = mc;
                    }
                }
            }
        }

        if(selection.equals("\"*\"")) {
            try {
                //if((hashedSelfId.compareTo(this.details.predecessor) != 0)) {
                //EndLine el = new EndLine(selection, "query");
                Log.d("Debug:", " " + details.sPort + " " + details.pPort);

                map = new HashMap<String, String>();

                if(merged.moveToFirst()) {
                    do {
                        /*cv = new ContentValues();
                        DatabaseUtils.cursorRowToContentValues(merged, cv);
                        retVal.add(cv);*/
                        Log.d("Debug:", "Adding to map"+ merged.getString(0) + " " + merged.getString(1));
                        this.map.put(merged.getString(0), merged.getString(1));
                    } while(merged.moveToNext());
                }
                merged.close();
                Log.d("Debug:", "Sending the map");
                sockConn = new SocketConnection(details);
                sockConn.writeForward(new LoadCarrier(map, details.id, Thread.currentThread().getId()));

                while (!Thread.currentThread().isInterrupted())
                    Thread.sleep(10);
            } catch (InterruptedException ie) {
                Log.d("Debug:", "Interrupted");
            }

            String[] columns = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columns);
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Log.d("Debug:", "Adding");
                Map.Entry pair = (Map.Entry)it.next();
                String[] cols = {(String)pair.getKey(), (String)pair.getValue()};
                mc.addRow(cols);
                it.remove();
            } //from stackOverflow

            merged = mc;
        }

        if(merged == null){
            Log.d("Debug:", "Nothing to return");
            return null;
        }
        else
            return merged;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public Cursor newQuery(String selection){
        Log.d("Debug:", selection);
        String clause = DatabaseHelper.KEY + " = '" + selection + "'";
        sqd = dbh.getDB();

        sqb.setTables(DatabaseHelper.TABLE);
        Cursor c = sqb.query(sqd, null, clause, null, null, null, null);

        if(c.getCount() == 0)
            return null;
        return c;
    }
}