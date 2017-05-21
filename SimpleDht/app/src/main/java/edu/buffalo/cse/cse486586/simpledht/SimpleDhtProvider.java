package edu.buffalo.cse.cse486586.simpledht;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {


    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;

    private ArrayList<String> _knownPids;
    private HashSet<String> _localKeys;
    private String _myPort;
    private AtomicBoolean _queryWait;
    private AtomicBoolean _queryWaitAll;
    private String _queryValue;
    private String[] _queryValues;


    private void join() {

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  "joinRequest" + "\0" +lookupPid(_myPort),REMOTE_PORT0);

    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        if(selection.equals("*")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteAll" + "\0" + lookupMyPort(), succPort());
        }else if(selection.equals("@")){
            actuallyDeleteAll(lookupMyPort());
        }
        else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete" + "\0" + selection, succPort());
        }
        return 0;
    }

    public void maybeDelete(String key){
        //checkMessage hash
        //check my hash
        //maybe store
        //maybe forward message
        try {
            String prevNodeHash = genHash(predPid());
            String nodeHash = genHash(lookupPid(_myPort));
            String dataHash = genHash(key);

            if(nodeHash.compareTo(dataHash) > 0 && dataHash.compareTo(prevNodeHash) > 0){
                actuallyDelete(key);
                Log.v("Actually Del",key);
            }else if((prevNodeHash.compareTo(nodeHash) > 0) && (dataHash.compareTo(prevNodeHash) > 0 || dataHash.compareTo(nodeHash) < 0)) {
                //if cross border and either ( data is bigger than previous or data less than current)
                actuallyDelete(key);
                Log.v("Actually Del",key);
            }else if(prevNodeHash.equals(nodeHash)){
                actuallyDelete(key);
                Log.v("Actually Del",key);
            }else{
                Log.v("Forwading Del",key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete" +"\0" + key,succPort());
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public void actuallyDelete(String key) {
        _localKeys.remove(key);
    }

    public void actuallyDeleteAll(String returnPort) { //only to be run on owner of data
        _localKeys = new HashSet<String>();
        if(!returnPort.equals(lookupMyPort())){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteAll" +"\0" + returnPort ,succPort());
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    public String lookupMyPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String port = String.valueOf((Integer.parseInt(portStr) * 2));
        return port;
    }

    public String lookupPid(String port) {
        if (port.equals(REMOTE_PORT0)) return "5554";
        if (port.equals(REMOTE_PORT1)) return "5556";
        if (port.equals(REMOTE_PORT2)) return "5558";
        if (port.equals(REMOTE_PORT3)) return "5560";
        return "5562";
    }

    public String lookupPort(String pid) {
        if (pid.equals("5554")) return REMOTE_PORT0;
        if (pid.equals("5556")) return REMOTE_PORT1;
        if (pid.equals("5558")) return REMOTE_PORT2;
        if (pid.equals("5560")) return REMOTE_PORT3;
        return REMOTE_PORT4;
    }

    public String succPort() {
        try {
            String myPort = _myPort;
            String myHash = genHash(lookupPid(myPort));
            String minBigger = "z";
            String res = lookupPid(lookupMyPort());
            for (String remotePid : _knownPids) {
                String remoteHash = genHash(remotePid);
                if (remoteHash.compareTo(myHash) > 0) { //if remoteHash is bigger than myHash
                    if (remoteHash.compareTo(minBigger) < 0) { //if remoteHash is smaller than previous minimum bigger than myHash
                        minBigger = remoteHash;
                        res = remotePid;
                    }

                }
            }
            if (minBigger.equals("z")) {//meaning no nodes larger. find overall min
                for (String remotePid : _knownPids) {
                    String remoteHash = genHash(remotePid);
                    if (remoteHash.compareTo(minBigger) < 0) {
                        minBigger = remoteHash;
                        res = remotePid;
                    }
                }
            }
            return lookupPort(res);
        } catch (NoSuchAlgorithmException e) {
            return "";
        }


    }

    public String predPid() {
        try {
            String myPort = _myPort;
            String myHash = genHash(lookupPid(myPort));
            String maxSmaller = "\0";
            String res = lookupPid(lookupMyPort());
            for (String remotePid : _knownPids) {
                String remoteHash = genHash(remotePid);
                if (remoteHash.compareTo(myHash) < 0) { //if remoteHash is smaller than myHash
                    if (remoteHash.compareTo(maxSmaller) > 0) { //if remoteHash is bigger than previous maximum smaller than myHash
                        maxSmaller = remoteHash;
                        res = remotePid;
                    }

                }
            }
            if (maxSmaller.equals("\0")) {//meaning no nodes smaller. find overall max
                for (String remotePid : _knownPids) {
                    String remoteHash = genHash(remotePid);
                    if (remoteHash.compareTo(maxSmaller) > 0) {
                        maxSmaller = remoteHash;
                        res = remotePid;
                    }
                }
            }
            return res;
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

    public void actuallyInsert(String key, String value) {
        try {
            FileOutputStream outputStream;
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            _localKeys.add(key);
            //Log.v("insert", values.toString());
        } catch (Exception e) {
        }
    }

    public void maybeInsert(String key, String value){
        //checkMessage hash
        //check my hash
        //maybe store
        //maybe forward message
        try {
            String prevNodeHash = genHash(predPid());
            String nodeHash = genHash(lookupPid(_myPort));
            String dataHash = genHash(key);

            Log.v("MyPort",_myPort);
            Log.v("Node",lookupPid(lookupMyPort()));
            Log.v("Data",key);
            Log.v("prevNode",predPid());

            Log.v("NodeHash",nodeHash);
            Log.v("DataHash",dataHash);
            Log.v("prevNodeHash",prevNodeHash);

            if(nodeHash.compareTo(dataHash) > 0 && dataHash.compareTo(prevNodeHash) > 0){
                actuallyInsert(key,value);
                Log.v("Actually Ins",key);
            }else if((prevNodeHash.compareTo(nodeHash) > 0) && (dataHash.compareTo(prevNodeHash) > 0 || dataHash.compareTo(nodeHash) < 0)) {
                //if cross border and either ( data is bigger than previous or data less than current)
                actuallyInsert(key,value);
                Log.v("Actually Ins",key);
            }else if(prevNodeHash.equals(nodeHash)){
                actuallyInsert(key,value);
                Log.v("Actually Ins",key);
            } else{
                Log.v("Forwading",key);
                Log.v("Membership ",""+ _knownPids.size());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert" +"\0" + key+"\0"+value,succPort());
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.get("key").toString();
        String value = values.get("value").toString();

        //Log.v("numKeys",""+_localKeys.size());

        //START RING MESSAGE WITH KEY VALUE TO SUCCESSOR
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert" + "\0" + key+"\0"+value, succPort());

//        Log.v("Length",""+_knownPids.size());
//        Log.v("SuccPID" + lookupPid(_myPort), lookupPid(succPort()));
//        Log.v("PredPID" + lookupPid(_myPort), predPid());
//        Log.v("SuccPort" + lookupPid(_myPort), succPort());
//        Log.v("PredPort" + lookupPid(_myPort), lookupPort(predPid()));

        return uri;
    }




    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        _myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.v("myPort is ",""+_myPort);

        _queryWait = new AtomicBoolean(false);
        _queryWaitAll = new AtomicBoolean(false);
        _queryValue = "";
        _queryValues = new String[0];
        _knownPids = new ArrayList<String>();
        _localKeys = new HashSet<String>();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            //e.printStackTrace();
        }
        join();
        return true;
    }

    public void actuallyQuery(String key,String returnPort) { //only to be run on owner of data


        try {

            FileInputStream inputStream;
            inputStream = getContext().openFileInput(key); //no context needed?
            ///http://stackoverflow.com/questions/2492076/android-reading-from-an-input-stream-efficiently
            int manyAvailable = inputStream.available();
            byte[] b = new byte[manyAvailable];
            inputStream.read(b);
            String value = new String(b);
            inputStream.close();

            //TODO: new thread to return port with response
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "querySingleResponse" +"\0" + value,returnPort);

            //

        } catch (Exception e) {
            Log.e("GROUPMESSAGEERROR", "File read failed");
        }

    }

    public void actuallyQueryAll(String returnPort,String valuesSoFar) { //only to be run on owner of data


        try {

            for(String key: _localKeys ){ //TODO: Unique keys
                FileInputStream inputStream;
                inputStream = getContext().openFileInput(key); //no context needed?
                ///http://stackoverflow.com/questions/2492076/android-reading-from-an-input-stream-efficiently
                int manyAvailable = inputStream.available();
                byte[] b = new byte[manyAvailable];
                inputStream.read(b);
                String value = new String(b);
                if(!valuesSoFar.isEmpty()){
                    valuesSoFar = valuesSoFar + "\0";
                }
                valuesSoFar = valuesSoFar + key + "\0" + value;
                inputStream.close();
            }
            if(returnPort.equals(lookupMyPort())){
                _queryValues = valuesSoFar.split("\0");
                if(_queryValues.length==1){
                   _queryValues = new String[0];
                }
                synchronized (_queryWaitAll) {
                    _queryWaitAll.set(false);
                    _queryWaitAll.notify();
                }
            }else{
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryAll" +"\0" + returnPort + "\0" + valuesSoFar,succPort());
            }



        } catch (Exception e) {
            Log.e("GROUPMESSAGEERROR", "File read failed");
        }

    }

    public void maybeQuery(String key,String returnPort){
        try {
            String prevNodeHash = genHash(predPid());
            String nodeHash = genHash(lookupPid(_myPort));
            String dataHash = genHash(key);


            if(nodeHash.compareTo(dataHash) > 0 && dataHash.compareTo(prevNodeHash) > 0){
                actuallyQuery(key,returnPort);
                Log.v("Actually query",key);
            }else if((prevNodeHash.compareTo(nodeHash) > 0) && (dataHash.compareTo(prevNodeHash) > 0 || dataHash.compareTo(nodeHash) < 0)) {
                //if cross border and either ( data is bigger than previous or data less than current)
                actuallyQuery(key,returnPort);
                Log.v("Actually qeury",key);
            }else if(prevNodeHash.equals(nodeHash)){
                actuallyQuery(key,returnPort);
                Log.v("Actually qeury",key);
            }
            else{
                Log.v("Forwading query",key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "querySingle" +"\0" + key+"\0"+returnPort,succPort());
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        if(selection.equals("*")){
            _queryWaitAll.set(true);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryAll" + "\0" + lookupMyPort() + "\0" + "", succPort());

            //
            synchronized (_queryWaitAll) {
                while (_queryWaitAll.get()) {
                    try {
                        _queryWaitAll.wait();
                    }catch (Exception e){}
                }
            }

            String columnNames[] = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames);
            Log.v("len:::",""+_queryValues.length);
            for(int i = 0; i<_queryValues.length; i=i+2){
                String columnValues[] = {_queryValues[i], _queryValues[i+1]};
                Log.v("QUERYALL",_queryValues[i] + ":" + _queryValues[i+1]);
                mc.addRow(columnValues);
            }

            return mc;


        }else if(selection.equals("@")){
            actuallyQueryAll(lookupMyPort(),"");
            String columnNames[] = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames);
            for(int i = 0; i<_queryValues.length; i=i+2){
                String columnValues[] = {_queryValues[i], _queryValues[i+1]};
                Log.v("QUERYLocal",_queryValues[i] + ":" + _queryValues[i+1]);
                mc.addRow(columnValues);
            }
            return mc;
        }else {
            _queryWait.set(true);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "querySingle" + "\0" + selection + "\0" + lookupMyPort(), succPort());

            synchronized (_queryWait){
                while(_queryWait.get()){
                    try{
                        _queryWait.wait();
                    }catch(Exception e){}
                }
            }

            String columnNames[] = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columnNames);
            String columnValues[] = {selection, _queryValue};
            mc.addRow(columnValues);
            return mc;
        }


        //probably handle * separately with separate array queryValues instaed of queryValue

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            String message = msgs[0];
            String port = msgs[1];

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                OutputStream os;
                os = socket.getOutputStream();
                byte[] ba = (message).getBytes();
                os.write(ba);
            } catch (IOException e) {
                //e.printStackTrace();
            }


            return null;
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {

                while (true) {
                    Socket s = serverSocket.accept();
                    InputStream is = s.getInputStream();

                    byte[] messageRecieved = new byte[10000];
                    int messageLength = is.read(messageRecieved, 0, 10000);
                    String temp = "";
                    for (int i = 0; i < messageLength; i++) {
                        temp += (char) messageRecieved[i];
                    }

                    String splits[] = temp.split("\0",2);
                    String key = splits[0];
                    String value = splits[1];

                    if (key.equals("joinRequest")) {
                        _knownPids.add(value);

                        String msg = "membershipList";
                        for (String pid : _knownPids) {
                            msg = msg + "\0" + pid;
                        }

                        ArrayList<String> allPorts = new ArrayList<String>();
                        allPorts.add(REMOTE_PORT0);
                        allPorts.add(REMOTE_PORT1);
                        allPorts.add(REMOTE_PORT2);
                        allPorts.add(REMOTE_PORT3);
                        allPorts.add(REMOTE_PORT4);
                        for (String remotePort : allPorts) {
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(remotePort));

                                OutputStream os = socket.getOutputStream();
                                byte[] ba = (msg).getBytes();
                                os.write(ba);

                            } catch (IOException e) {
                            }
                        }
                    } else if (key.equals("membershipList")) {
                        String[] members = value.split("\0");
                        _knownPids = new ArrayList<String>(Arrays.asList(members));
                    }else if (key.equals("insert")){
                        String splits2[] = value.split("\0");
                        maybeInsert(splits2[0],splits2[1]);
                    }else if(key.equals("delete")){
                        maybeDelete(value);
                    } else if (key.equals("querySingle")){
                        String splits2[] = value.split("\0");
                        maybeQuery(splits2[0],splits2[1]);//0 is key 1 is return port
                    }else if (key.equals("querySingleResponse")){
                        _queryValue = value;
                        synchronized (_queryWait) {
                            _queryWait.set(false);
                            _queryWait.notify();
                        }
                    }else if (key.equals("queryAll")){
                        String splits2[] = value.split("\0",2);
                        actuallyQueryAll(splits2[0],splits2[1]);//0 is  return port 1 is \0 separated results

                    }else if(key.equals("deleteAll")){
                        actuallyDeleteAll(value);
                    }






                }
            }catch(IOException e){
                return null;
            }

        }


    }
}
