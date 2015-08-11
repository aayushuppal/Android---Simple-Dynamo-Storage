package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientTask_ToDestination  extends AsyncTask<String, Void, Void> {
    static String TAG = ClientTask_ToDestination.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {
        Log.e(TAG,"PathCheck: here in ClientTask_ToDestination");
        String x = msgs[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x)*2);
            socket.setSoTimeout(4000);

            if(msgs[2].equals("del_this")){
                String[] msgToSend = {"del_this_here",msgs[1]};
                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
            }

            else {

                String[] msgToSend = {"QueryIn@Destination",msgs[1],msgs[2]};
                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
                Log.e(TAG,msgs[1]+" query sent to destination"+x);

                ObjectInputStream clientInputStream = new ObjectInputStream(socket.getInputStream());
                String[] tht = (String[]) clientInputStream.readObject();
                SimpleDynamoProvider.QueryToCsrStringMap.get(tht[0]).add(tht[0]+"~"+tht[1]);

            }


        }
        catch (UnknownHostException e) {
            Log.e(TAG, "UnknownHostException");
        } catch (Exception e) {
            SimpleDynamoProvider.QueryToSingleEXC.put(msgs[1],SimpleDynamoProvider.QueryToSingleEXC.get(msgs[1])-1);
            Log.e(TAG, "socket IOException ");
            e.printStackTrace();
        } 
        /*
        catch (ClassNotFoundException s){
            Log.e(TAG,"class not found");
            s.printStackTrace();
        }
        */

        return null;
    }
}