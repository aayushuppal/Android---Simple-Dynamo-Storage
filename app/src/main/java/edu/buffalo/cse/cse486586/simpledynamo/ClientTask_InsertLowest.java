package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientTask_InsertLowest  extends AsyncTask<String, Void, Void> {
    static String TAG = ClientTask_InsertLowest.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {

        String x = msgs[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x)*2);
            socket.setSoTimeout(2000);
            String[] msgToSend = {"InsertLowest_K_V",msgs[1],msgs[2]};

            ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
            clientOutputStream.writeObject(msgToSend);

            Log.e(TAG,"key-value"+"sent to "+x);

        }
        catch (UnknownHostException e) {
            Log.e(TAG, "ClientTaskToSpecific() UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "MIssing called for:"+msgs[0]);
            Log.e(TAG, "ClientTaskToSpecific() socket IOException ");
            e.printStackTrace();
        }

        return null;
    }
}