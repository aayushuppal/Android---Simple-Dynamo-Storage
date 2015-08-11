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

public class ClientTask_CheckAlive  extends AsyncTask<String, Void, Void> {
    static String TAG = ClientTask_CheckAlive.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {

        String x = msgs[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x)*2);
            socket.setSoTimeout(2000);
            if (msgs[2].equals("return_check_alive_ping")){
                String[] msgToSend = {"return_check_alive_ping",msgs[1]};

                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
                Log.e(TAG,"return check alive ping sent from "+SimpleDynamoProvider.myEmuId_Str+" to "+x);
            } else {
                String[] msgToSend = {"check_alive_ping",msgs[1]};

                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
                Log.e(TAG,"check alive ping sent from "+SimpleDynamoProvider.myEmuId_Str+" to "+x);
            }


        }
        catch (UnknownHostException e) {
            Log.e(TAG, " UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, " socket IOException ");
            e.printStackTrace();
        }

        return null;
    }
}