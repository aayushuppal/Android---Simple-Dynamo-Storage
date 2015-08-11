package edu.buffalo.cse.cse486586.simpledynamo;

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

public class ClientTask_DelReplica  extends AsyncTask<String, Void, Void> {
    static String TAG = ClientTask_DelReplica.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {

        String x = msgs[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x)*2);
            socket.setSoTimeout(2000);
            String[] msgToSend = {"Del_Replica",msgs[1]};

            ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
            clientOutputStream.writeObject(msgToSend);
            Log.e(TAG,"replic key-value"+"sent to "+x);

            ObjectInputStream clientInputStream = new ObjectInputStream(socket.getInputStream());
            String tht = (String) clientInputStream.readObject();

        }
        catch (UnknownHostException e) {
            Log.e(TAG, "UnknownHostException");
        } catch (Exception e) {
            Log.e(TAG, "socket IOException ");
            Log.e(TAG, "delete MIssing called for:"+msgs[0]+" & "+msgs[1]+" put");
            SimpleDynamoProvider.emu_to_Key_del_log_map.get(msgs[0]).add(msgs[1]);
            e.printStackTrace();
        }

        return null;
    }
}