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
import java.util.concurrent.TimeoutException;

public class ClientTaskForKV  extends AsyncTask<String, Void, Void> {
    static String TAG = ClientTaskForKV.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {
        String x = msgs[0];
        try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x)*2);
                socket.setSoTimeout(2000);

                String[] msgToSend = {"K_V_direct",msgs[1],msgs[2]};
                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
                Log.e(TAG,msgs[1]+" "+msgs[2]+"sent for insert to "+x+" from "+SimpleDynamoProvider.myEmuId_Str);

                ObjectInputStream clientInputStream = new ObjectInputStream(socket.getInputStream());
                String tht = (String) clientInputStream.readObject();

            }

            catch (UnknownHostException e) {
                Log.e(TAG, "UnknownHostException");
            } catch (Exception e) {
                Log.e(TAG, "ClientTaskForKV socket IOException ");
                Log.e(TAG, "MIssing called for:"+msgs[0]+" & "+msgs[1]+" put");
                SimpleDynamoProvider.emu_to_Key_val_log_map.get(msgs[0]).put(msgs[1],msgs[2]);
                //e.printStackTrace();
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