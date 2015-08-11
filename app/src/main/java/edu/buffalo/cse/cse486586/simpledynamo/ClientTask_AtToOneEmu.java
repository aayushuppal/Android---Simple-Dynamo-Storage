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

public class ClientTask_AtToOneEmu  extends AsyncTask<String, Void, Void> {
    static String TAG = ClientTask_AtToOneEmu.class.getSimpleName();

    @Override
    protected Void doInBackground(String... msgs) {

        String x = msgs[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(x)*2);
            socket.setSoTimeout(2000);

            if(msgs[1].equals("del@")){
                String[] msgToSend = {"del@"};
                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
            }
            else {

                String[] msgToSend = {"Process@here",msgs[1],msgs[2]};

                ObjectOutputStream clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
                clientOutputStream.writeObject(msgToSend);
                /*
                ObjectInputStream tIs = new ObjectInputStream(socket.getInputStream());
                String tmp = (String) tIs.readObject();
                Log.e(TAG,tmp+" "+x);
                */

                Log.e(TAG,"@"+"sent to destination"+x);
            }


        }
        catch (UnknownHostException e) {
            Log.e(TAG, " UnknownHostException");
        } catch (IOException e) {
            SimpleDynamoProvider.StarCountExp--;
            Log.e(TAG, " socket IOException ");
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
