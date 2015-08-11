package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {


    static String TAG = ServerTask.class.getSimpleName();
    static String[] strArray;
    static String temp_lowest;
    static String temp_highest;
    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];

        while(true) {
            try {

                Socket server = serverSocket.accept();

                /*
                ObjectOutputStream tOs = new ObjectOutputStream(server.getOutputStream());
                tOs.writeObject("I_AM_ALIVE");
                tOs.flush();
                */

                ObjectInputStream clientInputStream = new ObjectInputStream(server.getInputStream());
                try{
                    strArray = (String[])clientInputStream.readObject();
                }
                catch (ClassNotFoundException e) {
                    Log.e(TAG, "ServerTask ClassNotFoundException");
                    e.printStackTrace();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ServerTask UnknownHostException");
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "ServerTask socket IOException");
                    e.printStackTrace();
                }




                // Message type: set predecessor and successor //
                if (strArray[0].equals("set_pred_succ")){
                    SimpleDynamoProvider.myPred_Str = strArray[1];
                    SimpleDynamoProvider.mySucc_Str = strArray[2];
                    SimpleDynamoProvider.LowestEmu = strArray[3];
                    SimpleDynamoProvider.HighestEmu = strArray[4];
                    SimpleDynamoProvider.AllEmus[0] = strArray[5];
                    SimpleDynamoProvider.AllEmus[1] = strArray[6];
                    SimpleDynamoProvider.AllEmus[2] = strArray[7];
                    SimpleDynamoProvider.AllEmus[3] = strArray[8];
                    SimpleDynamoProvider.AllEmus[4] = strArray[9];

                    Log.e(TAG,"Set values: "+SimpleDynamoProvider.myEmuId_Str+" "+SimpleDynamoProvider.myPred_Str+" "+SimpleDynamoProvider.mySucc_Str+" "+SimpleDynamoProvider.LowestEmu+" "+SimpleDynamoProvider.HighestEmu+" "+SimpleDynamoProvider.AllEmus[0]+" "+SimpleDynamoProvider.AllEmus[1]+" "+SimpleDynamoProvider.AllEmus[2]+" "+SimpleDynamoProvider.AllEmus[3]+" "+SimpleDynamoProvider.AllEmus[4]);
                }
                // ---------------------------------------- //


                // Message type: inform emulator id //
                if (strArray[0].equals("inform_emu_id")){

                    String temp = strArray[1];

                    Log.e(TAG, "server listens at emulator"+SimpleDynamoProvider.myEmuId_Str+" "+temp);

                    if (!SimpleDynamoProvider.allEmuId_Str_HMap.containsKey(temp)){
                        SimpleDynamoProvider.allEmuId_Str_HMap.put(temp,SimpleDynamoProvider.returnHash(temp));
                    }

                    SetPredSucc(SimpleDynamoProvider.allEmuId_Str_HMap);

                    for (String x:SimpleDynamoProvider.EmuId_To_PdSc_HMap.keySet()){
                        new ClientTaskToSpecific().executeOnExecutor(SimpleDynamoProvider.AyncThreadObj, x,SimpleDynamoProvider.EmuId_To_PdSc_HMap.get(x).get(0),SimpleDynamoProvider.EmuId_To_PdSc_HMap.get(x).get(1),temp_lowest,temp_highest,SimpleDynamoProvider.AllEmus[0],SimpleDynamoProvider.AllEmus[1],SimpleDynamoProvider.AllEmus[2],SimpleDynamoProvider.AllEmus[3],SimpleDynamoProvider.AllEmus[4] );
                    }
                }
                // ------------------------------------ //

                // Message type: key value direct insert pair //
                if (strArray[0].equals("K_V_direct")){
                    Log.e(TAG,strArray[1]+" "+strArray[2]+" received for insert");
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();
                    obj1.InsertHere(strArray[1],strArray[2]);

                    ObjectOutputStream clientOutputStream = new ObjectOutputStream(server.getOutputStream());
                    clientOutputStream.writeObject("insert_done");
                    clientOutputStream.flush();
                }
                // ------------------------------------ //

                // Message type: InsertLowest_K_V //
                if (strArray[0].equals("InsertLowest_K_V")){
                    Log.e(TAG,"insert lowest key value received "+strArray);
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();
                    obj1.InsertHere(strArray[1],strArray[2]);
                }
                // ------------------------------------ //      FinalQuery_Lowest


                // Message type: QueryIn@Destination //
                if (strArray[0].equals("QueryIn@Destination")){
                    Log.e(TAG,"finally process query here"+strArray);
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();

                    String keyString = obj1.StringRespondToQuery(strArray[1])[0];
                    String valString = obj1.StringRespondToQuery(strArray[1])[1];
                    //String vers =  Integer.toString(SimpleDynamoProvider.key_to_version.get(keyString));
                    Log.e(TAG,"cursor string: "+keyString+" "+valString);

                    String[] temparr = {keyString,valString};
                    ObjectOutputStream clientOutputStream = new ObjectOutputStream(server.getOutputStream());
                    clientOutputStream.writeObject(temparr);
                    clientOutputStream.flush();
                }
                // ------------------------------------ //


                // Message type: Process@here //
                if (strArray[0].equals("Process@here")){
                    Log.e(TAG,"@ process query here"+strArray);
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();
                    String[] StOneArr = obj1.StringProcessAtInRing();
                    new ClientTask_ReturnAtToHost().executeOnExecutor(SimpleDynamoProvider.AyncThreadObj, strArray[2], StOneArr[0], StOneArr[1]);
                }
                // ------------------------------------ //

                // Message type: AtCollection //
                if (strArray[0].equals("AtCollection")){

                    if(!SimpleDynamoProvider.ForStar_Emu_To_KeyMap.containsKey(strArray[3])){
                        SimpleDynamoProvider.ForStar_Emu_To_KeyMap.put(strArray[3],strArray[1]);
                        SimpleDynamoProvider.ForStar_Emu_To_ValMap.put(strArray[3],strArray[2]);
                    }
                    Log.e(TAG,"* > @ updated for "+strArray[3]);
                }
                // ------------------------------------ //


                // Message type: recollect_ping //
                if (strArray[0].equals("recollect_ping")){
                    Log.e(TAG,"recollect ping received @ "+SimpleDynamoProvider.myEmuId_Str+" from "+strArray[1]+", return initiated too");
                    String[] temp = SimpleDynamoProvider.keyString_valString_Gen(strArray[1]);
                    new ClientTask_RecollectPing().executeOnExecutor(SimpleDynamoProvider.AyncThreadObj, strArray[1],SimpleDynamoProvider.myEmuId_Str,"return_recollect_ping",temp[0],temp[1]);
                }

                // ------------------------------------ //


                // Message type: return_recollect_ping //
                if (strArray[0].equals("return_recollect_ping")){
                    Log.e(TAG,"recollect ping returns @ "+SimpleDynamoProvider.myEmuId_Str+" from "+strArray[1]);
                    if (strArray[2].length() > 0){
                        SimpleDynamoProvider obj = new SimpleDynamoProvider();
                        obj.LongStringInsert(strArray[2],strArray[3]);
                    }
                    Log.e(TAG,"recollect ping file write done @ "+SimpleDynamoProvider.myEmuId_Str+" from "+strArray[1]);
                    SimpleDynamoProvider.respawnCounter++;
                }

                // ------------------------------------ //



                // Message type: MIssing_Mapper //
                if (strArray[0].equals("MIssing_Mapper")){

                    if (!SimpleDynamoProvider.return_to_emulator_map.contains(strArray[1])){
                        ArrayList<String> temp = new ArrayList<>();
                        temp.add(strArray[2]);
                        SimpleDynamoProvider.return_to_emulator_map.put(strArray[1],temp);
                        temp.clear();

                        ArrayList<String> temp2 = new ArrayList<>();
                        temp2.add(strArray[3]);
                        SimpleDynamoProvider.return_to_emulator_map_val.put(strArray[1],temp2);
                        temp2.clear();
                    }
                    else{
                        SimpleDynamoProvider.return_to_emulator_map.get(strArray[1]).add(strArray[2]);
                        SimpleDynamoProvider.return_to_emulator_map_val.get(strArray[1]).add(strArray[3]);
                    }

                }
                // ------------------------------------ //

                // Message type: Del_Replica //
                if (strArray[0].equals("Del_Replica")){
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();
                    Log.e(TAG,"deleting finally here"+strArray[1]);
                    obj1.Rep_LocalSingle_QueryDelete(strArray[1]);

                    ObjectOutputStream clientOutputStream = new ObjectOutputStream(server.getOutputStream());
                    clientOutputStream.writeObject("delete_done");
                    clientOutputStream.flush();
                }
                // ------------------------------------ //



                // Message type: del@ //
                if (strArray[0].equals("del@")){
                    Log.e(TAG,"@ del here");
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();
                    obj1.DelAt_InRing();
                }
                // ------------------------------------ //

                // Message type: del_this //
                if (strArray[0].equals("del_this")){
                    Log.e(TAG,"single query del here from server"+strArray);
                    SimpleDynamoProvider obj1 = new SimpleDynamoProvider();
                    obj1.LocalSingle_QueryDelete(strArray[1]);
                }
                // ------------------------------------ //


                server.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException lower");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException lower");
                e.printStackTrace();
            }
        }

    }

    protected void onProgressUpdate(String...stArr){



        final Uri mUri;
        final ContentValues mContentValues;

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        mContentValues = new ContentValues();
        mContentValues.put("key",stArr[1]);
        mContentValues.put("value", stArr[2]);

        try {
            SimpleDynamoProvider obj = new SimpleDynamoProvider();
            obj.insert(mUri, mContentValues);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
        }



    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public static void SetPredSucc(HashMap<String,String> Hmp){
        HashMap<String,String> InvHmp = new HashMap<>(); // map of hashvalue - emulatorid

        for (String key:Hmp.keySet()){
            InvHmp.put(Hmp.get(key),key);
        }

        ArrayList<String> hashVal_Lst = new ArrayList<>();
        hashVal_Lst.addAll(InvHmp.keySet()) ;
        Collections.sort(hashVal_Lst);
        ArrayList<String> Ordered_EmIds = new ArrayList<>();

        Log.e(TAG,"-----------------");
        int ic =0;
        for (String xc: hashVal_Lst){
            Log.e(TAG,""+xc+": "+InvHmp.get(xc));
            Ordered_EmIds.add(InvHmp.get(xc));
            SimpleDynamoProvider.AllEmus[ic]=InvHmp.get(xc);
            ic++;
        }
        Log.e(TAG,"-----------------");
        Log.e(TAG,""+Ordered_EmIds);
        temp_lowest = Ordered_EmIds.get(0);
        temp_highest = Ordered_EmIds.get(Ordered_EmIds.size()-1);

        String tmp_pred ="";
        String tmp_succ ="";
        String tmp_em_id = "";
        ArrayList<String> tmp_lst = new ArrayList<>();

        SimpleDynamoProvider.EmuId_To_PdSc_HMap.clear();

        for (int i =0; i<Ordered_EmIds.size(); i++){


            if (i == 0){
                tmp_lst.clear();
                tmp_em_id = Ordered_EmIds.get(i);
                tmp_pred = Ordered_EmIds.get(Ordered_EmIds.size()-1);
                tmp_succ = Ordered_EmIds.get(i+1);

                tmp_lst.add(tmp_pred);
                tmp_lst.add(tmp_succ);
                SimpleDynamoProvider.EmuId_To_PdSc_HMap.put(tmp_em_id,new ArrayList<String>(Arrays.asList(tmp_pred,tmp_succ)));
            }

            if (i == Ordered_EmIds.size()-1){
                tmp_lst.clear();
                tmp_em_id = Ordered_EmIds.get(i);
                tmp_pred = Ordered_EmIds.get(i-1);
                tmp_succ = Ordered_EmIds.get(0);

                tmp_lst.add(tmp_pred);
                tmp_lst.add(tmp_succ);
                SimpleDynamoProvider.EmuId_To_PdSc_HMap.put(tmp_em_id,new ArrayList<String>(Arrays.asList(tmp_pred,tmp_succ)));
            }

            if (i != 0 && i != Ordered_EmIds.size()-1){
                tmp_lst.clear();
                tmp_em_id = Ordered_EmIds.get(i);
                tmp_pred = Ordered_EmIds.get(i-1);
                tmp_succ = Ordered_EmIds.get(i+1);

                tmp_lst.add(tmp_pred);
                tmp_lst.add(tmp_succ);
                SimpleDynamoProvider.EmuId_To_PdSc_HMap.put(tmp_em_id,new ArrayList<String>(Arrays.asList(tmp_pred,tmp_succ)));
            }
        }


    }

}
