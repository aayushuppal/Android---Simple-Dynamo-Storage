package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static String myEmuId_Str="ll"; // identifies the port of the avd eg: 5554
    static String mySucc_Str; // identifies the successor
    static String myPred_Str; // identifies the predecessor
    static String LowestEmu = "5562";
    static String HighestEmu = "5560";
    static String[] AllEmus= {"5562","5556","5554","5558","5560"}; //"" 5556 5554 5558 5560
    static HashMap<String,String> allEmuId_Str_HMap = new HashMap<>(); //keeps a map of emulatorids to their hashes
    static HashMap<String,ArrayList<String>> EmuId_To_PdSc_HMap = new HashMap<>(); //keeps a map of emulatorids to their predecessors and successors
    static Context staticContext;

    static HashMap<String,ArrayList<String>> QueryToCsrStringMap = new HashMap<>();
    static Hashtable<String,Integer> QueryToSingleEXC = new Hashtable<>();
    static int StarCountExp;

    static HashMap<String,String> ForStar_Emu_To_KeyMap = new HashMap<>();
    static HashMap<String,String> ForStar_Emu_To_ValMap = new HashMap<>();
    static Hashtable<String,String[]> coord_rep_map = new Hashtable<>(); // coord number - replica numbers
    static Hashtable<String,ArrayList<String>> return_to_emulator_map = new Hashtable<>();
    static Hashtable<String,ArrayList<String>> return_to_emulator_map_val = new Hashtable<>();
    static Hashtable<String,Hashtable<String,String>> emu_to_Key_val_log_map = new Hashtable<>();
    static Hashtable<String,HashSet<String>> emu_to_Key_del_log_map = new Hashtable<>();

    static boolean checkAliveFlag = false;
    static int respawnCounter = 0;
    static Hashtable<String,Integer> key_to_version = new Hashtable<>();
    public static BlockingQueue<Runnable> sPoolWorkQueue =
            new LinkedBlockingQueue<Runnable>(20);


    static Executor AyncThreadObj = new ThreadPoolExecutor(60, 100, 20, TimeUnit.SECONDS, sPoolWorkQueue);

//---------------------------------------------ON CREATE-------------------------------//
    @Override
    public boolean onCreate() {
        staticContext = getContext();
        SimpleDynamoProvider_FirstTime();
        return false;
    }

    public void SimpleDynamoProvider_FirstTime(){

        TelephonyManager tel = (TelephonyManager) staticContext.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myEmuId_Str = String.valueOf((Integer.parseInt(portStr)));



        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AyncThreadObj, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
        }

        String[] aa = {"5558","5560"};
        String[] bb = {"5554","5558"};
        String[] cc = {"5560","5562"};
        String[] dd = {"5562","5556"};
        String[] ee = {"5556","5554"};
        coord_rep_map.put("5554",aa);
        coord_rep_map.put("5556",bb);
        coord_rep_map.put("5558",cc);
        coord_rep_map.put("5560",dd);
        coord_rep_map.put("5562",ee);


        if (myEmuId_Str.equals("5554")){
            myPred_Str = "5556";
            mySucc_Str = "5558";
        }

        if (myEmuId_Str.equals("5556")){
            myPred_Str = "5562";
            mySucc_Str = "5554";
        }

        if (myEmuId_Str.equals("5558")){
            myPred_Str = "5554";
            mySucc_Str = "5560";
        }

        if (myEmuId_Str.equals("5560")){
            myPred_Str = "5558";
            mySucc_Str = "5562";
        }

        if (myEmuId_Str.equals("5562")){
            myPred_Str = "5560";
            mySucc_Str = "5556";
        }


        Hashtable<String,String> aempt = new Hashtable<String,String>();
        Hashtable<String,String> bempt = new Hashtable<String,String>();
        Hashtable<String,String> cempt = new Hashtable<String,String>();
        Hashtable<String,String> dempt = new Hashtable<String,String>();
        Hashtable<String,String> eempt = new Hashtable<String,String>();
        emu_to_Key_val_log_map.put("5554",aempt);               // used to store all key values lost at this avd for the particular key's avd
        emu_to_Key_val_log_map.put("5556",bempt);
        emu_to_Key_val_log_map.put("5558",cempt);
        emu_to_Key_val_log_map.put("5560",dempt);
        emu_to_Key_val_log_map.put("5562",eempt);

        HashSet<String> aemptS = new HashSet<>();
        HashSet<String> bemptS = new HashSet<>();
        HashSet<String> cemptS = new HashSet<>();
        HashSet<String> demptS = new HashSet<>();
        HashSet<String> eemptS = new HashSet<>();
        emu_to_Key_del_log_map.put("5554",aemptS);               // used to store all key values lost at this avd for the particular key's avd
        emu_to_Key_del_log_map.put("5556",bemptS);
        emu_to_Key_del_log_map.put("5558",cemptS);
        emu_to_Key_del_log_map.put("5560",demptS);
        emu_to_Key_del_log_map.put("5562",eemptS);




        if (staticContext.fileList().length > 0){

            Log.e(TAG,"launch recollect lost files");
            respawnCounter = 0;
            for (String x:AllEmus){
                if (!x.equals(myEmuId_Str)){
                    new ClientTask_RecollectPing().executeOnExecutor(AyncThreadObj, x,myEmuId_Str,"recollect_ping");
                }
            }
            /*
            while (respawnCounter < 4){
                try{
                    Log.e(TAG,"waiting for respawn recollect to complete - 100ms sleep");
                    Thread.sleep(100L);
                }catch (InterruptedException e){
                    Log.e(TAG,"InterruptedException");
                    e.printStackTrace();
                }
            }
            */
        }
        else {
            Log.e(TAG,"first run of "+myEmuId_Str);
        }

        InsertHere("DummyKey","DummyVal");

    }

//---------------------------------------------INSERT-------------------------------//
    @Override
    public Uri insert(Uri uri, ContentValues values) {


            Log.e(TAG,"INITIAL INSERT: "+values.get("key").toString()+" value: "+values.get("value").toString()+" at "+myEmuId_Str);
            String K = values.get("key").toString();
            String V = values.get("value").toString();
            String keyHash = returnHash(K);
            String LowestHash = returnHash(LowestEmu);
            String HighestHash = returnHash(HighestEmu);

            if (keyHash.compareTo(LowestHash)<=0 || keyHash.compareTo(HighestHash) > 0){
                Log.e(TAG,"will insert "+K+" to "+LowestEmu+" 5556 5554");
                new ClientTaskForKV().executeOnExecutor(AyncThreadObj, LowestEmu,K,V);
                new ClientTaskForKV().executeOnExecutor(AyncThreadObj, "5556",K,V);
                new ClientTaskForKV().executeOnExecutor(AyncThreadObj, "5554",K,V);
            }
            else {
                String Destination_Emulator = WhereToSend(K);
                String[] xdr = coord_rep_map.get(Destination_Emulator);
                Log.e(TAG,"will insert "+K+" to "+Destination_Emulator+" "+xdr[0]+" "+xdr[1]);
                new ClientTaskForKV().executeOnExecutor(AyncThreadObj, Destination_Emulator,K,V);
                new ClientTaskForKV().executeOnExecutor(AyncThreadObj, xdr[0],K,V);
                new ClientTaskForKV().executeOnExecutor(AyncThreadObj, xdr[1], K, V);
            }


        return uri;
    }

//---------------------------------------------DELETE-------------------------------//
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v("TO DELETE", selection);

        if (mySucc_Str == null && myPred_Str == null) {
            Del_SingleEmulatorQuery(selection);
        }
        else {
            Del_RingEmulatorQuery(selection);
        }
        return 0;
    }

    public void Del_SingleEmulatorQuery(String selection){

        if(selection.equals("\"*\"") || selection.equals("\"@\"")){

            for (String filename:staticContext.fileList()){
                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    fin.delete();
                } catch (Exception e) {
                    Log.v(TAG, "file delete failed");
                }
            }

        }   else {
            LocalSingle_QueryDelete(selection);
        }
    }

    public void LocalSingle_QueryDelete(String quer){     // sends out client tasks for delete at itself and it's replicas
            String[] xdr = coord_rep_map.get(myEmuId_Str);
            Log.e(TAG,quer+" to be deleted @"+myEmuId_Str+" "+xdr[0]+" "+xdr[1]);

            new ClientTask_DelReplica().executeOnExecutor(AyncThreadObj, myEmuId_Str,quer);
            new ClientTask_DelReplica().executeOnExecutor(AyncThreadObj, xdr[0],quer);
            new ClientTask_DelReplica().executeOnExecutor(AyncThreadObj, xdr[1],quer);
    }

    public void Rep_LocalSingle_QueryDelete(String quer){
        try {
            File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + quer);
            fin.delete();

        } catch (Exception e) {
            Log.v(TAG, "delete failed");
        }
    }

    public void Del_RingEmulatorQuery(String selection){

        if(selection.equals("\"*\"") || selection.equals("\"@\"")){

            if(selection.equals("\"@\"")){
                DelAt_InRing();
            }

            if(selection.equals("\"*\"")){
                for (String g:AllEmus){
                    if(!g.equals("empty")){
                        new ClientTask_AtToOneEmu().executeOnExecutor(AyncThreadObj, g,"del@");
                    }
                }
            }

        }
        else {
            String Destination_Emulator = WhereToSend(selection);
            Log.e(TAG," this delete for "+selection+ " to process @"+Destination_Emulator);



                String[] xdr = coord_rep_map.get(Destination_Emulator);
                Log.e(TAG,selection+" to be deleted @"+myEmuId_Str+" "+xdr[0]+" "+xdr[1]);

                new ClientTask_DelReplica().executeOnExecutor(AyncThreadObj, myEmuId_Str,selection);
                new ClientTask_DelReplica().executeOnExecutor(AyncThreadObj, xdr[0],selection);
                new ClientTask_DelReplica().executeOnExecutor(AyncThreadObj, xdr[1],selection);

        }
    }

    public void DelAt_InRing(){
        for (String filename:staticContext.fileList()){
            if(!filename.equals("DummyKey")){
                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    fin.delete();
                } catch (Exception e) {
                    Log.v(TAG, "file delete failed");
                }
            }
        }
    }

//---------------------------------------------QUERY-------------------------------//

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder) {



        MatrixCursor OC = new MatrixCursor(new String[] { "key", "value"});

        Log.v("QUERY", selection);
        String queryArr[] = {selection, myEmuId_Str};

        if (mySucc_Str == null && myPred_Str == null) {         // case when only one emulator
            OC = (MatrixCursor) Final_ProcessSingleEmulatorQuery(selection);
        }
        else {
            OC = (MatrixCursor) ProcessQueryInRing(queryArr);
        }


        return  OC;
    }

    public Cursor Final_ProcessSingleEmulatorQuery(String selection){
        MatrixCursor LocalCursor = new MatrixCursor(new String[] { "key", "value"});

        if(selection.equals("\"*\"") || selection.equals("\"@\"")){

            for (String filename:staticContext.fileList()){
                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    FileInputStream fis = new FileInputStream(fin);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    String line = br.readLine();
                    br.close();
                    LocalCursor.addRow(new String[] {filename,line});

                } catch (Exception e) {
                    Log.v(TAG, "file read failed");
                }

            }

        }
        else {

            try{
                File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + selection);
                FileInputStream fis = new FileInputStream(fin);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                String line = br.readLine();
                br.close();
                LocalCursor.addRow(new String[] {selection,line});
            } catch (Exception e) {
                Log.v(TAG, "file read failed");
            }

        }

        return LocalCursor;
    }

    public Cursor ProcessQueryInRing(String[] queryArr){
        MatrixCursor LocalCursor = new MatrixCursor(new String[] { "key", "value"});

        if(queryArr[0].equals("\"*\"")){
            StarCountExp = 5;
            // send @ to all present emulators and collect at this one
            for (String g:AllEmus){
                if(g.equals(myEmuId_Str)){
                    Log.e(TAG,"* > @ updated for "+myEmuId_Str);
                    String[] StOneArr = StringProcessAtInRing();
                    if(!ForStar_Emu_To_KeyMap.containsKey(myEmuId_Str)){
                        ForStar_Emu_To_KeyMap.put(myEmuId_Str,StOneArr[0]);
                        ForStar_Emu_To_ValMap.put(myEmuId_Str,StOneArr[1]);
                    }
                }
                else{
                    new ClientTask_AtToOneEmu().executeOnExecutor(AyncThreadObj, g, "\"@\"", myEmuId_Str);
                }
            }



            while (ForStar_Emu_To_ValMap.keySet().size() < StarCountExp){
                try{
                    Log.e(TAG,"waiting for * - thread sleep for 100ms");
                    Thread.sleep(100L);
                }catch (InterruptedException e){
                    Log.e(TAG,"InterruptedException");
                    e.printStackTrace();
                }
            }

            LocalCursor = (MatrixCursor)BuildStarCursor(ForStar_Emu_To_KeyMap,ForStar_Emu_To_ValMap);
            ForStar_Emu_To_KeyMap.clear(); // to enable logic for same key again
            ForStar_Emu_To_ValMap.clear(); // to enable logic for same key again

        }

        if(queryArr[0].equals("\"@\"")){
            LocalCursor = (MatrixCursor)ProcessAtInRing();
        }

        if(!queryArr[0].equals("\"*\"") && !queryArr[0].equals("\"@\"")) {
            QueryToSingleEXC.put(queryArr[0],3);
            ArrayList<String> empt = new ArrayList<>();
            QueryToCsrStringMap.put(queryArr[0],empt);

            String Destination_Emulator = WhereToSend(queryArr[0]);
            Log.e(TAG,"destination of query is "+Destination_Emulator);

            if (Destination_Emulator.equals(myEmuId_Str)){
                LocalCursor = (MatrixCursor)RespondToQuery(queryArr[0]);
            }
            else {
                Log.e(TAG,"query sending to "+Destination_Emulator);
                String[] xdr = coord_rep_map.get(Destination_Emulator);

                new ClientTask_ToDestination().executeOnExecutor(AyncThreadObj, Destination_Emulator,queryArr[0],myEmuId_Str);
                new ClientTask_ToDestination().executeOnExecutor(AyncThreadObj, xdr[0],queryArr[0],myEmuId_Str);
                new ClientTask_ToDestination().executeOnExecutor(AyncThreadObj, xdr[1],queryArr[0],myEmuId_Str);



                while (QueryToCsrStringMap.get(queryArr[0]).size() < QueryToSingleEXC.get(queryArr[0])){
                    try{
                        Log.e(TAG,"single query Thread sleep for 100ms");
                        Thread.sleep(100L);
                    }catch (InterruptedException e){
                        Log.e(TAG,"InterruptedException");
                        e.printStackTrace();
                    }
                }

                /*
                String[] hh = {"","","0"};
                ArrayList<String> str = QueryToCsrStringMap.get(queryArr[0]);
                for (String f : str){
                    String[] hth = f.split("~");
                    if( Integer.parseInt(hth[2]) > Integer.parseInt(hh[2])){
                        hh = hth;
                    }
                }
                */
                Log.e(TAG,"SQEXC: "+QueryToSingleEXC.get(queryArr[0])+" for "+queryArr[0]);
                String[] hh = QueryToCsrStringMap.get(queryArr[0]).get(0).split("~");

                MatrixCursor csr2 = new MatrixCursor(new String[] { "key", "value"});
                csr2.addRow(new String[]{hh[0], hh[1]});

                LocalCursor = csr2;


                QueryToCsrStringMap.remove(queryArr[0]);
                QueryToSingleEXC.remove(queryArr[0]);
            }
        }

        return LocalCursor;
    }

    public Cursor BuildStarCursor(HashMap<String,String> kmap,HashMap<String,String> vmap){
        MatrixCursor LocalCursor = new MatrixCursor(new String[] { "key", "value"});
        String NetKey="";
        String NetVal="";
        ArrayList<String> KeyList = new ArrayList<>();
        ArrayList<String> ValList = new ArrayList<>();
        for(String emu:kmap.keySet()){
            NetKey = NetKey+kmap.get(emu);
            NetVal = NetVal+vmap.get(emu);
        }
        for (String retval: NetKey.split("~")){
            KeyList.add(retval);
        }
        for (String retval: NetVal.split("~")){
            ValList.add(retval);
        }
        for(int i=0;i<KeyList.size();i++){
            LocalCursor.addRow(new String[] {KeyList.get(i),ValList.get(i)});
        }
        return LocalCursor;
    }

    public Cursor ProcessAtInRing(){
        MatrixCursor LCursor = new MatrixCursor(new String[] { "key", "value"});
        for (String filename:staticContext.fileList()){
            if (!filename.equals("DummyKey")){
                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    FileInputStream fis = new FileInputStream(fin);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    String line = br.readLine();
                    br.close();
                    LCursor.addRow(new String[] {filename,line});
                } catch (Exception e) {
                    Log.v(TAG, "file read failed");
                }
            }

        }
        return LCursor;
    }

    public  Cursor RespondToQuery(String query){
        try{
            File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + query);

            FileInputStream fis = new FileInputStream(fin);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = br.readLine();
            br.close();
            MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
            mc.addRow(new String[] {query,line});

            return mc;
        } catch (Exception e) {
            Log.v(TAG, "file read failed");
        }
        return null;
    }


    public String[] StringProcessAtInRing(){
        String[] STArr = {"",""};

        for (String filename:staticContext.fileList()){
            if(!filename.equals("DummyKey")){

                try{
                    File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + filename);
                    FileInputStream fis = new FileInputStream(fin);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                    String line = br.readLine();
                    br.close();
                    STArr[0] = STArr[0]+"~"+filename;
                    STArr[1] = STArr[1]+"~"+line;

                } catch (Exception e) {
                    Log.v(TAG, "file read failed");
                }
            }

        }
        return STArr;
    }


    public  String[] StringRespondToQuery(String query){
        String[] arr = new String[2];
        try{
            File fin = new File(staticContext.getFilesDir().getAbsolutePath() + File.separator + query);
            Log.v("file path check", fin.toString());

            FileInputStream fis = new FileInputStream(fin);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = br.readLine();
            Log.v("check line read", line);
            br.close();

            arr[0] = query;
            arr[1] = line;


        } catch (Exception e) {
            Log.v(TAG, "file read failed");
        }
        return  arr;
    }





//---------------------------------------------METHODS-------------------------------//

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static String returnHash(String x){
        String hashedVal = "";

        try {
            hashedVal = genHash(x);
        }catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException @ SimpleDynamoActivity");
            e.printStackTrace();
        }

        return hashedVal;
    }

    public void InsertHere(String K, String V){
        try {
            FileOutputStream outputStream;
            outputStream = staticContext.openFileOutput(K, Context.MODE_PRIVATE);
            outputStream.write(V.getBytes());
            Log.e(TAG,"File write done for "+K+" - "+V+" @ "+myEmuId_Str);
            if (!key_to_version.contains(K)){
                key_to_version.put(K,1);
            } else {
                key_to_version.put(K,key_to_version.get(K)+1);
            }
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            e.printStackTrace();
        }
    }

    public String WhereToSend(String quer){
        String querhash = returnHash(quer);
        String Lowhash = returnHash(LowestEmu);
        String Hihash = returnHash(HighestEmu);
        String returnval="";
        if (querhash.compareTo(Hihash)>0 || querhash.compareTo(Lowhash)<=0){
            returnval = LowestEmu;
        }
        else{
            for (int i =1; i<=4;i++){
                if(!AllEmus[i].equals("empty") && querhash.compareTo(returnHash(AllEmus[i]))<=0){
                    returnval = AllEmus[i];
                    break;
                }
            }
        }
        return returnval;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public static String[] keyString_valString_Gen(String emuid){

        if (emu_to_Key_val_log_map.get(emuid).isEmpty()){
            Log.e(TAG,myEmuId_Str+" has nothing for "+emuid);
            String[] arrt ={"",""};
            emu_to_Key_val_log_map.get(emuid).clear();
            return arrt;
        } else {
            String k = "";
            String v = "";
            Set<String> keys = emu_to_Key_val_log_map.get(emuid).keySet();
            for (String x: keys){
                k = k+"~"+x;
                v = v+"~"+emu_to_Key_val_log_map.get(emuid).get(x);
            }
            k = k.substring(1,k.length());
            v = v.substring(1,v.length());
            String[] arr ={k,v};
            emu_to_Key_val_log_map.get(emuid).clear();
            return arr;
        }

    }

    public void LongStringInsert(String K,String V){
        String[] kk = K.split("~");
        String[] vv = V.split("~");
        for (int i=0;i<kk.length;i++){
            InsertHere(kk[i],vv[i]);
        }
    }



}
