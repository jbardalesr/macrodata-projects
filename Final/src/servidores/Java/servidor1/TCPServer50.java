package sparkstreaming;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServer50 {
    private String message;
    
    int nrcli = 0;

    public static final int SERVERPORT = 9998;
    private OnMessageReceived messageListener = null;
    private boolean running = false;
    TCPServerThread50[] sendclis = new TCPServerThread50[10];

    PrintWriter mOut;
    BufferedReader in;
    
    ServerSocket serverSocket;

    //el constructor pide una interface OnMessageReceived
    public TCPServer50(OnMessageReceived messageListener) {
        this.messageListener = messageListener;
    }
    
    public OnMessageReceived getMessageListener(){
        return this.messageListener;
    }
    
    public void sendMessageTCPServer(String message){
        for (int i = 1; i <= nrcli; i++) {
            sendclis[i].sendMessage(message);
            System.out.println("ENVIANDO A CLIENTE " + (i));
        }
    }
    

    
    public void run(){
        running = true;
        try{
            System.out.println("TCP Server"+"S : Connecting..."+SERVERPORT );
            serverSocket = new ServerSocket(SERVERPORT);
            
            while(running){
                Socket client = serverSocket.accept();
                System.out.println("TCP Server Receiving...");
                nrcli++;
                System.out.println("Nueva solicitud " + nrcli);
                sendclis[nrcli] = new TCPServerThread50(client,this,nrcli,sendclis);
                Thread t = new Thread(sendclis[nrcli]);
                t.start();
                System.out.println("Nuevo cliente conectado:"+ nrcli+" cliente(s) en total");
                String row="Not data";
                int i = 0;
                BufferedReader csvReader;
                try {
                    csvReader = new BufferedReader(new FileReader("healthcare_clean_num.csv"));
                    while ((row = csvReader.readLine()) != null) {
                     //String[] data = row.split(",");
                     System.out.println(row);
                     sendMessageTCPServer(row);
                         i++;
                         if(i%1000==0){
                             try
                            {
                                Thread.sleep(1000);
                            }
                            catch(InterruptedException ex)
                            {
                                Thread.currentThread().interrupt();
                            }
                         }
                     }
                    System.out.println("Envío terminado");
                     csvReader.close();
                } catch (FileNotFoundException ex) {
                    System.err.println("sparkstreaming.TCPServer50.run()");//Logger.getLogger(Servidor50.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
        }catch( Exception e){
            System.out.println("Error"+e.getMessage());
        }finally{

        }
    }
    public  TCPServerThread50[] getClients(){
        return sendclis;
    } 

    public interface OnMessageReceived {
        public void messageReceived(String message);
    }
}
