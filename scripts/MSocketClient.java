import edu.umass.cs.msocket.FlowPath;
import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.mobility.MobilityManagerClient;

import java.io.InputStream;
import java.net.SocketException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;

public class MSocketClient implements Runnable {


    private static final int    LOCAL_PORT = 5556;
    private static final String LOCALHOST  = "127.0.0.1";
    private static DecimalFormat df = new DecimalFormat("0.00##");
    private static final int TOTAL_ROUND = 100;
    private static int numBytes = 1000000;

    public static Long calc_median(Long[] input){
      Arrays.sort(input);
      Long median;
      if (input.length % 2 == 0)
        median = ((long)input[input.length/2] + (long)input[input.length/2 - 1])/2;
      else
        median = (long) input[input.length/2];
      return median;
    }
    public static Long calc_avg(Long[] input){
            int len = input.length;
            Long sum = 0L;
            for(int i=0;i<len;i++){
                sum = sum  + input[i];
            }
            return sum/len;
    }
    public MSocketClient(){

    }
    public void run(){

      String serverIPOrName = LOCALHOST;
      int serverPort = LOCAL_PORT;
      int numRound = TOTAL_ROUND;
      int numOfBytes= numBytes;
      long median_time = 0;

      try{
        MSocket ms = new MSocket(InetAddress.getByName(serverIPOrName), serverPort);

        OutputStream os = ms.getOutputStream();
        InputStream is = ms.getInputStream();
        try {
          Thread.sleep(2000); // wait for 2 seconds for all connections
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try{
          ms.setTcpNoDelay(true);
          // System.out.println("Client is setting the tcp no delay option.");
        }catch(SocketException e){
          e.printStackTrace();
        }

        int current_round = 0;
        Long[]  transferTime  = new Long[numRound];
        while (current_round < numRound) {
            int numSent = numOfBytes;
            byte[] b = new byte[numSent];
            ByteBuffer dbuf = ByteBuffer.allocate(4);
            dbuf.putInt(numSent);
            byte[] bytes = dbuf.array();
            int numRead;
            int totalRead = 0;
            long start = System.currentTimeMillis();
            os.write(bytes);
            // System.out.println("CLIENT ----> Time taken to write is " + (System.currentTimeMillis() - start));
            long t = System.currentTimeMillis();
            do {
                numRead = is.read(b);
                if (numRead >= 0)
                    totalRead += numRead;

            } while (totalRead < numSent);
            // System.out.println("CLIENT ----> Time taken to read is " + (System.currentTimeMillis() - t));
            long elapsed = System.currentTimeMillis() - start;
            // System.out.println("CLIENT ----> Total Transfer time is " + elapsed);
            transferTime[current_round] = elapsed;
            current_round++;
        }

        os.write(-1);
        os.flush();

        median_time = calc_avg(transferTime);
        System.out.println(median_time);
        ms.close();


        return;

      }catch(Exception e){
        e.printStackTrace();
      }



    }
}
