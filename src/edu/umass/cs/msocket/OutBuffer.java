/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.logging.Level;
import edu.umass.cs.msocket.logger.MSocketLogger;
// import sun.jvm.hotspot.utilities.soql.MapScriptObject;

/**
 * This class implements the Output buffer of MSocket. Data is stored in the
 * outbput buffer, before it is sent out to the other side.
 *
 * @author aditya
 */

public class OutBuffer
{
  public static final int MAX_OUTBUFFER_SIZE = 30000000;                                   // 30MB

  ArrayList<ByteBuffer>       sbuf               = null;

  /*
   * Same as ConnectionInfo.dataSendSeq, this is the sequence number of the next
   * byte to be sent.
   */
  int                    dataSendSeq        = 0;

  /*
   * dataBaseSeq is the sequence number of the lowest byte that has not been
   * cumulatively acknowledged by the receiver.
   */
  int                    dataBaseSeq        = 0;

  /*
   * dataStartSeq is the sequence number of first byte in the buffer. It may be
   * less than dataBaseSeq as dataStartSeq is advanced only when dataBaseSeq
   * moves beyond the first whole buffer in the buffer list sbuf.
   */
  int                    dataStartSeq       = 0;

  boolean                 Close_Obuffer      = false;                                      // indicates
                                                                                            // that
                                                                                            // outbuffer
                                                                                            // contains
                                                                                            // close
                                                                                            // mesg,
                                                                                            // actually
                                                                                            // it
                                                                                            // doesn't,
                                                                                            // flag
                                                                                            // indicates
                                                                                            // that
                                                                                            // at
                                                                                            // the
                                                                                            // end
                                                                                            // of
                                                                                            // sending
                                                                                            // all
                                                                                            // data
                                                                                            // through
                                                                                            // outbuffer
                                                                                            // also
                                                                                            // send
                                                                                            // close
                                                                                            // messgae
  boolean                 ACK_Obuffer        = false;                                      // similar
                                                                                            // for
                                                                                            // ACK

  OutBuffer()
  {
    sbuf = new ArrayList<ByteBuffer>();
  }

  public synchronized boolean add(byte[] src, int offset, int length)
  {
    if (src.length - (offset + length) < 0)
      return false;
    // FIXME: may need to improve here
    if ((getOutbufferSize() + length) - (java.lang.Runtime.getRuntime().maxMemory() / 2) > 0)
    {

      MSocketLogger.getLogger().log(Level.FINE,"Local write fail JVM Heap memeory threshold exceeded");
      return false;
    }
    ByteBuffer dst = ByteBuffer.wrap(src,offset,length);
    sbuf.add(dst);
    dataSendSeq += length;
    return true;
  }

  public synchronized int getOutbufferSize()
  {
      int sizeinbytes = 0;
      for(int i=0;i< sbuf.size();i++){
          sizeinbytes += sbuf.get(i).remaining();
      }
    return sizeinbytes;
  }

  public boolean add(byte[] b)
  {
    return add(b, 0, b.length);
  }

  public synchronized boolean ack(int ack)
  {
      if (ack - dataBaseSeq <= 0 || ack - dataSendSeq > 0)
          return false;
      dataBaseSeq = ack;
      while ((ack - dataStartSeq > 0) && sbuf.size() > 0) {
          int length_buffer = sbuf.get(0).remaining();
          if (ack - (dataStartSeq + length_buffer) >= 0) {
              sbuf.remove(0);
              dataStartSeq += length_buffer;
          } else
              break;
      }
      return true;
  }

  public void freeOutBuffer()
  {
      int curStart = dataStartSeq;
      int freeIndex = -1;
      for (int i = 0; i < sbuf.size(); i++)
      {
          ByteBuffer b = sbuf.get(i).duplicate();
          int length_buffer = b.limit()-b.position();
          if (curStart + length_buffer - dataBaseSeq > 0)
          {
              freeIndex = i;
              break;
          }
          curStart += length_buffer;
      }
      dataStartSeq = curStart;

      int i = 0;
      while (i < freeIndex)
      {
          sbuf.remove(0); // remove the first element, as element slides left
          i++;
      }
  }

  public synchronized void releaseOutBuffer()
  {
    sbuf.clear();
  }

  public synchronized int getDataBaseSeq()
  {
    return dataBaseSeq;
  }

  public synchronized void setDataBaseSeq(int bs)
  {
    if ((bs - dataStartSeq >= 0) && (bs - dataSendSeq <= 0) && (bs - dataBaseSeq > 0))
    {
      dataBaseSeq = bs;
      freeOutBuffer();
    }
  }

  public synchronized byte[] getUnacked()
  {
      if (dataSendSeq - dataBaseSeq <= 0)
          return null;
      ByteBuffer buf = ByteBuffer.wrap(new byte[(int) (dataSendSeq - dataBaseSeq)]);
      long curStart = dataStartSeq;
      for (int i = 0; i < sbuf.size(); i++)
      {
          ByteBuffer b = sbuf.get(i);
          int length_buffer = b.limit()-b.position();

          if ((curStart + length_buffer) - dataBaseSeq > 0)
          {
              int srcPos = (int) Math.max(0, dataBaseSeq - curStart);
              byte[] t = b.array();
              String data_in_string = new String(t);
              buf.put(t, srcPos, t.length - srcPos);
          }
          curStart += length_buffer;
      }
      if (buf.array().length == 0){

          MSocketLogger.getLogger().log(Level.FINE,"BaseSeq = {0}, SendSeq = {1}", new Object[]{this.dataBaseSeq,this.dataSendSeq});
      }
      return buf.array();
  }

  public synchronized ArrayList<ByteBuffer> getDataFromOutBuffer(int startSeqNum, int EndSeqNum)
  {
      if (EndSeqNum - startSeqNum <= 0)
          return null;
//    ArrayList<ByteBuffer> ret_array = ByteBuffer.wrap(new byte[(int) (EndSeqNum - startSeqNum)]);
      ArrayList<ByteBuffer> ret_array = new ArrayList<ByteBuffer>();

      {
          int byte_range = EndSeqNum - startSeqNum;
          int curStart = dataStartSeq;
          for (int i = 0; i < sbuf.size(); i++)
          {
              ByteBuffer b = sbuf.get(i);
              int length_buffer = b.remaining();
              if ((curStart + length_buffer) - startSeqNum > 0)
              {
                  int srcPos = (int) Math.max(0, startSeqNum - curStart);
                  int copy = 0;
                  if (byte_range - (length_buffer - srcPos) > 0)
                  {
                      copy = (length_buffer - srcPos);
                      byte[] t = b.array();
                      ByteBuffer temp = ByteBuffer.wrap(t,srcPos,copy);
                      ret_array.add(temp);
                      byte_range = byte_range - copy;
                  }
                  else
                  {
                      copy = byte_range;
                      byte[] t = b.array();
                      ByteBuffer temp = ByteBuffer.wrap(t,srcPos,copy);
                      temp.limit(copy);
                      ret_array.add(temp);
                      break;
                  }
              }
              curStart += length_buffer;
          }
      }
      return ret_array;
  }

  public String toString()
  {
    String s = "[";
    s += "dataSendSeq=" + dataSendSeq + ", ";
    s += "dataBaseSeq=" + dataBaseSeq + ", ";
    s += "dataStartSeq=" + dataStartSeq + ", ";
    s += "numbufs=" + sbuf.size();
    s += "]";

    return s;
  }

  public static void main(String[] args)
  {

      OutBuffer ob = new OutBuffer();
      byte[] b1 = "Test1".getBytes();
      byte[] b2 = "Test2".getBytes();
      byte[] b3 = "Test3".getBytes();
      byte[] b4 = "Test4".getBytes();

    ob.add(b1);
    System.out.println(ob.toString());
    ob.add(b2);
    System.out.println(ob.toString());
    ob.add(b3);
    System.out.println(ob.toString());
    ob.add(b4);
    System.out.println(ob.toString());

    ArrayList<ByteBuffer> bbuffer = ob.getDataFromOutBuffer(0,8);
    for(int i=0;i<bbuffer.size();i++){
      byte[] ss = new byte[bbuffer.get(i).remaining()];
      bbuffer.get(i).get(ss);
      System.out.println(new String(ss));
    }
    ob.ack(5);
    String data_in_string = new String(ob.getUnacked());
    System.out.println(data_in_string);
    System.out.println(ob.toString());
//    ob.ack(4);
//    System.out.println(ob.toString());
//    data_in_string = new String(ob.getUnacked());
//    System.out.println(data_in_string);
//    data_in_string = new String(ob.getDataFromOutBuffer(0,5));
//    System.out.println(data_in_string);



//    ByteBuffer bb = ByteBuffer.allocate(8);
//    bb.wrap(b2);
//    bb.putInt(5);
//    bb.putInt(10);
//    System.out.println(bb.position());
//    System.out.println(bb.capacity());
//    bb.flip();
//    System.out.println(bb.position());
//    System.out.println(bb.capacity());
//    System.out.println(bb.toString());
  }
}
