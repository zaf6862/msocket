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
import java.util.Arrays;
import java.util.logging.Level;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class defines the data message format, every data is sent encapsulated
 * in the data message. The class also defines the serialization and
 * de-serialization of the data message
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DataMessage
{
  public static final int      DATA_MESG     = 0;
  public static final int      DATA_ACK_REQ  = 2;

  public static final int      FIN           = 1;
  public static final int      ACK           = 3;
  public static final int      ACK_FIN       = 4;
  public static final int      DATA_ACK_REP  = 5;
  public static final int      KEEP_ALIVE    = 6;
  public static final int      CLOSE_FP    	 = 7;
  public static final int      CLOSE_FP_ACK  = 8;

  public static final String[] Mesg_Type_Str = {"DATA_MESG", "FIN", "DATA_ACK_REQ", "ACK", "ACK_FIN"};

  public static final int      HEADER_SIZE   = (Integer.SIZE * 4) / 8 + Long.SIZE / 8;
  final int                    sendSeq;
  final int                    ackSeq;
  final int                    length;
  final int                    Type;
  //num of bytes receieved on the socket on which ACK will be sent. Required by the sending side for Socket performance
  final long                   RecvdBytes;
  final ArrayList<ByteBuffer>  msg;

  // stores the beginning position of data copy in the given buffer.
  private final int arrayCopyOffset;

  /*
   * If the byte[] argument b is null or longer than the specified length
   * argument l, then length is set to l; else length is shortened to b.length.
   * We need to allow length>0 and msg==null in the case of a header-only
   * DataMessage.
   */
  public DataMessage(int Type, int s, int a, int l, long RecvdBytes, ArrayList<ByteBuffer> b, int offset)
  {
    this.Type = Type;
    sendSeq = s;
    ackSeq = a;
    int len = 0;
    if(b != null){
      for(int i=0;i<b.size();i++){
        len = len + b.get(i).remaining();
      }
    }

    if (b == null || l <= len)
      length = l;
    else
      length = len;
    this.RecvdBytes = RecvdBytes;
    arrayCopyOffset = offset;
    msg = b;
  }

  public static int sizeofHeader()
  {
    return HEADER_SIZE;
  }

  public int size()
  {
    return sizeofHeader() + length;
  }

  public ArrayList<ByteBuffer> getBytes()
  {
    ArrayList<ByteBuffer> ret_arr = new ArrayList<ByteBuffer>();
    ByteBuffer header = ByteBuffer.allocate(DataMessage.HEADER_SIZE);
    header.putInt(Type);
    header.putInt(sendSeq);
    header.putInt(ackSeq);
    header.putInt(length);
    header.putLong(RecvdBytes);
    header.flip();
    ret_arr.add(header);
    if(msg != null){
      for(int i=0;i<msg.size();i++){
        ret_arr.add(msg.get(i));
      }
    }
    return ret_arr;
  }

  /*
   * This method assumes that the byte[] argument b exactly contains a
   * DataMessage object, i.e., there is no excess bytes beyond the header and
   * the message body. If that is not the case, it will return null.
   */
//  public static DataMessage getDataMessage(byte[] b)
//  {
//    if (b == null || b.length < DataMessage.HEADER_SIZE)
//      return null;
//    ByteBuffer buf = ByteBuffer.wrap(b);
//    DataMessage dm = new DataMessage(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), buf.getLong(),
//        Arrays.copyOfRange(b, DataMessage.HEADER_SIZE, b.length), 0);
//    return dm;
//  }

  public static DataMessage getDataMessageHeader(byte[] b)
  {
    if (b == null || b.length < DataMessage.HEADER_SIZE)
      return null;
    ByteBuffer buf = ByteBuffer.wrap(b, 0, DataMessage.HEADER_SIZE);
    return new DataMessage(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), buf.getLong(), null, -1);
  }

  public String toString()
  {
    String s = "";
    s += "Type: "+ Type + ",sendSeq: " + sendSeq + ",ackSeq: " + ackSeq + ",length: " + length;
    return s;
  }

  public static void main(String[] args)
  {
    byte[] b = "Testing the waters to get a feel".getBytes();
    ByteBuffer bb = ByteBuffer.wrap(b);
    ArrayList<ByteBuffer> bb_al = new ArrayList<ByteBuffer>();
    bb_al.add(bb);
    int len = 0;
    for(int i=0;i<bb_al.size();i++){
      len = len + bb_al.get(i).remaining();
    }
    DataMessage dm = new DataMessage(0, 23, 19, len, 1, bb_al, 0);
    ArrayList<ByteBuffer> enc = dm.getBytes();
    for(int i=0;i<enc.size();i++){
      byte[] temp = new byte[enc.get(i).remaining()];
      enc.get(i).get(temp);
      System.out.println(new String(temp));
    }


  }
}
