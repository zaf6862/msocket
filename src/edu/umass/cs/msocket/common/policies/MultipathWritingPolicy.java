/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.common.policies;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Vector;
import java.util.logging.Level;
import edu.umass.cs.msocket.ByteRangeInfo;
import edu.umass.cs.msocket.ConnectionInfo;
import edu.umass.cs.msocket.DataMessage;
import edu.umass.cs.msocket.SocketInfo;
import edu.umass.cs.msocket.logger.MSocketLogger;


/**
 *
 * This class defines the parent class for different
 * multipath write policies
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class MultipathWritingPolicy {

	protected ConnectionInfo cinfo			= null;


	/**
	 * child class implements the write call according to the
	 * policy.
	 * @throws IOException
	 */
	public abstract void writeAccordingToPolicy(byte[] b, int offset, int length, int MesgType) throws IOException;


	  /**
	   * @param tempDataSendSeqNum
	   * @param Obj
	   * @throws IOException
	   */
	  protected void handleMigrationInMultiPath(SocketInfo Obj) throws IOException
	  {

	    MSocketLogger.getLogger().log(Level.FINE, "handleMigrationInMultiPath called");
	    // if queue size is > 0 then it means that there is a non-blocking
	    // write pending and it should be sent first, instead of migration data
	    if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
	    {
	      attemptSocketWrite(Obj);
	      return;
	    }


	    MSocketLogger.getLogger().log(Level.FINE,"HandleMigrationInMultiPath SocektId {0}",Obj.getSocketIdentifer() );
	    cinfo.multiSocketRead();
	    int dataAck = (int) cinfo.getDataBaseSeq();

	    MSocketLogger.getLogger().log(Level.FINE,"DataAck from other side {0}", dataAck);
	    Obj.byteInfoVectorOperations(SocketInfo.QUEUE_REMOVE, dataAck, -1);

	    @SuppressWarnings("unchecked")
	    Vector<ByteRangeInfo> byteVect = (Vector<ByteRangeInfo>) Obj.byteInfoVectorOperations(SocketInfo.QUEUE_GET, -1, -1);

	    for (int i = 0; i < byteVect.size(); i++)
	    {
	      ByteRangeInfo currByteR = byteVect.get(i);

	      if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
	      {
	        // setting the point to start from next time
	        Obj.setHandleMigSeqNum(currByteR.getStartSeqNum());
	        attemptSocketWrite(Obj);
	        return;
	      }

	      cinfo.multiSocketRead();
	      dataAck = (int) cinfo.getDataBaseSeq();

	      // already acknowledged, no need to send again
	      if (dataAck - (currByteR.getStartSeqNum() + currByteR.getLength()) > 0)
	      {
	        continue;
	      }

	      // if already sent
	      if ((currByteR.getStartSeqNum() + currByteR.getLength()) - Obj.getHandleMigSeqNum() < 0)
	      {
	        continue;
	      }

	      ArrayList<ByteBuffer> buf = cinfo.getDataFromOutBuffer(currByteR.getStartSeqNum(),
	          currByteR.getStartSeqNum() + currByteR.getLength());
			int len = 0;
			for(int iter=0;iter<buf.size();iter++){
				len = len + buf.get(iter).remaining();
			}
	      int arrayCopyOffset = 0;
	      DataMessage dm = new DataMessage(DataMessage.DATA_MESG, (int) currByteR.getStartSeqNum(), cinfo.getDataAckSeq(),
	          len, 0, buf, arrayCopyOffset);
	      ArrayList<ByteBuffer> writebuf = dm.getBytes();

	      Obj.queueOperations(SocketInfo.QUEUE_PUT, writebuf);
	      attemptSocketWrite(Obj);

	    }

	    Obj.setneedToReqeustACK(false);

	    MSocketLogger.getLogger().log(Level.FINE,"HandleMigrationInMultiPath Complete");
	  }


	  protected void attemptSocketWrite(SocketInfo Obj) throws IOException
	  {

	    Obj.getDataChannel().configureBlocking(false);
	    //TAG: come back to this
	    ArrayList<ByteBuffer> writebuf = (ArrayList<ByteBuffer>) Obj.queueOperations(SocketInfo.QUEUE_GET, null);

		  int len = 0;
		  for(int i=0;i<writebuf.size();i++){
			  len = len + writebuf.get(i).remaining();
		  }
		  long startTime = System.currentTimeMillis();
		  for(int i=0;i<writebuf.size();i++){
			  while(writebuf.get(i).hasRemaining()){
				  Obj.getDataChannel().write(writebuf.get(i));
			  }
		  }
		  int gotWritten = len;

//	    int curroffset = Obj.currentChunkWriteOffsetOper(-1, SocketInfo.VARIABLE_GET);
//	    ByteBuffer bytebuf = ByteBuffer.allocate(writebuf.length - curroffset);
//
//	    bytebuf.put(writebuf, curroffset, writebuf.length - curroffset);
//	    bytebuf.flip();
//	    long startTime = System.currentTimeMillis();
//	    int gotWritten = Obj.getDataChannel().write(bytebuf);

	    if (gotWritten > 0)
	    {

	      MSocketLogger.getLogger().log(Level.FINE,"Wrote {0} bytes, buffer length is {1}, SocketID {2}", new Object[]{gotWritten,len,Obj.getSocketIdentifer()});
	      Obj.currentChunkWriteOffsetOper(gotWritten, SocketInfo.VARIABLE_UPDATE);
	    }

	    if (Obj.currentChunkWriteOffsetOper(-1, SocketInfo.VARIABLE_GET) == len) // completely
	                                                                                         // written,
	                                                                                         // time
	                                                                                         // to
	                                                                                         // remove
	                                                                                         // from
	                                                                                         // head
	                                                                                         // of
	                                                                                         // queue
	                                                                                         // and
	                                                                                         // reset
	                                                                                         // it
	    {

	      MSocketLogger.getLogger().log(Level.FINE,"currentChunkWriteOffset: {0}", len);
	      Obj.currentChunkWriteOffsetOper(0, SocketInfo.VARIABLE_SET);
	      Obj.queueOperations(SocketInfo.QUEUE_REMOVE, null);
	    }

	    long endTime = System.currentTimeMillis();

	    Obj.getDataChannel().configureBlocking(false);
	    if (gotWritten > 0){
						MSocketLogger.getLogger().log(Level.FINE,"Using SocketID {0}, Remote IP {1}, for writing. Time taken: {2}", new Object[]{Obj.getSocketIdentifer(),Obj.getSocket().getInetAddress(),(endTime - startTime)});
	    }
	  }
}
