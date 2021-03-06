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

import java.util.ArrayList;
import java.lang.Math;
import edu.umass.cs.msocket.logger.MSocketLogger;
import java.util.logging.Level;

/**
 * This class implements the Inbuffer of the MSocket. Out of order data is read
 * from the input stream and stored in the inbuffer.
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class InBufferOutOrder
{

  ArrayList<InBufferStorageChunk> rbuf               = null;

  int                            dataReadSeq        = 0;                                                 // assuming
                                                                                                           // that
                                                                                                           // data
                                                                                                           // starts
                                                                                                           // from
                                                                                                           // 0
                                                                                                           // seq
                                                                                                           // num
  int                            byteRecvInInbuffer = 0;                                                 // mainly
                                                                                                           // for
                                                                                                           // ideal
                                                                                                           // case
                                                                                                           // of
                                                                                                           // multipath

  InBufferOutOrder()
  {
    rbuf = new ArrayList<InBufferStorageChunk>();
  }

  public synchronized boolean putInBuffer(InBufferStorageChunk Obj)
  {
    byteRecvInInbuffer += Obj.chunkSize; // may not be accurate if there are
                                         // retransmissions due to migration or
                                         // otherwise

    if( dataReadSeq - (Obj.startSeqNum+Obj.chunkSize) >= 0)
	{
		return false;
	}

    insertSorted(Obj);
    return true;
  }

  public int getInBuffer(byte[] b)
  {
    return getInBuffer(b, 0, b.length);
  }

  public boolean isInBufferData()
  {
    for (int i = 0; i < rbuf.size(); i++)
    {
      InBufferStorageChunk CurChunk = rbuf.get(i);

      if ((dataReadSeq - CurChunk.startSeqNum >= 0) && (dataReadSeq - (CurChunk.startSeqNum + CurChunk.chunkSize) < 0)) // required
                                                                                                                // for
                                                                                                                // considering
                                                                                                                // holes
                                                                                                                // ,FIXME:
                                                                                                                // may
                                                                                                                // not
                                                                                                                // have
                                                                                                                // checked
                                                                                                                // for
                                                                                                                // repeated
                                                                                                                // data
      {
        return true;
      }
    }
    return false;
  }

  public synchronized int getInBuffer(byte[] b, int offset, int length)
  {

    int numread = 0;

    if (rbuf.size() > 0)
    {

    MSocketLogger.getLogger().log(Level.INFO,"Inside the getinbuffer");
    }
    for (int i = 0; i < rbuf.size(); i++)
    {

      InBufferStorageChunk CurChunk = rbuf.get(i);
      if ((dataReadSeq - CurChunk.startSeqNum >= 0) && (dataReadSeq - (CurChunk.startSeqNum + CurChunk.chunkSize) < 0)) // required
                                                                                                                // for
                                                                                                                // considering
                                                                                                                // holes
                                                                                                                // ,FIXME:
                                                                                                                // may
                                                                                                                // not
                                                                                                                // have
                                                                                                                // checked
                                                                                                                // for
                                                                                                                // repeated
                                                                                                                // data
      {
        int srcPos = (int) Math.max(0, dataReadSeq - CurChunk.startSeqNum);
        // FIXME: check for long to int conversion
        int cpylen = CurChunk.chunkSize - srcPos;
        int actlen = 0;
        if ((numread + cpylen) - length > 0)
        {
          actlen = length - numread;
        }
        else
        {
          actlen = cpylen;
        }
        // MSocketLogger.getLogger().info("this is the actlen"+ actlen);
        System.arraycopy(CurChunk.chunkData, srcPos, b, offset+numread, actlen);
        numread += actlen;
        dataReadSeq += actlen;
        if (numread - length >= 0)
          break;
      }
    }
    freeInBuffer();
    return numread;
  }

  /**
	 * Checks if the given data seq num is for in ordered data,
	 * if that is the case then it is returned directly from stream
	 * and not stored in input buffer.
	 *
	 * @return
	 */
	public synchronized boolean isDataInOrder(int chunckStartSeq, int chunkLength) {

		// if dataReadSeq is in between this chunk data, then it is in-order
		if( ( dataReadSeq - chunckStartSeq >= 0) && ( dataReadSeq - (chunckStartSeq + chunkLength) < 0 ) )
		{
			return true;
		}
		return false;
	}

	/**
	 * Copy data read from stream to the app buffer. Also updates the dataReadSeqNum
	 * It bypasses the storing of data in input buffer
	 * @param readFromStream
	 * @param startSeqNum
	 * @param appBuffer
	 * @param offset
	 * @param appLen
	 */
	public synchronized int copyOrderedDataToAppBuffer(byte[] readFromStream, int startSeqNum,
			int chunkLen, byte[] appBuffer, int offset, int appLen)
	{
		if(chunkLen > 0)
		{
		  MSocketLogger.getLogger().log(Level.FINE,"copyOrderedDataToAppBuffer: startSeqNum: {0}, chunkLen: {1}, offset: {2}, appLen: {3}, readFromStream[0]: {4}", new Object[]{startSeqNum,chunkLen,offset,appLen,readFromStream[0]});
		}
		int actualCopied =0;
		if( (dataReadSeq - startSeqNum >= 0) && (dataReadSeq - (startSeqNum+chunkLen) < 0) )
		{
			int srcPos = (int)Math.max(0,dataReadSeq-startSeqNum);
			//FIXME: check for long to int conversion
			int cpylen=chunkLen-srcPos;
			actualCopied = cpylen;
			System.arraycopy(readFromStream, srcPos, appBuffer, offset , cpylen );
			dataReadSeq+=cpylen;
		}
		return actualCopied;
	}

	public int getDataReadSeqNum() {
		return dataReadSeq;
	}

	/**
	 * return the size of inbuffer in number of elements
	 * @return
	 */
	public long getInBufferSize() {
		return rbuf.size();
	}

	/**
	 * Inserts chunk in in buffer in sorted order
	 * @param Obj
	 */
	private void insertSorted(InBufferStorageChunk Obj)
	{
		int i=0;
		// inserting from reverse, as it might require less iterations.
		// may eventually need to be replaces with heap
		for(i=rbuf.size()-1; i>=0; i--) {
			if( rbuf.get(i).startSeqNum - Obj.startSeqNum < 0) // may need to do overlap check also
			{
				break;
			}
		}
		rbuf.add(i+1, Obj);
	}

	private void freeInBuffer()
	{
		while(rbuf.size() > 0)
		{
			InBufferStorageChunk CurChunk = rbuf.get(0);

			// required for considering holes ,FIXME: may not have checked for repeated data
			if( (dataReadSeq - (CurChunk.startSeqNum+CurChunk.chunkSize)>= 0 ) )
			{
				//remove the first element, as element slides left
				InBufferStorageChunk removed = rbuf.remove(0);
				removed.chunkData = null;
				removed = null;
			} else
			{
				break;
			}
		}
	}
}
