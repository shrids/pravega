/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.common.netty.InvalidMessageException;
import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.util.CircularBuffer;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

/**
 * Manages buffering and provides a synchronus to {@link AsyncSegmentInputStream}
 * @see SegmentInputStream
 */
@RequiredArgsConstructor
class SegmentInputStreamImpl extends SegmentInputStream {

	private final AsyncSegmentInputStream asyncInput;
	private static final int READ_LENGTH = SegmentOutputStream.MAX_WRITE_SIZE;
	@GuardedBy("$lock")
	private final CircularBuffer buffer = new CircularBuffer(2 * SegmentOutputStream.MAX_WRITE_SIZE);
	@GuardedBy("$lock")
	private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
	private long offset = 0;
	@GuardedBy("$lock")
	private boolean receivedEndOfSegment = false;
	@GuardedBy("$lock")
	private Future<SegmentRead> outstandingRequest = null;

	@Override
	@Synchronized
	public void setOffset(long offset) {
		this.offset = offset;
		buffer.clear();
		receivedEndOfSegment = false;
	}

	@Override
	@Synchronized
	public long getOffset() {
		return offset;
	}

	/**
	 * @see com.emc.pravega.stream.impl.segment.SegmentInputStream#read()
	 */
	@Override
	@Synchronized
	public ByteBuffer read() throws EndOfSegmentException {
		issueRequestIfNeeded();
		if (outstandingRequest.isDone() || buffer.dataAvailable() < WireCommands.TYPE_PLUS_LENGTH_SIZE) {
			handleRequest();
		}
		if (buffer.dataAvailable() <= 0 && receivedEndOfSegment) {
			throw new EndOfSegmentException();
		}
		headerReadingBuffer.clear();
		offset += buffer.read(headerReadingBuffer);
		headerReadingBuffer.flip();
		int type = headerReadingBuffer.getInt();
		int length = headerReadingBuffer.getInt();
		if (type != WireCommandType.APPEND.getCode()) {
		    throw new InvalidMessageException("Event was of wrong type: "+type);
		}
		if (length < 0 || length > WireCommands.MAX_WIRECOMMAND_SIZE) {
		    throw new InvalidMessageException("Event of invalid length: "+length);
		}
	    ByteBuffer result = ByteBuffer.allocate(length);
	    offset += buffer.read(result);
		while (result.hasRemaining()) {
		    handleRequest();
		    offset += buffer.read(result);
		}
		result.flip();
		return result;
	}

	private void handleRequest() {
		SegmentRead segmentRead;
		try {
			segmentRead = outstandingRequest.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
		if (segmentRead.getData().hasRemaining()) {
			buffer.fill(segmentRead.getData());
		}
		if (segmentRead.isEndOfSegment()) {
			receivedEndOfSegment = true;
		}
		if (!segmentRead.getData().hasRemaining()) {
			outstandingRequest = null;
			issueRequestIfNeeded();
		}
	}

	/**
	 * @return If there is enough room for another request, and we aren't already waiting on one
	 */
	private void issueRequestIfNeeded() {
		if (!receivedEndOfSegment && outstandingRequest == null && buffer.capacityAvailable() > READ_LENGTH) {
			outstandingRequest = asyncInput.read(offset + buffer.dataAvailable(), READ_LENGTH);
		}
	}

	@Override
	@Synchronized
	public void close() {
		asyncInput.close();
	}

}
