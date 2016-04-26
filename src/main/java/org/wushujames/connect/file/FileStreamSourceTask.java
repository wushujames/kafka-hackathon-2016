/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.wushujames.connect.file;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String filename;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;

    private String shardId;
    private String streamArn;
    private String nextItr = null;
    
    private Long streamOffset;
    private String tableName;
    private String region;
    
    private static AmazonDynamoDBStreamsClient streamsClient = 
            new AmazonDynamoDBStreamsClient(new ProfileCredentialsProvider());


    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        shardId = props.get("shardId");
        streamArn = props.get("streamArn");
        tableName = props.get("tableName");
        region = props.get("region");
        
        String streamsEndpoint = "https://streams.dynamodb.us-west-2.amazonaws.com";
        streamsClient.setEndpoint(streamsEndpoint);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //  
        SchemaBuilder builder = SchemaBuilder.struct().name(region + "." + tableName);
        // hardcode the datatype for the table primary key
        // IRL, I'd have to do a "describe" of sorts on the table, to see what
        // type the primary key is.
        builder.field("name", Schema.STRING_SCHEMA);
        Schema connectKeySchema = builder.build();
        Struct connectKey = new Struct(connectKeySchema);
        
        Map<String, String> sourcePartition = new HashMap<String, String>();
        sourcePartition.put("tableName", tableName);
        sourcePartition.put("shardId", shardId);
        
        // Get an iterator for the current shard

        if (nextItr == null) {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
            .withStreamArn(streamArn)
            .withShardId(shardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
            GetShardIteratorResult getShardIteratorResult = 
                    streamsClient.getShardIterator(getShardIteratorRequest);
            nextItr = getShardIteratorResult.getShardIterator();
        }
        
        // Use the iterator to read the data records from the shard
        GetRecordsResult getRecordsResult = 
            streamsClient.getRecords(new GetRecordsRequest().
                withShardIterator(nextItr));
        List<Record> records = getRecordsResult.getRecords();
        System.out.println("Getting records...");

        List<SourceRecord> results = new ArrayList<SourceRecord>();
        for (Record record : records) {
            System.out.println(record);
            StreamRecord streamRecord = record.getDynamodb();

            System.out.println("Record key:");
            Map<String, AttributeValue> dynamoKey = streamRecord.getKeys();
            
            for (Entry<String, AttributeValue> entry : dynamoKey.entrySet()) {
                System.out.println(String.format("key: %s, value: %s",
                        entry.getKey(),
                        entry.getValue()));
                AttributeValue s = entry.getValue();
                if (s.getS() != null) {
                    String sVal = s.getS();
                    connectKey.put(entry.getKey(), sVal);
                }
            }

            System.out.println("Record value:");
            Map<String, AttributeValue> data = streamRecord.getNewImage();
            Map<String, String> connectRecord = new HashMap<String, String>();

            for (Entry<String, AttributeValue> entry : data.entrySet()) {
                System.out.println(String.format("key: %s, value: %s",
                        entry.getKey(),
                        entry.getValue()));
                connectRecord.put(entry.getKey(), entry.getValue().getS());
            }
            
            String sequenceNumber = streamRecord.getSequenceNumber();
            System.out.println("shardSequenceNumber: " + sequenceNumber);
            
            Map<String, String> connectOffset = new HashMap<String, String>();
            connectOffset.put("sequenceNumber", sequenceNumber);
            
            SourceRecord rec = new SourceRecord(
                    sourcePartition, 
                    connectOffset,
                    tableName, 
                    null, //partition 
                    connectKeySchema,
                    connectKey,
                    null,
                    connectRecord);
            results.add(rec);
        }
        nextItr = getRecordsResult.getNextShardIterator();

        return results;
//
//        
//        
//        if (stream == null) {
//            try {
//                stream = new FileInputStream(filename);
//                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
//                if (offset != null) {
//                    Object lastRecordedOffset = offset.get(POSITION_FIELD);
//                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
//                        throw new ConnectException("Offset position is the incorrect type");
//                    if (lastRecordedOffset != null) {
//                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
//                        long skipLeft = (Long) lastRecordedOffset;
//                        while (skipLeft > 0) {
//                            try {
//                                long skipped = stream.skip(skipLeft);
//                                skipLeft -= skipped;
//                            } catch (IOException e) {
//                                log.error("Error while trying to seek to previous offset in file: ", e);
//                                throw new ConnectException(e);
//                            }
//                        }
//                        log.debug("Skipped to offset {}", lastRecordedOffset);
//                    }
//                    streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
//                } else {
//                    streamOffset = 0L;
//                }
//                reader = new BufferedReader(new InputStreamReader(stream));
//                log.debug("Opened {} for reading", logFilename());
//            } catch (FileNotFoundException e) {
//                log.warn("Couldn't find file for FileStreamSourceTask, sleeping to wait for it to be created");
//                synchronized (this) {
//                    this.wait(1000);
//                }
//                return null;
//            }
//        }
//
//        // Unfortunately we can't just use readLine() because it blocks in an uninterruptible way.
//        // Instead we have to manage splitting lines ourselves, using simple backoff when no new data
//        // is available.
//        try {
//            final BufferedReader readerCopy;
//            synchronized (this) {
//                readerCopy = reader;
//            }
//            if (readerCopy == null)
//                return null;
//
//            ArrayList<SourceRecord> records = null;
//
//            int nread = 0;
//            while (readerCopy.ready()) {
//                nread = readerCopy.read(buffer, offset, buffer.length - offset);
//                log.trace("Read {} bytes from {}", nread, logFilename());
//
//                if (nread > 0) {
//                    offset += nread;
//                    if (offset == buffer.length) {
//                        char[] newbuf = new char[buffer.length * 2];
//                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
//                        buffer = newbuf;
//                    }
//
//                    String line;
//                    do {
//                        line = extractLine();
//                        if (line != null) {
//                            log.trace("Read a line from {}", logFilename());
//                            if (records == null)
//                                records = new ArrayList<>();
//                            records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, VALUE_SCHEMA, line));
//                        }
//                        new ArrayList<SourceRecord>();
//                    } while (line != null);
//                }
//            }
//
//            if (nread <= 0)
//                synchronized (this) {
//                    this.wait(1000);
//                }
//
//            return records;
//        } catch (IOException e) {
//            // Underlying stream was killed, probably as a result of calling stop. Allow to return
//            // null, and driving thread will handle any shutdown if necessary.
//        }
//        return null;
    }

    private String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)
                streamOffset += newStart;
            return result;
        } else {
            return null;
        }
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    log.trace("Closed input stream");
                }
            } catch (IOException e) {
                log.error("Failed to close FileStreamSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }
}
