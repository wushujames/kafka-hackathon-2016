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
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import java.io.*;
import java.nio.ByteBuffer;
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

    public static final String REGION_CONFIG = "region";
    public static final String TABLE_CONFIG = "table";
    public static final String STREAM_ARN_CONFIG = "streamArn";
    public static final String SHARD_ID_CONFIG = "shardId";

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
    private Schema connectKeySchema;
    private Set<String> dynamoKeyNames = new HashSet<String>();

    
    private AmazonDynamoDBStreamsClient streamsClient;


    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        shardId = props.get(SHARD_ID_CONFIG);
        streamArn = props.get(STREAM_ARN_CONFIG);
        tableName = props.get(TABLE_CONFIG);
        region = props.get(REGION_CONFIG);
        
        Regions awsRegion = Regions.fromName(region);
        streamsClient = 
                new AmazonDynamoDBStreamsClient(new ProfileCredentialsProvider()).withRegion(awsRegion);
        
        // to get the table key schema
        AmazonDynamoDBClient dynamoDBClient = 
                new AmazonDynamoDBClient(new ProfileCredentialsProvider()).withRegion(awsRegion);

        DescribeTableResult describeTableResult = dynamoDBClient.describeTable(tableName);

        // get the schema for the table key
        TableDescription tableDesc = describeTableResult.getTable();
        
        // map from field name to field type
        List<AttributeDefinition> defns = tableDesc.getAttributeDefinitions();
        Map<String, ScalarAttributeType> fieldToType = new HashMap<String, ScalarAttributeType>();
        for (AttributeDefinition defn : defns) {
            fieldToType.put(defn.getAttributeName(),
                    ScalarAttributeType.fromValue(defn.getAttributeType()));
        }
        
        // build the connect schema
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName + "." + region);
        
        List<KeySchemaElement> keySchemas = tableDesc.getKeySchema();
        for (KeySchemaElement keySchema : keySchemas) {
            // get field type
            String fieldName = keySchema.getAttributeName();
            dynamoKeyNames.add(fieldName);
            
            ScalarAttributeType fieldType = fieldToType.get(fieldName);
            switch (fieldType) {
            case B:
                builder.field(fieldName, Schema.BYTES_SCHEMA);
                break;
            case N:
                // Per http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
                // "If number precision is important, you should pass numbers to DynamoDB using strings that you convert from number type."
                builder.field(fieldName, Schema.STRING_SCHEMA);
                break;
            case S:
                builder.field(fieldName, Schema.STRING_SCHEMA);
                break;
            default:
                throw new RuntimeException(String.format("table has unknown field type. region = %s, table = %s, field = %s, type = %s",
                        region, tableName, fieldName, fieldType.toString()
                        ));

            }
            
        }
        connectKeySchema = builder.build();

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //  
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
            
            // fill out the primary key
            for (Entry<String, AttributeValue> entry : dynamoKey.entrySet()) {
                String fieldName = entry.getKey();
                AttributeValue fieldValue = entry.getValue();
                System.out.println(String.format("key: %s, value: %s",
                        fieldName,
                        fieldValue));
                
                if (dynamoKeyNames.contains(fieldName)) { 
                    if (fieldValue.getS() != null) {
                        String sVal = fieldValue.getS();
                        connectKey.put(fieldName, sVal);
                    } else if (fieldValue.getB() != null) {
                        ByteBuffer bVal = fieldValue.getB();
                        byte[] arr = bVal.array();
                        connectKey.put(fieldName, Arrays.copyOf(arr, arr.length));
                    } else if (fieldValue.getN() != null) {
                        String nVal = fieldValue.getN();
                        connectKey.put(fieldName, nVal);
                    } else {
                        throw new RuntimeException(String.format("key field type is unsupported, region = %s, table = %s, field = %s, value = %s",
                                region, tableName, fieldName, entry));
                    }
                }
            }

            System.out.println("Record value:");
            Map<String, AttributeValue> data = streamRecord.getNewImage();
            Map<String, Object> connectRecord = new HashMap<String, Object>();

            for (Entry<String, AttributeValue> entry : data.entrySet()) {
                String fieldName = entry.getKey();
                AttributeValue fieldValue = entry.getValue();
                System.out.println(String.format("key: %s, value: %s",
                        fieldName,
                        fieldValue));
                if (fieldValue.getS() != null) {
                    connectRecord.put(fieldName, fieldValue.getS());
                } else if (fieldValue.getB() != null) {
                    ByteBuffer bVal = fieldValue.getB();
                    byte[] arr = bVal.array();
                    connectRecord.put(fieldName, Arrays.copyOf(arr, arr.length));
                } else if (fieldValue.getN() != null) {
                    String nVal = fieldValue.getN();
                    connectRecord.put(fieldName, nVal);
                } else {
                    throw new RuntimeException(String.format("record field type is unsupported, region = %s, table = %s, field = %s, value = %s",
                            region, tableName, fieldName, entry));

                }
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

        Thread.sleep(1000);

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
