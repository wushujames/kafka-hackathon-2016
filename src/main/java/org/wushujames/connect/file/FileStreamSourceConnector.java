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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class FileStreamSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";
    
    
    private String filename;
    private String topic;
    private String tableName;
    private String awsRegion;

    private static AmazonDynamoDBClient dynamoDBClient = 
            new AmazonDynamoDBClient(new ProfileCredentialsProvider());

    private static AmazonDynamoDBStreamsClient streamsClient = 
            new AmazonDynamoDBStreamsClient(new ProfileCredentialsProvider());


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        tableName = "test02";
        awsRegion = "us-west-2";

        String dynamoDbEndpoint = "https://dynamodb.us-west-2.amazonaws.com";
        String streamsEndpoint = "https://streams.dynamodb.us-west-2.amazonaws.com";

        dynamoDBClient.setEndpoint(dynamoDbEndpoint);  
        streamsClient.setEndpoint(streamsEndpoint);

    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        
        
        // Determine the Streams settings for the table
        DescribeTableResult describeTableResult = dynamoDBClient.describeTable(tableName);

        String myStreamArn = describeTableResult.getTable().getLatestStreamArn();
        
        StreamSpecification myStreamSpec = 
                describeTableResult.getTable().getStreamSpecification();
        
        System.out.println("Current stream ARN for " + tableName + ": "+ myStreamArn);
        System.out.println("Stream enabled: "+ myStreamSpec.getStreamEnabled());
        System.out.println("Update view type: "+ myStreamSpec.getStreamViewType());

        // get the shards
        DescribeStreamResult describeStreamResult = 
                streamsClient.describeStream(new DescribeStreamRequest()
                    .withStreamArn(myStreamArn));
        
        String streamArn = 
                describeStreamResult.getStreamDescription().getStreamArn();
        List<Shard> shards = 
                describeStreamResult.getStreamDescription().getShards();

        System.out.println("Number of shards: " + shards.size());
        
        for (Shard shard : shards) {
            Map<String, String> config = new HashMap<>();

            String shardId = shard.getShardId();
            config.put("streamArn", streamArn);
            config.put("shardId", shardId);
            System.out.println("streamArn: " + streamArn + ", shardId: " + shardId);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
    }
}
