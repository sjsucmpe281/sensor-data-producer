package main.java;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.util.json.*;

/**
 * Harish Kumar K V
 */
public class SensorDataProducer {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (~/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    private static AmazonKinesisClient kinesis;

    private static final String STREAM_NAME = "teststream";

    private static final String ACCESS_KEY = "AKIAIQTGQYLPNFGDRS2A";

    private static final String SECRET_KEY = "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";

    private static void init() throws Exception {
        kinesis = new AmazonKinesisClient(new BasicAWSCredentials (ACCESS_KEY, SECRET_KEY));
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        kinesis.setRegion (usWest2);
    }

    public static void main(String[] args) throws Exception {
        init();

        final Integer myStreamSize = 1;

        // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(STREAM_NAME);
        try {
            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
            System.out.printf("Stream %s has a status of %s.\n", STREAM_NAME, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. This sample will now exit.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(STREAM_NAME);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.printf("Stream %s does not exist. Creating it now.\n", STREAM_NAME);

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(STREAM_NAME);
            createStreamRequest.setShardCount(myStreamSize);
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(STREAM_NAME);
        }

        // List all of my streams.
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesis.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        // Print all of my streams.
        System.out.println("List of my streams: ");

        for (int i = 0; i < streamNames.size(); i++) {
            System.out.println("\t- " + streamNames.get(i));
        }

        System.out.printf("Putting records in stream : %s until this application is stopped...\n", STREAM_NAME);

        System.out.println("Press CTRL-C to stop.");

        // create a calendar
        Calendar calendar = Calendar.getInstance();

        java.text.SimpleDateFormat sdf =
                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String timestamp = sdf.format(calendar.getTime ());

        System.out.println(timestamp);

        int seconds = 0; // Data will be generated for these many seconds - approx 4 hours enough for our demo...

        // Write records to the stream until this program is aborted.
        while (seconds++ < 14400) {

            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(STREAM_NAME);

            JSONObject sensorData = new JSONObject ();
            sensorData.put ("latitude", 32.752949);
            sensorData.put("longitude", 111.671257);
            sensorData.put("temperature", 123.45);
            sensorData.put("humidity", 23.45);
            sensorData.put("precipitation",12.34);
            sensorData.put("CO2", 12.3);
            sensorData.put("CO", 14.56);
            sensorData.put("NO", 12.67);
            sensorData.put("O3", 19.7);
            sensorData.put("timestamp" , timestamp);
            sensorData.put("windSpeed", 123);
            sensorData.put("windDirection", 234);

            putRecordRequest.setData(ByteBuffer.wrap(sensorData.toString ().getBytes ()));
            putRecordRequest.setPartitionKey("partitionKey-shardId-000000000000");
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);

            System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                    putRecordRequest.getPartitionKey(),
                    putRecordResult.getShardId(),
                    putRecordResult.getSequenceNumber());

            Thread.sleep(1000);

            calendar.add (Calendar.SECOND, 1);
            timestamp = sdf.format(calendar.getTime ());
            System.out.println(timestamp);
        }
    }

    private static void waitForStreamToBecomeAvailable(String STREAM_NAME) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", STREAM_NAME);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(STREAM_NAME);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", STREAM_NAME));
    }
}
