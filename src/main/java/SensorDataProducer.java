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
	 * Before running the code: Fill in your AWS access credentials in the
	 * provided credentials file template, and be sure to move the file to the
	 * default location (~/.aws/credentials) where the sample code will load the
	 * credentials from.
	 * https://console.aws.amazon.com/iam/home?#security_credential
	 *
	 * WARNING: To avoid accidental leakage of your credentials, DO NOT keep the
	 * credentials file in your source directory.
	 */

	private static AmazonKinesisClient kinesis;

	private static final String STREAM_NAME = "teststream";

	private static final String ANOMALY_STREAM_NAME = "anomalystream";

	private static final String ACCESS_KEY = "AKIAJBNNVIGONEHV7TTA";

	private static final String SECRET_KEY = "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";

	private static void init() throws Exception {
		kinesis = new AmazonKinesisClient(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		kinesis.setRegion(usWest2);
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

			// Create a stream. The number of shards determines the provisioned
			// throughput.
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

		// Initialize the values for cities, lat, long and sensors

		int nyTrack = 0, sfTrack = 0, azTrack = 0, cityTrack = 0;
		double trackLat = 0, trackLong = 0, tracktemp = 0, precipitation = 0, ws = 0, humidity = 0, avgNy = 55.5,
				avgSf = 57.3, avgAz = 75.05, avghum=0;
		int co = 0, no = 0, co2 = 0, o3 = 0;
		String anomalyTrack = "";
		int[] sensorTrack = {};

		// String
		String cities[] = { "New York", "San Fransisco", "Arizona City" };
		int cityLen = cities.length;

		// New York
		double[] nyLatitudes = { 40.712784, 40.759011, 40.749599, 40.754931 };
		double nyLat = 40.712784;
		double[] nyLongitudes = { -74.005941, -73.984472, -73.998936, -73.984019 };
		double nyLong = -74.005941;
		int nyLen = nyLatitudes.length;

		// San Fransisco
		double[] sfLatitudes = { 37.809085, 37.772066, 37.806053, 37.745346 };
		double sfLat = 37.774929;
		double[] sfLongitudes = { -122.412040, -122.431153, -122.410331, -122.420074 };
		double sfLong = -122.419416;
		int sfLen = sfLatitudes.length;

		// Arizona City
		double[] azLatitudes = { 32.901836, 32.879502, 32.755893, 32.755896 };
		double azLat = 32.755893;
		double[] azLongitudes = { -111.742252, -111.757352, -111.670958, -111.554844 };
		double azLong = -111.670958;
		int azLen = azLatitudes.length;

		// Anomaly
		String anomalies[] = { "fire", "cyclone", "None" };

		// create a calendar
		Calendar calendar = Calendar.getInstance();

		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String timestamp = sdf.format(calendar.getTime());

		System.out.println(timestamp);

		int seconds = 0; // Data will be generated for these many seconds -
							// approx 4 hours enough for our demo...

		// Write records to the stream until this program is aborted.
		while (seconds++ < 14400) {

			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setStreamName(STREAM_NAME);

			anomalyTrack = anomalies[(int) Math.floor(Math.random() * anomalies.length)];

			if (anomalyTrack.equalsIgnoreCase("None")) {
				cityTrack = (cityTrack % cityLen)+1;
				if (cities[cityTrack-1].equalsIgnoreCase("New York")) {
					trackLat = nyLat;
					trackLong = nyLat;
					tracktemp = avgNy + Math.random() * (2 + 2) - 2;
					avghum = 71 + Math.random() * (4 + 4) - 4;
					calendar.add(Calendar.SECOND, 1);
					timestamp = sdf.format(calendar.getTime());
					System.out.println(timestamp);

				}
				if (cities[cityTrack-1].equals("San Fransisco")) {
					trackLat = sfLat;
					trackLong = sfLong;
					tracktemp = avgSf + Math.random() * (2 + 2) - 2;
					avghum = 82 + Math.random() * (4 + 4) - 4;
				}

				if (cities[cityTrack-1] == "Arizona City") {
					trackLat = azLat;
					trackLong = azLong;
					tracktemp = avgAz + Math.random() * (2 + 2) - 2;
					avghum = 74 + Math.random() * (4 + 4) - 4;
				}

				JSONObject sensorData = new JSONObject();
				sensorData.put("city", cities[cityTrack-1]);
				sensorData.put("detectedAnomaly", anomalyTrack);
				sensorData.put("latitude", trackLat);
				sensorData.put("longitude", trackLong);
				sensorData.put("temperature", tracktemp);
				sensorData.put("humidity", avghum);
				sensorData.put("precipitation", 28 + Math.random() * (1 + 1) - 1);
				sensorData.put("CO2", Math.round(Math.random() * (50 - 0)));
				sensorData.put("CO", Math.round(Math.random() * (50 - 0)));
				sensorData.put("NO", Math.round(Math.random() * (50 - 0)));
				sensorData.put("O3", Math.round(Math.random() * (50 - 0)));
				sensorData.put("timestamp", timestamp);
				sensorData.put("windSpeed", (8 + Math.random() * (2 + 2) - 2));
				sensorData.put("windDirection", (Math.random() * (360 - 0) + 0));

				System.out.println(sensorData.toString());

				// Put Record
				putRecordRequest.setData(ByteBuffer.wrap(sensorData.toString().getBytes()));
				putRecordRequest.setPartitionKey("partitionKey-shardId-000000000000");
				PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);

				System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
						putRecordRequest.getPartitionKey(), putRecordResult.getShardId(),
						putRecordResult.getSequenceNumber());

				Thread.sleep(1000);
			} /*
				 * else {
				 * 
				 * //Setting values according to anomalies
				 * 
				 * 
				 * //Sensor Data JSONObject sensorData = new JSONObject();
				 * sensorData.put("latitude", trackLat);
				 * sensorData.put("longitude", trackLong);
				 * sensorData.put("temperature", tracktemp);
				 * sensorData.put("humidity", 87 + Math.random() * (2 + 2) - 2);
				 * sensorData.put("precipitation", 28 + Math.random() * (1 + 1)
				 * - 1); sensorData.put("CO2", Math.round(Math.random() * (50 -
				 * 0))); sensorData.put("CO", Math.round(Math.random() * (50 -
				 * 0))); sensorData.put("NO", Math.round(Math.random() * (50 -
				 * 0))); sensorData.put("O3", Math.round(Math.random() * (50 -
				 * 0))); sensorData.put("timestamp", timestamp);
				 * sensorData.put("windSpeed", (Math.random() * (50 - 0) + 0));
				 * sensorData.put("windDirection", (Math.random() * (360 - 0) +
				 * 0));
				 * 
				 * //Put Record for a city
				 * putRecordRequest.setData(ByteBuffer.wrap(sensorData.toString
				 * ().getBytes ())); putRecordRequest.setPartitionKey(
				 * "partitionKey-shardId-000000000000"); PutRecordResult
				 * putRecordResult = kinesis.putRecord(putRecordRequest);
				 * 
				 * System.out.
				 * printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n"
				 * , putRecordRequest.getPartitionKey(),
				 * putRecordResult.getShardId(),
				 * putRecordResult.getSequenceNumber());
				 * 
				 * //Put a record to anomaly stream
				 * putRecordRequest.setStreamName(ANOMALY_STREAM_NAME);
				 * PutRecordResult putRecordResultAnomaly =
				 * kinesis.putRecord(putRecordRequest); System.out.
				 * printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n"
				 * , putRecordRequest.getPartitionKey(),
				 * putRecordResultAnomaly.getShardId(),
				 * putRecordResultAnomaly.getSequenceNumber());
				 * 
				 * 
				 * 
				 * Thread.sleep(1000);
				 * 
				 * }
				 */

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
				// ask for no more than 10 shards at a time -- this is an
				// optional parameter
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
