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

	private static final String STREAM_NAME = "sensor-aggregation-data";

	private static final String ANOMALY_STREAM_NAME = "anomalystream";

	private static final String ACCESS_KEY = "";

	private static final String SECRET_KEY = "";

	private static void init() throws Exception {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider()
					.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (~/.aws/credentials), and is in valid format.",
					e);
		}
		kinesis = new AmazonKinesisClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		kinesis.setRegion(usWest2);
	}

	public static void main(String[] args) throws Exception {
		init();
		
		//Stream Check
		streamCheck(STREAM_NAME);
		streamCheck(ANOMALY_STREAM_NAME);

		/*final Integer myStreamSize = 1;

		// Describe the stream and check if it exists.
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(STREAM_NAME);
		try {
			StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
			System.out.printf("Stream %s has a status of %s.\n", STREAM_NAME, streamDescription.getStreamStatus());

			StreamDescription anomalyStreamDescription = kinesis.describeStream(describeStreamRequest)
					.getStreamDescription();
			System.out.printf("Stream %s has a status of %s.\n", ANOMALY_STREAM_NAME,
					anomalyStreamDescription.getStreamStatus());

			if ("DELETING".equals(streamDescription.getStreamStatus())) {
				System.out.println("Stream is being deleted. This sample will now exit.");
				System.exit(0);
			}

			if ("DELETING".equals(streamDescription.getStreamStatus())) {
				System.out.println("Stream is being deleted. This sample will now exit.");
				System.exit(0);
			}

			// Wait for the stream to become active if it is not yet ACTIVE.
			if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
				waitForStreamToBecomeAvailable(STREAM_NAME);
			}

			if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
				waitForStreamToBecomeAvailable(ANOMALY_STREAM_NAME);
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

			CreateStreamRequest createAnomalyStreamRequest = new CreateStreamRequest();
			createAnomalyStreamRequest.setStreamName(ANOMALY_STREAM_NAME);
			createAnomalyStreamRequest.setShardCount(myStreamSize);
			kinesis.createStream(createAnomalyStreamRequest);
			// The stream is now being created. Wait for it to become active.
			waitForStreamToBecomeAvailable(ANOMALY_STREAM_NAME);
		}*/

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
				avgSf = 57.3, avgAz = 75.05, avghum = 0, avgnyhum = 71, avgsfhum = 82, avgazhum = 74;
		int co = 0, no = 0, co2 = 0, o3 = 0;

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
		double sfLat = 37.809085;
		double[] sfLongitudes = { -122.412040, -122.431153, -122.410331, -122.420074 };
		double sfLong = -122.41204;
		int sfLen = sfLatitudes.length;

		// Arizona City
		double[] azLatitudes = { 32.901836, 32.879502, 32.755893, 32.755896 };
		double azLat = 32.752949;
		double[] azLongitudes = { -111.742252, -111.757352, -111.670958, -111.554844 };
		double azLong = -111.671257;
		int azLen = azLatitudes.length;

		// Anomaly
		String anomalies[] = { "fire", "cyclone" };
		String anomalyTrack1 = "", anomalyTrack2 = "", anomaly = "";
		int city1 = 0, city2 = 0, cityAnomalyTracker = 0;
		int sensor1 = 0, sensor2 = 0, sensorAnomalyTracker = 0;

		// Create a sensor map
		Map<String, List<Sensor>> sensorMap = generateSensorsMap();
		Sensor sensorTrack1 = null;
		Sensor sensorTrack2 = null;

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

			

			if (seconds % 10 != 0) {
				cityTrack = (cityTrack % cityLen) + 1;
				if (cities[cityTrack - 1].equalsIgnoreCase("New York")) {
					trackLat = nyLat;
					trackLong = nyLat;
					tracktemp = avgNy + Math.round((Math.random() * (2 + 2) - 2)*100)/100;
					avghum = avgnyhum + Math.round((Math.random() * (4 + 4) - 4)*10)/10;
					timestamp = sdf.format(calendar.getTime());
					System.out.println(timestamp);

				}
				if (cities[cityTrack - 1].equals("San Fransisco")) {
					trackLat = sfLat;
					trackLong = sfLong;
					tracktemp = avgSf + Math.round((Math.random() * (2 + 2) - 2)*100)/100;
					avghum = avgsfhum + Math.round((Math.random() * (4 + 4) - 4)*10)/10;
				}

				if (cities[cityTrack - 1] == "Arizona City") {
					trackLat = azLat;
					trackLong = azLong;
					tracktemp = avgAz +Math.round((Math.random() * (2 + 2) - 2)*100)/100;
					avghum = avgazhum + Math.round((Math.random() * (4 + 4) - 4)*10)/10;
				}

				JSONObject sensorData = new JSONObject();
				sensorData.put("city", cities[cityTrack - 1]);
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

				System.out.println(sensorData.toString() + "***********************" + seconds);

				// Put Record
				putRecordRequest.setData(ByteBuffer.wrap(sensorData.toString().getBytes()));
				putRecordRequest.setPartitionKey("partitionKey-shardId-000000000000");
				PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);

				System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
						putRecordRequest.getPartitionKey(), putRecordResult.getShardId(),
						putRecordResult.getSequenceNumber());

				Thread.sleep(1000);
			} else {

				// Setting values according to anomalies
				anomalyTrack1 = anomalies[(int) Math.floor(Math.random() * anomalies.length)];
				anomalyTrack2 = anomalies[(int) Math.floor(Math.random() * anomalies.length)];
				// Cities where anomaly is generated
				city1 = (cityAnomalyTracker % cityLen);
				city2 = ((cityAnomalyTracker + 1) % cityLen);
				cityAnomalyTracker = city2;

				// Choose City Sensors
				sensor1 = sensorAnomalyTracker % sensorMap.get(cities[city1]).size();
				sensor2 = (sensorAnomalyTracker + 1) % sensorMap.get(cities[city2]).size();
				sensorAnomalyTracker = sensor2;
				sensorTrack1 = sensorMap.get(cities[city1]).get(sensor1);
				sensorTrack2 = sensorMap.get(cities[city2]).get(sensor2);
				Sensor sensor = new Sensor();

				// latitude and longitude of sensors
				double lat = 0, lng = 0;

				while (seconds++ < 300) {
					cityTrack = (cityTrack % cityLen) + 1;
					JSONObject aggregationData = new JSONObject();
					JSONObject dataHeader = new JSONObject();

					if (cities[cityTrack - 1].equalsIgnoreCase("New York")
							&& (cities[city1].equalsIgnoreCase("New York")
									|| cities[city2].equalsIgnoreCase("New York"))) {

						// timestamp
						calendar.add(Calendar.SECOND, 1);
						timestamp = sdf.format(calendar.getTime());
						System.out.println(timestamp);

						// send anomaly data for anomaly
						if (cities[city1].equalsIgnoreCase("New York")) {
							anomaly = anomalyTrack1;
							sensor = sensorTrack1;
							lat = nyLatitudes[sensor1];
							lng = nyLongitudes[sensor1];
						} else {
							anomaly = anomalyTrack2;
							sensor = sensorTrack2;
							lat = nyLatitudes[sensor2];
							lng = nyLongitudes[sensor2];
						}
						JSONObject anomalyData = createAnomalyData(anomaly, avgNy, avgnyhum);
						anomalyData.put("city", "New York");
						anomalyData.put("timestamp", timestamp);

						// Aggregation Data
						aggregationData = anomalyData;
						aggregationData.put("latitude", nyLat);
						aggregationData.put("longitude", nyLong);

						// Anomaly lat long
						anomalyData.put("latitude", lat);
						anomalyData.put("longitude", lng);

						dataHeader.put("sensorName", sensor.getSensorName());
						dataHeader.put("sensorId", sensor.getSensorId());
						dataHeader.put("detectedAnomaly", anomaly);
						dataHeader.put("data", anomalyData);

					} else {
						
						// send aggregation data
						if (cities[cityTrack - 1].equalsIgnoreCase("New York")) {
							calendar.add(Calendar.SECOND, 1);
							timestamp = sdf.format(calendar.getTime());
							System.out.println(timestamp);
							aggregationData = createAggregationObject("New York", nyLat, nyLong, avgNy, avgnyhum,
									timestamp);
						}
					}

					if (cities[cityTrack - 1].equalsIgnoreCase("San Fransisco")
							&& (cities[city1].equalsIgnoreCase("San Fransisco")
									|| cities[city2].equalsIgnoreCase("San Fransisco"))) {
						// send anomaly data for anomaly
						if (cities[city1].equalsIgnoreCase("San Fransisco")) {
							anomaly = anomalyTrack1;
							sensor = sensorTrack1;
							lat = sfLatitudes[sensor1];
							lng = sfLongitudes[sensor1];

						} else {
							anomaly = anomalyTrack2;
							sensor = sensorTrack2;
							lat = sfLatitudes[sensor2];
							lng = sfLongitudes[sensor2];
						}
						JSONObject anomalyData = createAnomalyData(anomaly, avgSf, avgsfhum);
						anomalyData.put("city", "San Fransisco");
						anomalyData.put("timestamp", timestamp);

						// Aggregation Data
						aggregationData = anomalyData;
						aggregationData.put("latitude", sfLat);
						aggregationData.put("longitude", sfLong);

						// Anomaly lat long
						anomalyData.put("latitude", lat);
						anomalyData.put("longitude", lng);

						dataHeader.put("sensorName", sensor.getSensorName());
						dataHeader.put("sensorId", sensor.getSensorId());
						dataHeader.put("detectedAnomaly", anomaly);
						dataHeader.put("data", anomalyData);
					} else {
						// send aggregation data
						if (cities[cityTrack - 1].equalsIgnoreCase("San Fransisco")) {
							aggregationData = createAggregationObject("San Fransisco", sfLat, sfLong, avgSf, avgsfhum,
									timestamp);
						}
					}

					if (cities[cityTrack - 1].equalsIgnoreCase("Arizona City")
							&& (cities[city1].equalsIgnoreCase("Arizona City")
									|| cities[city2].equalsIgnoreCase("Arizona City"))) {
						// send anomaly data for anomaly
						if (cities[city1].equalsIgnoreCase("Arizona City")) {
							anomaly = anomalyTrack1;
							sensor = sensorTrack1;
							lat = azLatitudes[sensor1];
							lng = azLongitudes[sensor1];

						} else {
							anomaly = anomalyTrack2;
							sensor = sensorTrack2;
							lat = azLatitudes[sensor2];
							lng = azLongitudes[sensor2];
						}
						JSONObject anomalyData = createAnomalyData(anomaly, avgAz, avgazhum);
						anomalyData.put("city", "Arizona City");
						anomalyData.put("timestamp", timestamp);

						// Aggregation Data
						aggregationData = anomalyData;
						aggregationData.put("latitude", azLat);
						aggregationData.put("longitude", azLong);

						// Anomaly lat long
						anomalyData.put("latitude", lat);
						anomalyData.put("longitude", lng);

						dataHeader.put("sensorName", sensor.getSensorName());
						dataHeader.put("sensorId", sensor.getSensorId());
						dataHeader.put("detectedAnomaly", anomaly);
						dataHeader.put("data", anomalyData);
					} else {
						// send aggregation data
						if (cities[cityTrack - 1].equalsIgnoreCase("Arizona City")) {
						aggregationData = createAggregationObject("Arizona City", azLat, azLong, avgAz, avgazhum,
								timestamp);
						}

					}

					System.out.println(dataHeader.toString() + "***********************" + seconds);
					System.out.println(aggregationData.toString() + "***********************" + seconds);

					// Put records in kinesis
					if (dataHeader.length() != 0) {
						PutRecordRequest putAnomlayRecordRequest = new PutRecordRequest();
						putAnomlayRecordRequest.setStreamName(ANOMALY_STREAM_NAME);
						// Put Record
						putAnomlayRecordRequest.setData(ByteBuffer.wrap(dataHeader.toString().getBytes()));
						putAnomlayRecordRequest.setPartitionKey("shardId-000000000000");
						PutRecordResult putAnomalyRecordResult = kinesis.putRecord(putAnomlayRecordRequest);

						System.out.printf(
								"Successfully put anomaly record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
								putRecordRequest.getPartitionKey(), putAnomalyRecordResult.getShardId(),
								putAnomalyRecordResult.getSequenceNumber());

					}

					// Put Record
					putRecordRequest.setData(ByteBuffer.wrap(aggregationData.toString().getBytes()));
					putRecordRequest.setPartitionKey("partitionKey-shardId-000000000000");
					PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);

					System.out.printf(
							"Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
							putRecordRequest.getPartitionKey(), putRecordResult.getShardId(),
							putRecordResult.getSequenceNumber());

					Thread.sleep(1000);

				}

			}

		}
	}

	private static JSONObject createAggregationObject(String city, double latitude, double longitude, double avgTemp,
			double avgHum, String timestamp) throws JSONException {

		JSONObject sensorData = new JSONObject();
		sensorData.put("city", city);
		sensorData.put("latitude", latitude);
		sensorData.put("longitude", longitude);
		sensorData.put("temperature", avgTemp + Math.random() * (2 + 2) - 2);
		sensorData.put("humidity", avgHum + Math.random() * (4 + 4) - 4);
		sensorData.put("precipitation", 28 + Math.random() * (1 + 1) - 1);
		sensorData.put("CO2", Math.round(Math.random() * (50 - 0)));
		sensorData.put("CO", Math.round(Math.random() * (50 - 0)));
		sensorData.put("NO", Math.round(Math.random() * (50 - 0)));
		sensorData.put("O3", Math.round(Math.random() * (50 - 0)));
		sensorData.put("timestamp", timestamp);
		sensorData.put("windSpeed", (8 + Math.random() * (2 + 2) - 2));
		sensorData.put("windDirection", (Math.random() * (360 - 0) + 0));

		return sensorData;
	}

	private static JSONObject createAnomalyData(String anomaly, double avgTemp, double avgHum) throws JSONException {
		JSONObject anomalyData = new JSONObject();
		if (anomaly.equalsIgnoreCase("fire")) {
			anomalyData.put("temperature", Math.round((Math.random() * (1200 - 400) + 400) * 100) / 100);
			anomalyData.put("CO2", Math.round(Math.random() * (500 - 100) + 100));
			anomalyData.put("CO", Math.round(Math.random() * (500 - 100) + 100));
			anomalyData.put("NO", Math.round(Math.random() * (500 - 100) + 100));
			anomalyData.put("O3", Math.round(Math.random() * (500 - 100) + 100));
			anomalyData.put("windSpeed", Math.round((8 + Math.random() * (2 + 2) - 2)*10)/10);
			anomalyData.put("windDirection", Math.round((Math.random() * (360 - 0) + 0)*100)/100);
			anomalyData.put("precipitation", 28 + Math.round((Math.random() * (1 + 1) - 1)*10)/10);
			anomalyData.put("humidity", avgHum + Math.round((Math.random() * (4 + 4) - 4)*10)/10);
		}

		if (anomaly.equalsIgnoreCase("cyclone")) {
			anomalyData.put("temperature", avgTemp + Math.round((Math.random() * (2 + 2) - 2)*100)/100);
			anomalyData.put("humidity", 100);
			anomalyData.put("windSpeed", Math.round((Math.random() * (300 - 69) + 69) * 10) / 10);
			anomalyData.put("precipitation", Math.round((Math.random() * (197 - 50) + 50) * 10) / 10);
			anomalyData.put("CO2", Math.round(Math.random() * (50 - 0)));
			anomalyData.put("CO", Math.round(Math.random() * (50 - 0)));
			anomalyData.put("NO", Math.round(Math.random() * (50 - 0)));
			anomalyData.put("O3", Math.round(Math.random() * (50 - 0)));
			anomalyData.put("windDirection", Math.round((Math.random() * (360 - 0) + 0)*100)/100);
		}

		return anomalyData;
	}

	private static Map<String, List<Sensor>> generateSensorsMap() {

		Map<String, List<Sensor>> sensorMap = new HashMap<String, List<Sensor>>();

		// New York Sensors
		List<Sensor> nySensors = new ArrayList<>();
		Sensor nySensor1 = new Sensor();
		nySensor1.setSensorName("NY1");
		nySensor1.setSensorId(76301);
		nySensors.add(nySensor1);

		Sensor nySensor2 = new Sensor();
		nySensor2.setSensorName("NY2");
		nySensor2.setSensorId(76302);
		nySensors.add(nySensor2);

		Sensor nySensor3 = new Sensor();
		nySensor3.setSensorName("NY3");
		nySensor3.setSensorId(76303);
		nySensors.add(nySensor3);

		Sensor nySensor4 = new Sensor();
		nySensor4.setSensorName("NY4");
		nySensor4.setSensorId(76304);
		nySensors.add(nySensor4);

		// San Fransisco Sensors
		List<Sensor> sfSensors = new ArrayList<>();
		Sensor sfSensor1 = new Sensor();
		sfSensor1.setSensorName("sf1");
		sfSensor1.setSensorId(76305);
		sfSensors.add(sfSensor1);

		Sensor sfSensor2 = new Sensor();
		sfSensor2.setSensorName("sf2");
		sfSensor2.setSensorId(76306);
		sfSensors.add(sfSensor2);

		Sensor sfSensor3 = new Sensor();
		sfSensor3.setSensorName("sf3");
		sfSensor3.setSensorId(76307);
		sfSensors.add(sfSensor3);

		Sensor sfSensor4 = new Sensor();
		sfSensor4.setSensorName("sf4");
		sfSensor4.setSensorId(76308);
		sfSensors.add(sfSensor4);

		// Arizona City Sensor
		List<Sensor> azSensors = new ArrayList<>();
		Sensor azSensor1 = new Sensor();
		azSensor1.setSensorName("az1");
		azSensor1.setSensorId(76309);
		azSensors.add(azSensor1);

		Sensor azSensor2 = new Sensor();
		azSensor2.setSensorName("az2");
		azSensor2.setSensorId(763010);
		azSensors.add(azSensor2);

		Sensor azSensor3 = new Sensor();
		azSensor3.setSensorName("az3");
		azSensor3.setSensorId(763011);
		azSensors.add(azSensor3);

		Sensor azSensor4 = new Sensor();
		azSensor4.setSensorName("az4");
		azSensor4.setSensorId(763012);
		azSensors.add(azSensor4);

		sensorMap.put("New York", nySensors);
		sensorMap.put("San Fransisco", sfSensors);
		sensorMap.put("Arizona City", azSensors);

		return sensorMap;

	}
	
	private static void streamCheck(String STREAM_NAME){
		
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
						try {
							waitForStreamToBecomeAvailable(STREAM_NAME);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
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
					try {
						waitForStreamToBecomeAvailable(STREAM_NAME);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
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
