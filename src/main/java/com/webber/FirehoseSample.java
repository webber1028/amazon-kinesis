package com.webber;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;

public class FirehoseSample {

	public static void main(String[] args) {
		AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
		
		AmazonKinesisFirehose firehoseClient = AmazonKinesisFirehoseClientBuilder
				  .standard()
				  .withCredentials(credentialsProvider)
				  .withRegion(Regions.US_WEST_2)
				  .build();
		
		String streamName = "firehose-datastream";
		
		// Put record 
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setDeliveryStreamName(streamName);
		putRecordRequest.setRecord(createData());
		firehoseClient.putRecord(putRecordRequest);
		
		// Put batch records
		PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
		putRecordBatchRequest.setDeliveryStreamName(streamName);
		
		List<Record> recordList = new ArrayList<Record>();
		for (int i = 0; i < 10 ; i++) {
			recordList.add(createData());
		}
		
		putRecordBatchRequest.setRecords(recordList);
		firehoseClient.putRecordBatch(putRecordBatchRequest);

		recordList.clear();
	}
	
	public static Record createData() {
		String data = ThreadLocalRandom.current().nextInt() + "\n";
		return new Record().withData(ByteBuffer.wrap(data.getBytes()));
	}

}
