package com.amazonaws.samples;

import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/*
 * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-sqs-message-queues.html
 */
public class SQSSample {

	private static AWSCredentials credentials;
	private static AmazonSQSClient client;

	public static void main(String[] args) throws IOException {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential profile
		 * by reading from the credentials file located at
		 * (C:\\Users\\Ray\\.aws\\credentials).
		 */

		try {
			// credentials = new ProfileCredentialsProvider("default").getCredentials();
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (C:\\Users\\Ray\\.aws\\credentials), and is in valid format.", e);
		}

		client = new AmazonSQSClient(credentials);
		client.setRegion(Region.getRegion(Regions.US_EAST_2));

		// It will be standard queue by default. It is ok if the queue already exist
		CreateQueueResult createQueueResult = client.createQueue(new CreateQueueRequest("DemoQueue"));
		String myQueueUrl = createQueueResult.getQueueUrl();
		System.out.println();

		ListQueuesResult listQueueResult = client.listQueues();
		System.out.println("Your SQS Queue URLs:");
		for (String url : listQueueResult.getQueueUrls()) {
			System.out.println(url);
		}

		SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl,
		 "My test message body");

		SendMessageRequest sendMessageRequest = new SendMessageRequest().withQueueUrl(myQueueUrl)
				.withMessageBody("My test message body").withDelaySeconds(5);
		client.sendMessage(sendMessageRequest);

		
		for(int i=0; i<=3; i++) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
					.withMaxNumberOfMessages(10)
					.withWaitTimeSeconds(10);

			/*
			 * Not get all messages every time and can keep getting the same messages.
			 * If every message are in flight, it will waiting for seconds.
			 */
			List<Message> messages = client.receiveMessage(receiveMessageRequest).getMessages();
			messages.forEach(m -> System.out.println(m.getBody()));
			
			if(messages.size() == 0) {
				break;
			}
			// Delete the message
			System.out.println("Deleting a message.");
			final String messageReceiptHandle = messages.get(0).getReceiptHandle();
			client.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
			
		}
		
		System.out.println("Deleting queue " + myQueueUrl);
		client.deleteQueue(myQueueUrl);
	
	}

}
