package com.aws.sqs;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSUtils {
  
  public AmazonSQS sqs;
  
  /**
   * This constructor instantiate sqs client and regions
   */
  public SQSUtils(){
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    sqs.setRegion(Region.getRegion(Regions.US_EAST_1));
  }
  
  /**
   * This method creates the queue with the given specifications and queuename
   * 
   * @param QUEUE_NAME
   */
  public void createQueue(String QUEUE_NAME) {
    
    CreateQueueRequest create_request =
        new CreateQueueRequest(QUEUE_NAME).addAttributesEntry("DelaySeconds", "60")
            .addAttributesEntry("MessageRetentionPeriod", "86400");
    // You can use the simplified form of createQueue, which needs only a queue name, to create a
    // standard queue.
    // sqs.createQueue("MyQueue" + new Date().getTime());
    try {
      sqs.createQueue(create_request);
    } catch (AmazonSQSException e) {
      if (!e.getErrorCode().equals("QueueAlreadyExists")) {
        throw e;
      }
    }
  }

  /**
   * This method gets you the queue URL for the given queue
   * 
   * @param QUEUE_NAME
   * @return
   */
  public String getQueueURL(String QUEUE_NAME) {
    return sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
  }

  /**
   * This method deletes the queue that is being passed to it.
   * 
   * @param queue_url
   */
  public void deleteQueue(String queue_url) {
    sqs.deleteQueue(queue_url);
  }

  /**
   * This method lists all the queues in the given region
   * 
   * @return
   */
  public List<String> listAllQueues() {
    List<String> allQueues = new ArrayList<String>();
    ListQueuesResult lq_result = sqs.listQueues();
    for (String url : lq_result.getQueueUrls()) {
      allQueues.add(url);
    }
    return allQueues;
  }

  /**
   * This method receives all the messages in the given queue
   * 
   * @param myQueueUrl
   */
  public void receiveMessagesfromQueue(String myQueueUrl) {
    try {
      System.out.println("Receiving messages from MyQueue.\n");
      ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
      List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
      for (Message message : messages) {
        System.out.println("  Message");
        System.out.println("    MessageId:     " + message.getMessageId());
        System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
        System.out.println("    Body:          " + message.getBody());
      }
      System.out.println();
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon SQS, but was rejected with an error response for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with SQS, such as not "
          + "being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }

  /**
   * This method sends messages to queue
   * 
   * @param myQueueUrl
   * @param myMessage
   */
  public void sendMessagetoQueue(String myQueueUrl, String myMessage) {
    try {
      sqs.sendMessage(new SendMessageRequest(myQueueUrl, myMessage));
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon SQS, but was rejected with an error response for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with SQS, such as not "
          + "being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }

  /**
   * This method purges the given queue
   * 
   * @param myQueueUrl
   */
  public void purgeQueue(String myQueueUrl) {
    try {
      AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
      sqs.purgeQueue(new PurgeQueueRequest(myQueueUrl));
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon SQS, but was rejected with an error response for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with SQS, such as not "
          + "being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }

  /**
   * This method deletes the given message from queue
   * 
   * @param myQueueUrl
   * @param message
   */
  private void deleteMessageFromQueue(String myQueueUrl, Message message) {
    try {
      DeleteMessageRequest dmr = new DeleteMessageRequest(myQueueUrl, message.getReceiptHandle());
      sqs.deleteMessage(dmr);
      return;
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon SQS, but was rejected with an error response for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with SQS, such as not "
          + "being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }
  
}
