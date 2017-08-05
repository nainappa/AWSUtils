package com.aws.sns;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class SNSUtils {
  public AmazonSNSClient snsClient;
  public AmazonSQS sqsClient;

  /**
   * This constructor instantiate SQS and SNS Client which are used in the subsequent methods
   */
  public SNSUtils() {
    snsClient = new AmazonSNSClient();
    //Use the below method in case if you dont have credentials file stored in home or user folder.
    //snsClient = new AmazonSNSClient(new BasicAWSCredentials("<Your AccessKey>","<Your SecretKey>"));
    snsClient.setRegion(Region.getRegion(Regions.US_EAST_1));
    sqsClient = new AmazonSQSClient();
    sqsClient.setRegion(Region.getRegion(Regions.US_EAST_1));
  }

  /**
   * This method creates the SNS Topic for your usage
   * @param strTopicName
   * @return TopicARN
   */
  public String createSNSTopic(String strTopicName) {
    // create a new SNS topic
    CreateTopicRequest createTopicRequest = new CreateTopicRequest(strTopicName);
    CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
    // print TopicArn
    System.out.println(createTopicResult);
    // get request id for CreateTopicRequest from SNS metadata
    System.out
        .println("CreateTopicRequest - " + snsClient.getCachedResponseMetadata(createTopicRequest));
    return createTopicResult.getTopicArn();
  }

  /**
   * This method deletes the SNS topic
   * @param topicArn
   */
  public void deleteTopic(String topicArn) {
    // delete an SNS topic
    DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest(topicArn);
    snsClient.deleteTopic(deleteTopicRequest);
    // get request id for DeleteTopicRequest from SNS metadata
    System.out
        .println("DeleteTopicRequest - " + snsClient.getCachedResponseMetadata(deleteTopicRequest));
  }

  /**
   * This method is used to subscribe the topic from queue
   * @param myTopicArn
   * @param myQueueUrl
   */
  public void subscribeToTopic(String myTopicArn, String myQueueUrl) {
    Topics.subscribeQueue(snsClient, sqsClient, myTopicArn, myQueueUrl);
  }
  
  public void listAllTopics(){
    System.out.println(snsClient.listTopics());
  }

  /**
   * This method is used for publishing the message to topic
   * @param topicArn
   * @param strMessage
   */
  public void publishToTopic(String topicArn, String strMessage) {
    // publish to an SNS topic
    PublishRequest publishRequest = new PublishRequest(topicArn, strMessage);
    PublishResult publishResult = snsClient.publish(publishRequest);
    // print MessageId of message published to SNS topic
    System.out.println("MessageId - " + publishResult.getMessageId());
  }
}
