## AmazonWebServices Utilities
This project contains the most frequently used or essential methods in order to work with 3 important AWS components namingly SQS, SNS, DynamoDB. The utility methods are written by keeping QE in mind. However these can be used by anybody working with these AWS components. 

Here the assumption is that, user will have his AWS credentials file being stored in ~/home/.aws/credentials(in mac) or /user/<username>/.aws/credentials (in windows). In case if you don't have credentials in that folder, use the below method to pass them.

```java
snsClient = new AmazonSNSClient(new BasicAWSCredentials("<Your AccessKey>","<Your SecretKey>"));
```

### Support
For support and more details visit https://aws.amazon.com/free/?sc_channel=PS&sc_campaign=acquisition_US&sc_publisher=google&sc_medium=cloud_computing_hv_b&sc_content=aws_core_e_control_q32016&sc_detail=aws&sc_category=cloud_computing&sc_segment=188908133959&sc_matchtype=e&sc_country=US&s_kwcid=AL!4422!3!188908133959!e!!g!!aws&ef_id=WRPX3QAAAIfC_lzd:20170805224831:s.

