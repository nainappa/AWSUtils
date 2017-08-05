## AmazonWebServices Utilities
This project contains the most frequently used or essential methods in order to work with 3 important AWS components namingly SQS, SNS, DynamoDB. The utility methods are written by keeping QE in mind. However these can be used by anybody working with these AWS components. 

Here the assumption is that, user will have his AWS credentials file being stored in ~/home/.aws/credentials(in mac) or /user/<username>/.aws/credentials (in windows). In case if you don't have credentials in that folder, use the below method to pass them.

```java
snsClient = new AmazonSNSClient(new BasicAWSCredentials("<Your AccessKey>","<Your SecretKey>"));
```

### Support
For support please visit the Autobahn-Support HipChat room.

