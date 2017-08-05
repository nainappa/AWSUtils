package com.aws.dynamoDB;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class DynamoDBUtils {
  AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
  DynamoDB dynamoDB = new DynamoDB(client);

  /**
   * This method is to create a table in dynamoDB with the given specifications
   * 
   * @param tableName
   * @param readCapacityUnits
   * @param writeCapacityUnits
   * @param partitionKeyName
   * @param partitionKeyType
   */
  private void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
      String partitionKeyName, String partitionKeyType) {
    createTable(tableName, readCapacityUnits, writeCapacityUnits, partitionKeyName,
        partitionKeyType, null, null);
  }

  private void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
      String partitionKeyName, String partitionKeyType, String sortKeyName, String sortKeyType) {
    try {
      ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
      keySchema.add(
          new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
      ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
      attributeDefinitions.add(new AttributeDefinition().withAttributeName(partitionKeyName)
          .withAttributeType(partitionKeyType));
      if (sortKeyName != null) {
        keySchema
            .add(new KeySchemaElement().withAttributeName(sortKeyName).withKeyType(KeyType.RANGE)); // Sort
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(sortKeyName)
            .withAttributeType(sortKeyType));
      }
      CreateTableRequest request = new CreateTableRequest().withTableName(tableName)
          .withKeySchema(keySchema).withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));
      request.setAttributeDefinitions(attributeDefinitions);
      System.out.println("Issuing CreateTable request for " + tableName);
      Table table = dynamoDB.createTable(request);
      System.out.println("Waiting for " + tableName + " to be created...this may take a while...");
      table.waitForActive();
    } catch (Exception e) {
      System.err.println("CreateTable request failed for " + tableName);
      System.err.println(e.getMessage());
    }
  }

  /**
   * This method deletes the table
   * 
   * @param tableName
   */
  public void deleteTable(String tableName) {
    Table table = dynamoDB.getTable(tableName);
    try {
      System.out.println("Issuing DeleteTable request for " + tableName);
      table.delete();
      System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");
      table.waitForDelete();
    } catch (Exception e) {
      System.err.println("DeleteTable request failed for " + tableName);
      System.err.println(e.getMessage());
    }
  }


  /**
   * This method is to get the entire table information the given tablename
   * 
   * @param strTable
   */
  public void getTableInformation(String strTable) {
    System.out.println("Describing " + strTable);
    TableDescription tableDescription = dynamoDB.getTable(strTable).describe();
    System.out.format(
        "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
            + "Provisioned Throughput (write capacity units/sec): %d \n",
        tableDescription.getTableName(), tableDescription.getTableStatus(),
        tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
        tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
  }

  /**
   * This method is to create a entry from table in dynamoDB
   * 
   * @param strTableName
   * @throws InterruptedException
   */
  public void createItem(String strTableName) throws InterruptedException {
    Table table = dynamoDB.getTable(strTableName);
    Item item = new Item().withPrimaryKey("id", "<id here>").withString("name", "<Name here>")
        .withString("degree", "<degree here>").withString("section", "<section here>")
        .withString("college", "<College here>");
    table.putItem(item);
  }

  /**
   * This method is to delete a entry from table in dynamoDB
   * 
   * @param strTableName
   */
  public void deleteItem(String strTableName) {
    Table table = dynamoDB.getTable(strTableName);
    table.deleteItem("<your primary key>", "<your primary key value>");
  }

  /**
   * This method is to retrieve the record specified from the table
   * 
   * @param strTableName
   */
  public void retrieveItem(String strTableName) {
    Table table = dynamoDB.getTable(strTableName);
    try {
      Item item = table.getItem("<PrimaryKey>", "<Item value that you want to retrieve>",
          "<projectionExpression>", null);
      System.out.println("Printing item after retrieving it....");
      System.out.println(item.toJSONPretty());
    } catch (Exception e) {
      System.err.println("GetItem failed.");
      System.err.println(e.getMessage());
    }
  }


}
