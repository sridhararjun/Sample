package CiteChk.DynamoDB;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.ion.Timestamp;

import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class SaveArchiveSessionHandler 
implements RequestHandler<ArchivingRequest, List<AttributeValue>> {
   
  private DynamoDB dynamoDb;
  private String DYNAMODB_TABLE_NAME = "Sessions";
  private Regions REGION = Regions.US_EAST_1;
  AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

  public List<AttributeValue> handleRequest(
    ArchivingRequest personRequest, Context context) {
	  LambdaLogger logger = context.getLogger();

      this.initDynamoDbClient();
      logger.log("Request Received");
      logger.log("Printing Request"+personRequest);
//      QuerySpec spec = new QuerySpec()
//    		    .withKeyConditionExpression("sessionId = :v_id")
//    		    .withValueMap(new ValueMap()
//    		        .withString(":v_id", "Amazon DynamoDB#DynamoDB Thread 1"));
      List<AttributeValue> result = readData();
      
      persistData(personRequest);
      logger.log("Request completed and persisted");
      ArchivingResponse personResponse = new ArchivingResponse();
      personResponse.setMessage("Saved Successfully!!!");
      return result;
  }

     private List<AttributeValue> readData() {
    	 ScanRequest scanRequest = new ScanRequest()
     		    .withTableName(DYNAMODB_TABLE_NAME);
       ScanResult result = client.scan(scanRequest);
       List<AttributeValue> list = null;
//       String createdDate;
       String ExpirationDate;
       LocalDateTime expireTime = null;
       LocalDateTime today = LocalDateTime.now();
//       DateTimeFormatter todayTime = DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss")
       DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss");
       LocalDateTime current = LocalDateTime.parse(today, dateTimeFormatter);
       AttributeValue results = null;
       for (Map<String, AttributeValue> item : result.getItems()){
//    	    createdDate=item.get("createdDate").toString();
    	    ExpirationDate=item.get("expirationDate").toString();
//    	    createTime.parse(createdDate, dateTimeFormatter);
    	    expireTime = LocalDateTime.parse(ExpirationDate, dateTimeFormatter);
    	    if(expireTime.isEqual(current) || expireTime.isBefore(current)) {
    	    	list.add(item.get("sessionId"));
    	    }
    	}
    	 
    	 return list;
     }

//  private GetItemOutcome recoverData(ArchivingRequest sessionRequest) throws ConditionalCheckFailedException {
//	  	return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
//	  			.getItem(
//	  					new GetItemSpec().with)
//  }
//  private <Map>
  
  
  private PutItemOutcome persistData(ArchivingRequest sessionRequest) 
    throws ConditionalCheckFailedException {
	  System.out.println("Inside Persist Data");
      return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
        .putItem(
          new PutItemSpec().withItem(new Item()
            .withString("sessionId", sessionRequest.getSessionId())
            .withString("createdDate", sessionRequest.getCreatedDate())
            .withString("expirationDate", sessionRequest.getExpirationDate())));
  }

  private void initDynamoDbClient() {
System.out.println("DynamoDB initialised");
      AmazonDynamoDBClient client = new AmazonDynamoDBClient();
      client.setRegion(Region.getRegion(REGION));
      this.dynamoDb = new DynamoDB(client);
  }
}
