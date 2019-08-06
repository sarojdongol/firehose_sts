import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


public class SerializeNew {
    public static void main(String args[]) throws IOException {

        AWSSecurityTokenServiceClientBuilder stsBuilder = AWSSecurityTokenServiceClientBuilder.standard();

        AWSSecurityTokenService stsClient = stsBuilder.build();

        AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
                .withRoleArn("arn:aws:iam::686402826740:role/FirehoseCrossRole")
                .withDurationSeconds(3600)
                .withRoleSessionName("demo");

        AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRequest);


        // Step 2. AssumeRole returns temporary security credentials for the IAM role.

        BasicSessionCredentials temporaryCredentials =
                new BasicSessionCredentials(
                        assumeResult.getCredentials().getAccessKeyId(),
                        assumeResult.getCredentials().getSecretAccessKey(),
                        assumeResult.getCredentials().getSessionToken());
        // Instantiating the Schema.Parser class.
        Schema schema = new Schema.Parser().parse(new File(
                "src/main/resources/emp.avsc"));
        // Instantiating the GenericRecord class.
        GenericRecord e1 = new GenericData.Record(schema);
        // Insert data according to schema
        e1.put("name", "ramu");
        e1.put("id", 001);
        e1.put("salary", 30000);
        e1.put("age", 25);
        e1.put("address", "chenni");

        GenericRecord e2 = new GenericData.Record(schema);
        e2.put("name", "rahman");
        e2.put("id", 002);
        e2.put("salary", 35000);
        e2.put("age", 30);
        e2.put("address", "Delhi");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
                schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
                datumWriter);
        dataFileWriter.create(schema, new File(
                "/home/local/ANT/sddongol/Desktop/mydata.txt"));
        dataFileWriter.append(e1);
        dataFileWriter.append(e2);

        String  deliveryStreamName = "deliverystreamsd";

        AmazonKinesisFirehose clientBuilder =  AmazonKinesisFirehoseClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(temporaryCredentials))
                .withRegion(Regions.US_EAST_1)
                .build();



        Record record = new Record().withData(ByteBuffer.wrap(data.getBytes()));

        PutRecordRequest request = new PutRecordRequest()
                .withDeliveryStreamName(deliveryStreamName)
                .withRecord(record);

        PutRecordResult putRecordResult = clientBuilder.putRecord(request);
        System.out.println("Put Result" + putRecordResult);


        dataFileWriter.close();
        System.out.println("data successfully serialized");
    }
}