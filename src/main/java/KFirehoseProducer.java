
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.internal.securitytoken.STSProfileCredentialsServiceProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;


import java.nio.ByteBuffer;

public class KFirehoseProducer {
    public static void main(String[] args) {

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


        String data = "line" + "\n";

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

    }

    }
