
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;


import java.nio.ByteBuffer;

public class KFirehoseProducer {
    public static void main(String[] args) {
        String data = "line" + "\n";


        final static String  deliveryStreamName = "sarojdongol";

        AmazonKinesisFirehoseClientBuilder clientBuilder =  AmazonKinesisFirehoseClientBuilder.standard();

        AmazonKinesisFirehose firehoseclient = clientBuilder.build();



        Record record = new Record().withData(ByteBuffer.wrap(data.getBytes()));

        PutRecordRequest request = new PutRecordRequest()
                .withDeliveryStreamName(deliveryStreamName)
                .withRecord(record);

        PutRecordResult putRecordResult = firehoseclient.putRecord(request);
        System.out.println("Put Result" + putRecordResult);

    }

    }
