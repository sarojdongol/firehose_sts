import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

public class FirehoseStsCrossAccount {

    public static class FirehoseStsAccess {

        private static final String ROLE_ARN = "arn:aws:iam::987654321099:role/PikselAssumeRole";

        public static BasicSessionCredentials usingStsCrossAccount() {

            // Step 1. AWS Security Token Service (STS) AssumeRole API, specifying
            // the ARN for the role created by the customer.

            AWSSecurityTokenServiceClientBuilder stsBuilder = AWSSecurityTokenServiceClientBuilder.standard();

            AWSSecurityTokenService stsClient = stsBuilder.build();

            AssumeRoleRequest assumeRequest = new AssumeRoleRequest()
                    .withRoleArn(ROLE_ARN)
                    .withDurationSeconds(3600)
                    .withRoleSessionName("demo");

            AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRequest);


            // Step 2. AssumeRole returns temporary security credentials for the IAM role.

            BasicSessionCredentials temporaryCredentials =
                    new BasicSessionCredentials(
                            assumeResult.getCredentials().getAccessKeyId(),
                            assumeResult.getCredentials().getSecretAccessKey(),
                            assumeResult.getCredentials().getSessionToken());
            return temporaryCredentials;
        }
    }
}
