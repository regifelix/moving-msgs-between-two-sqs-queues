package br.com.regifelix.movingmsgsqsqueue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ReprocessDlqService {

    private static final Integer MAX_NUMBER_OF_MESSAGES = 10;
    private static final Integer DELAY_IN_SECONDS = 1;


    private static String sqsURL = "https://sqs.us-east-1.amazonaws.com/45454545454/your_queue_dlq";

    private static String sqsDestinoURL = "https://sqs.us-east-1.amazonaws.com/45454545454/your_destination_queue";

    private ReprocessDlqService() {
        throw new UnsupportedOperationException();
    }

    public static void executeReprocessDlq() {
        BasicAWSCredentials creds =
            new BasicAWSCredentials("awsAcessKeyXpto", "awsSecretKeyXpto");

        Region region = Region.getRegion(Regions.US_EAST_1);


        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
            .withRegion(region.getName()).build();

        log.info("Receiving messages from MyQueue.\n");
        final ReceiveMessageRequest receiveMessageRequest =
            new ReceiveMessageRequest(sqsURL)
                .withMaxNumberOfMessages(MAX_NUMBER_OF_MESSAGES)
                .withWaitTimeSeconds(3);

        boolean hasMore = true;

        List<Message> messagesToPublih = new ArrayList();

        while (hasMore) {
            final List<Message> messages =
                sqs.receiveMessage(receiveMessageRequest).getMessages();
            if (messages.size() > 0) {
                for (Message msg : messages) {
                    messagesToPublih.add(msg);
                    sqs.deleteMessage(new DeleteMessageRequest()
                        .withQueueUrl(sqsURL)
                        .withReceiptHandle(msg.getReceiptHandle()));
                }
            } else {
                hasMore = false;
            }


        }


        log.info("Total of messages in the DLQ queue " + messagesToPublih.size());
        for (Message msg : messagesToPublih) {

            String messageToRefund = msg.getBody();
            log.info("Enviando mensagem: [" + msg.getBody() + "]");

            SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(sqsDestinoURL)
                .withMessageBody(messageToRefund)
                .withDelaySeconds(DELAY_IN_SECONDS);
            sqs.sendMessage(sendMessageRequest);
            log.info("Message sent to refund_notify queue [{}]", messageToRefund);

        }

    }

    public static void main(final String[] args) {
        ReprocessDlqService.executeReprocessDlq();
    }


}
