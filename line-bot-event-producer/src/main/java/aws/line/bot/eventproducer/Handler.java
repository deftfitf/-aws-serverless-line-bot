package aws.line.bot.eventproducer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.bot.model.event.Event;
import com.linecorp.bot.parser.LineSignatureValidator;
import com.linecorp.bot.parser.WebhookParseException;
import com.linecorp.bot.parser.WebhookParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Collectors;

public class Handler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String QUEUE_URL = System.getenv("WEBHOOK_EVENT_QUEUE_SQS_URL");

    private final AmazonSQS sqs;
    private final WebhookParser parser;
    private final ObjectMapper objectMapper;

    public Handler() {
        final byte[] CHANNEL_SECRET =
                System.getenv("CHANNEL_SECRET")
                        .getBytes(StandardCharsets.US_ASCII);
        final var lineSignatureValidator = new LineSignatureValidator(CHANNEL_SECRET);
        parser = new WebhookParser(lineSignatureValidator);
        sqs = AmazonSQSClientBuilder.defaultClient();
        objectMapper = new ObjectMapper();
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        final var logger = context.getLogger();
        try {
            final var lineBotSignature =
                    Optional.ofNullable(input.getHeaders().get(WebhookParser.SIGNATURE_HEADER_NAME))
                            .orElseThrow(() -> new IllegalArgumentException("Can't find " + WebhookParser.SIGNATURE_HEADER_NAME + " header."));
            final var parsed = parser.handle(lineBotSignature, input.getBody().getBytes(StandardCharsets.UTF_8));

            final var requests = parsed.getEvents()
                    .stream()
                    .map(this::eventToSendMessageRequest)
                    .collect(Collectors.toList());
            final var batchRequest = new SendMessageBatchRequest()
                    .withQueueUrl(QUEUE_URL)
                    .withEntries(requests);
            sqs.sendMessageBatch(batchRequest);
            logger.log("send event successfully: " + input.getBody());

            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(200)
                    .withBody("{\"send\":\"success\"}");
        } catch (IllegalArgumentException | WebhookParseException e) {
            logger.log("bad request: " + e.getMessage());
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(400)
                    .withBody("{\"send\":\"failure\"}");
        } catch (IOException e) {
            logger.log("internal server error" + e.getMessage());
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("{\"send\":\"failure\"}");
        }
    }

    private SendMessageBatchRequestEntry eventToSendMessageRequest(Event event) {
        try {
            final var eventId = generateEventId(event);
            return new SendMessageBatchRequestEntry()
                    .withId(eventId)
                    .withMessageDeduplicationId(eventId)
                    .withMessageGroupId(event.getSource().getUserId())
                    .withMessageBody(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            throw new InternalError("Can't serialize event object: " + event.toString(), e);
        }
    }

    private String generateEventId(Event event) {
        return event.getSource().getUserId() + event.getTimestamp().toEpochMilli();
    }

}