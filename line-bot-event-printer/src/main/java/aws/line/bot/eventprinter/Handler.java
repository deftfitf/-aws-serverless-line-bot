package aws.line.bot.eventprinter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;

public class Handler implements RequestHandler<SQSEvent, String> {

    @Override
    public String handleRequest(SQSEvent sqsEvent, Context context) {
        final var logger = context.getLogger();
        for (final var record : sqsEvent.getRecords()) {
            logger.log(record.getBody());
        }
        return "OK";
    }

}
