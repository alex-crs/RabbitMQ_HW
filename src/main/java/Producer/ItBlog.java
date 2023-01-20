package Producer;

import Receiver.Subscriber;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ItBlog {
    private static final String EXCHANGE_NAME = "direct_message";
    static Logger logger = LoggerFactory.getLogger(ItBlog.class);

    public static void main(String[] args) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                System.out.println("Введите сообщение формате \"ТЕМА СООБЩЕНИЕ\":");
                String[] message = reader.readLine().split(" ", 2);
                if (message[0].equals("exit")) {
                    break;
                }
                sendNews(message[0], message[1]);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendNews(String routingKey, String message) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
            logger.info("[!!!] отправляется сообщение " + routingKey + " : " + message + ".");

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}
