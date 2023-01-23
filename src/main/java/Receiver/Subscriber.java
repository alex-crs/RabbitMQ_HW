package Receiver;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;


public class Subscriber {
    private static final String EXCHANGE_NAME = "direct_message";
    static Logger logger = LoggerFactory.getLogger(Subscriber.class);

    public static void main(String[] args) throws IOException, TimeoutException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();
        logger.info("QUEUE NAME: " + queueName);
        callBackListener(queueName, channel);
        while (true) {
            try {
                System.out.println("-Введите тему на которую хотите подписаться в формате" +
                        " \"set_topic ТЕМА\".\n-Что бы отписаться" +
                        " используйте \"delete_topic ТЕМА\"\n-Для выхода введите exit");
                String[] message = reader.readLine().split(" ", 2);

                if (message[0].equals("set_topic") && message.length > 1 && message[1].length() > 0) {
                    channel.queueBind(queueName, EXCHANGE_NAME, message[1]);
                    logger.info("Вы подписались на тему: " + message[1]);
                }

                if (message[0].equals("delete_topic") && message.length > 1 && message[1].length() > 0) {
                    channel.queueUnbind(queueName, EXCHANGE_NAME, message[1]);
                    logger.info("Вы отписались от темы: " + message[1]);
                }

                if (message[0].equals("exit")) {
                    connection.close();
                    reader.close();
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void callBackListener(String queueName, Channel channel) throws IOException {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            logger.info("[x] Received " + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

}
