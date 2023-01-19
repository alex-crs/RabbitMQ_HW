package Receiver;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;


public class Subscriber {
    private static final String EXCHANGE_NAME = "direct_message";
    static HashMap<String, Listener> subscribes = new HashMap<>();
    static Logger logger = LoggerFactory.getLogger(Subscriber.class);

    public static void main(String[] args) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                System.out.println("-Введите тему на которую хотите подписаться в формате \"set_topic ТЕМА\".\n-Что бы отписаться используйте \"delete_topic ТЕМА\"\n-Для выхода введите exit");
                String[] message = reader.readLine().split(" ", 2);

                if (message[0].equals("set_topic") && message.length > 1 && message[1].length() > 0) {
                    Listener listener = new Listener(message[1]);
                    listener.run();
                    subscribes.put(message[1], listener);
                    logger.info("Вы подписались на тему: " + message[1]);
                }

                if (message[0].equals("delete_topic") && message.length > 1 && message[1].length() > 0) {
                    Listener listener = subscribes.get(message[1]);
                    listener.close();
                    subscribes.remove(message[1]);
                    logger.info("Вы отписались от темы: " + message[1]);
                }

                if (message[0].equals("exit")) {
                    subscribes.forEach((s, listener) -> listener.close());
                    reader.close();
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class Listener implements Runnable {
        private Connection connection;
        private final String routingKey;

        private void close() {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Listener(String routingKey) {
            this.routingKey = routingKey;
        }

        @Override
        public void run() {
            try {
                receiveMessage(routingKey);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }

        private void receiveMessage(String routingKey) throws IOException, TimeoutException {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            String queueName = channel.queueDeclare().getQueue();
            logger.info("QUEUE NAME: " + queueName);

            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
            logger.info("[x] Waiting for messages with routing key (" + routingKey + "):");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                logger.info("[x] Received " + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        }
    }
}
