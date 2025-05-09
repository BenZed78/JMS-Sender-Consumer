import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

public class JMSSender {
    private static final Random RANDOM = new Random();
    private static final int BYTES_PER_MB = 1_048_576; // 1 MB = 1.048.576 Bytes

    public static void main(String[] args) {
        try {
            // Konfiguration laden
            Config config = new Config("config.properties");

            // JNDI-Einstellungen für WebLogic
            Properties props = new Properties();
            props.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
            props.put(Context.PROVIDER_URL, config.getProviderUrl());
            props.put(Context.SECURITY_PRINCIPAL, config.getSecurityPrincipal());
            props.put(Context.SECURITY_CREDENTIALS, config.getSecurityCredentials());

            // JNDI-Kontext initialisieren
            InitialContext ctx = new InitialContext(props);

            // JMS-Ressourcen lookup
            ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup(config.getJmsConnectionFactory());
            Queue queue = (Queue) ctx.lookup(config.getJmsQueue());

            // JMS-Verbindung erstellen
            try (Connection connection = connectionFactory.createConnection()) {
                // Verbindung starten
                connection.start();

                // Anzahl der Threads und Nachrichten pro Thread
                int threadCount = config.getThreadCount();
                int messageCount = config.getMessageCount();
                int messagesPerThread = messageCount / threadCount;
                int remainingMessages = messageCount % threadCount; // Rest für den ersten Thread

                // Threads starten
                Thread[] threads = new Thread[threadCount];
                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t + 1;
                    final int threadMessages = (t == 0) ? messagesPerThread + remainingMessages : messagesPerThread;
                    threads[t] = new Thread(() -> {
                        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                             MessageProducer producer = session.createProducer(queue)) {
                            int minSizeMB = config.getMinSizeMB();
                            int maxSizeMB = config.getMaxSizeMB();
                            long pauseMs = config.getPauseMs();

                            for (int i = 1; i <= threadMessages; i++) {
                                // Zufällige Nachrichtengröße generieren
                                int sizeMB = minSizeMB + RANDOM.nextInt(maxSizeMB - minSizeMB + 1);
                                int sizeBytes = sizeMB * BYTES_PER_MB;

                                // BytesMessage erstellen
                                BytesMessage message = session.createBytesMessage();
                                byte[] data = new byte[sizeBytes];
                                RANDOM.nextBytes(data);
                                message.writeBytes(data);

                                // Nachricht senden
                                producer.send(message);
                                System.out.printf("Thread %d: Nachricht %d gesendet: Größe = %d MB (%d Bytes)%n",
                                        threadId, i, sizeMB, sizeBytes);

                                // Pause zwischen Nachrichten
                                Thread.sleep(pauseMs);
                            }
                        } catch (Exception e) {
                            System.err.println("Fehler in Thread " + threadId + ": " + e.getMessage());
                            e.printStackTrace();
                        }
                    });
                    threads[t].start();
                }

                // Warte auf das Beenden aller Threads
                for (Thread thread : threads) {
                    thread.join();
                }

                System.out.println("Alle Nachrichten gesendet.");
            }
        } catch (Exception e) {
            System.err.println("Fehler beim Senden der Nachricht: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

// Hilfsklasse zum Laden der Konfiguration
class Config {
    private final Properties properties;

    public Config(String filePath) throws Exception {
        properties = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            properties.load(fis);
        }
    }

    public String getProviderUrl() {
        return properties.getProperty("weblogic.provider.url");
    }

    public String getSecurityPrincipal() {
        return properties.getProperty("weblogic.security.principal");
    }

    public String getSecurityCredentials() {
        return properties.getProperty("weblogic.security.credentials");
    }

    public String getJmsConnectionFactory() {
        return properties.getProperty("jms.connection.factory");
    }

    public String getJmsQueue() {
        return properties.getProperty("jms.queue");
    }

    public int getMessageCount() {
        return Integer.parseInt(properties.getProperty("sender.message.count", "10"));
    }

    public int getMinSizeMB() {
        return Integer.parseInt(properties.getProperty("sender.min.size.mb", "1"));
    }

    public int getMaxSizeMB() {
        return Integer.parseInt(properties.getProperty("sender.max.size.mb", "250"));
    }

    public long getPauseMs() {
        return Long.parseLong(properties.getProperty("sender.pause.ms", "100"));
    }

    public int getThreadCount() {
        return Integer.parseInt(properties.getProperty("sender.thread.count", "1"));
    }
}