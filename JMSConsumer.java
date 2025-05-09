import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.FileInputStream;
import java.util.Properties;

public class JMSConsumer {
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

                // Anzahl der Consumer-Threads
                int threadCount = config.getConsumerThreadCount();
                Thread[] threads = new Thread[threadCount];

                // Threads starten
                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t + 1;
                    threads[t] = new Thread(() -> {
                        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                             MessageConsumer consumer = session.createConsumer(queue)) {
                            System.out.println("Consumer-Thread " + threadId + ": Warte auf Nachrichten...");
                            consumer.setMessageListener(message -> {
                                try {
                                    if (message instanceof BytesMessage) {
                                        BytesMessage bytesMessage = (BytesMessage) message;
                                        long sizeBytes = bytesMessage.getBodyLength();
                                        readMessageInChunks(bytesMessage);
                                        System.out.printf("Consumer-Thread %d: Nachricht empfangen und verworfen: Größe = %.2f MB (%d Bytes)%n",
                                                threadId, sizeBytes / (double) Config.BYTES_PER_MB, sizeBytes);
                                    } else {
                                        System.out.println("Consumer-Thread " + threadId + ": Nachricht empfangen und verworfen: " + message);
                                    }
                                    // Nachricht wird automatisch durch AUTO_ACKNOWLEDGE gelöscht
                                } catch (JMSException e) {
                                    System.err.printf("Consumer-Thread %d: Fehler beim Verarbeiten der Nachricht: %s%n",
                                            threadId, e.getMessage());
                                }
                            });

                            // Halte den Thread am Laufen, bis er unterbrochen wird
                            while (!Thread.interrupted()) {
                                try {
                                    Thread.sleep(100); // Kurze Pause, um CPU-Last zu reduzieren
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt(); // Interrupt-Status wiederherstellen
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Fehler in Consumer-Thread " + threadId + ": " + e.getMessage());
                            e.printStackTrace();
                        }
                    });
                    threads[t].start();
                }

                // Warte auf Benutzereingabe, um das Programm zu beenden
                System.out.println("Drücke Enter, um die Consumer-Threads zu beenden...");
                System.in.read();
                for (Thread thread : threads) {
                    thread.interrupt();
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                System.out.println("Alle Consumer-Threads beendet.");
            }
        } catch (Exception e) {
            System.err.println("Fehler beim Empfangen der Nachricht: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Methode zum Lesen der Nachricht in Chunks
    private static void readMessageInChunks(BytesMessage message) throws JMSException {
        byte[] buffer = new byte[512 * 1024]; // Reduzierter Puffer: 512 KB
        int bytesRead;
        while ((bytesRead = message.readBytes(buffer)) != -1) {
            // Daten verwerfen (wir lesen nur, um die Nachricht zu konsumieren)
        }
    }
}

// Hilfsklasse zum Laden der Konfiguration
class Config {
    public static final int BYTES_PER_MB = 1_048_576; // 1 MB = 1.048.576 Bytes
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

    public int getConsumerThreadCount() {
        return Integer.parseInt(properties.getProperty("consumer.thread.count", "1"));
    }
}