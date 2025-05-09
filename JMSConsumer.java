import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class JMSConsumer {
    private static final AtomicLong totalMessages = new AtomicLong(0);
    private static final AtomicLong totalBytes = new AtomicLong(0);
    private static final long startTime = System.nanoTime();

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

                                        // Statistiken aktualisieren
                                        long currentMessages = totalMessages.incrementAndGet();
                                        long currentBytes = totalBytes.addAndGet(sizeBytes);

                                        // Statistiken periodisch ausgeben
                                        if (currentMessages % config.getStatsInterval() == 0) {
                                            logStatistics(currentMessages, currentBytes);
                                        }
                                    } else {
                                        System.out.println("Consumer-Thread " + threadId + ": Nachricht empfangen und verworfen: " + message);
                                        totalMessages.incrementAndGet();
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

                // Abschließende Statistiken ausgeben
                logStatistics(totalMessages.get(), totalBytes.get());
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

    // Methode zum Protokollieren der Statistiken
    private static void logStatistics(long messages, long bytes) {
        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double messagesPerSecond = elapsedMs > 0 ? messages * 1000.0 / elapsedMs : 0;
        double mbPerSecond = elapsedMs > 0 ? (bytes / (double) Config.BYTES_PER_MB) * 1000.0 / elapsedMs : 0;

        System.out.printf(
                "Statistik: %d Nachrichten, %.2f MB gesamt, %.2f Nachrichten/Sek, %.2f MB/Sek, Speicher: %.2f MB verwendet%n",
                messages, bytes / (double) Config.BYTES_PER_MB, messagesPerSecond, mbPerSecond,
                usedMemory / (double) Config.BYTES_PER_MB);
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

    public int getStatsInterval() {
        return Integer.parseInt(properties.getProperty("consumer.stats.interval", "100"));
    }
}