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
            props.put(Context.PROVIDER_URL, config.getProviderUrl()); // Korrigiert: Bindestrich zu Punkt
            props.put(Context.SECURITY_PRINCIPAL, config.getSecurityPrincipal());
            props.put(Context.SECURITY_CREDENTIALS, config.getSecurityCredentials());

            // JNDI-Kontext initialisieren
            InitialContext ctx = new InitialContext(props);

            // JMS-Ressourcen lookup
            ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup(config.getJmsConnectionFactory());
            Queue queue = (Queue) ctx.lookup(config.getJmsQueue());

            // JMS-Verbindung und Session erstellen
            try (Connection connection = connectionFactory.createConnection();
                 Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

                // Nachrichtensender erstellen
                MessageProducer producer = session.createProducer(queue);

                // Nachrichten mit variabler Größe senden
                int messageCount = config.getMessageCount();
                int minSizeMB = config.getMinSizeMB();
                int maxSizeMB = config.getMaxSizeMB();
                long pauseMs = config.getPauseMs();

                for (int i = 1; i <= messageCount; i++) {
                    // Zufällige Nachrichtengröße zwischen minSizeMB und maxSizeMB generieren
                    int sizeMB = minSizeMB + RANDOM.nextInt(maxSizeMB - minSizeMB + 1);
                    int sizeBytes = sizeMB * BYTES_PER_MB;

                    // BytesMessage erstellen
                    BytesMessage message = session.createBytesMessage();
                    byte[] data = new byte[sizeBytes];
                    RANDOM.nextBytes(data); // Fülle das Array mit Zufallsdaten
                    message.writeBytes(data);

                    // Nachricht senden
                    producer.send(message);
                    System.out.printf("Nachricht %d gesendet: Größe = %d MB (%d Bytes)%n", i, sizeMB, sizeBytes);

                    // Pause zwischen Nachrichten
                    Thread.sleep(pauseMs);
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
}