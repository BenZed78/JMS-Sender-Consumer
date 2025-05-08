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

            // JMS-Verbindung und Session erstellen
            try (Connection connection = connectionFactory.createConnection();
                 Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 MessageConsumer consumer = session.createConsumer(queue)) {

                // Verbindung starten
                connection.start();
                System.out.println("Warte auf Nachrichten...");

                // Endlosschleife zum Konsumieren und Verwerfen von Nachrichten
                while (true) {
                    Message message = consumer.receive(5000); // Erhöhtes Timeout: 5 Sekunden
                    if (message != null) {
                        if (message instanceof BytesMessage) {
                            BytesMessage bytesMessage = (BytesMessage) message;
                            long sizeBytes = bytesMessage.getBodyLength();
                            try {
                                // Nachricht in Chunks lesen
                                readMessageInChunks(bytesMessage);
                                System.out.printf("Nachricht empfangen und verworfen: Größe = %.2f MB (%d Bytes)%n",
                                        sizeBytes / (double) Config.BYTES_PER_MB, sizeBytes);
                            } catch (JMSException e) {
                                System.err.printf("Fehler beim Verarbeiten der Nachricht (Größe: %d Bytes): %s%n",
                                        sizeBytes, e.getMessage());
                                throw e; // Fehler weiterleiten, um Schleife nicht zu blockieren
                            }
                        } else {
                            System.out.println("Nachricht empfangen und verworfen: " + message);
                        }
                        // Nachricht wird automatisch durch AUTO_ACKNOWLEDGE gelöscht
                    }
                }
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
}