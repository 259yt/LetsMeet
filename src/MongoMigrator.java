import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MongoMigrator {

    // Datenbank-Konstanten
    private static final String PG_URL = "jdbc:postgresql://localhost:5433/lf8_lets_meet_db";
    private static final String PG_USER = "user";
    private static final String PG_PASSWORD = "secret";

    // MongoDB-Konstanten
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String MONGO_DB = "LetsMeet";

    public static void main(String[] args) {
        System.out.println("Starte MongoDB-Datenmigration...");

        try (MongoClient mongoClient = MongoClients.create(MONGO_URI);
             Connection pgConnection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)) {
            pgConnection.setAutoCommit(false); // Transaktionen aktivieren
            MongoDatabase mongoDb = mongoClient.getDatabase(MONGO_DB);

            // Migrationsmethoden aufrufen
            migrateUsers(mongoDb, pgConnection);
            migrateFriendships(mongoDb, pgConnection);
            migrateInterests(mongoDb, pgConnection);
            migrateConversations(mongoDb, pgConnection);
            migrateMessages(mongoDb, pgConnection);

            pgConnection.commit(); // Transaktion abschließen
            System.out.println("MongoDB-Datenmigration erfolgreich abgeschlossen.");
        } catch (SQLException e) {
            System.err.println("Datenbankfehler während der MongoDB-Migration: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("MongoDB-Migration fehlgeschlagen: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void migrateUsers(MongoDatabase mongoDb, Connection pgConnection) throws Exception {
        System.out.println("Migriere MongoDB Benutzer...");
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String userSql = "INSERT INTO app_user (user_email, first_name, last_name, gender, date_of_birth) " +
                "VALUES (?, ?, ?, ?, ?) " +
                "ON CONFLICT (user_email) DO UPDATE SET " +
                "first_name = COALESCE(EXCLUDED.first_name, app_user.first_name), " +
                "last_name = COALESCE(EXCLUDED.last_name, app_user.last_name), " +
                "gender = COALESCE(EXCLUDED.gender, app_user.gender), " +
                "date_of_birth = COALESCE(EXCLUDED.date_of_birth, app_user.date_of_birth)";

        try (PreparedStatement userStmt = pgConnection.prepareStatement(userSql)) {
            for (Document userDoc : usersCollection.find()) {
                String email = userDoc.getString("_id");
                String fullName = userDoc.getString("name");
                String firstName = null;
                String lastName = null;

                if (fullName != null && fullName.contains(", ")) {
                    String[] nameParts = fullName.split(", ", 2);
                    lastName = nameParts[0].trim();
                    if (nameParts.length > 1) {
                        firstName = nameParts[1].trim();
                    }
                }

                userStmt.setString(1, email);
                userStmt.setString(2, firstName);
                userStmt.setString(3, lastName);
                userStmt.setNull(4, java.sql.Types.VARCHAR);
                userStmt.setNull(5, java.sql.Types.DATE);
                userStmt.executeUpdate();
            }
        }
        System.out.println("MongoDB Benutzerdaten erfolgreich migriert.");
    }

    private static void migrateFriendships(MongoDatabase mongoDb, Connection pgConnection) throws Exception {
        System.out.println("Migriere MongoDB Freundschaften...");
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String friendshipSql = "INSERT INTO friendship (user_id_1, user_id_2) VALUES (?, ?) ON CONFLICT (user_id_1, user_id_2) DO NOTHING";
        try (PreparedStatement friendshipStmt = pgConnection.prepareStatement(friendshipSql)) {
            for (Document userDoc : usersCollection.find()) {
                String userEmailFrom = userDoc.getString("_id");
                List<String> friends = userDoc.getList("friends", String.class);

                if (friends != null) {
                    for (String friendEmailTo : friends) {
                        friendshipStmt.setString(1, userEmailFrom);
                        friendshipStmt.setString(2, friendEmailTo);
                        friendshipStmt.executeUpdate();
                    }
                }
            }
        }
        System.out.println("MongoDB Freundschaften erfolgreich migriert.");
    }

    private static void migrateInterests(MongoDatabase mongoDb, Connection pgConnection) throws Exception {
        System.out.println("Migriere MongoDB Interessen...");
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String interestsSql = "INSERT INTO interest (from_user_id, to_user_id, date_of_creation) VALUES (?, ?, NOW()) ON CONFLICT (from_user_id, to_user_id) DO NOTHING";
        try (PreparedStatement interestsStmt = pgConnection.prepareStatement(interestsSql)) {
            for (Document userDoc : usersCollection.find()) {
                String userEmail = userDoc.getString("_id");

                // Versuche, die 'likes' als Liste von Dokumenten zu erhalten
                List<Document> likesAsDocuments = userDoc.getList("likes", Document.class);
                if (likesAsDocuments != null) {
                    for (Document likeDoc : likesAsDocuments) {
                        interestsStmt.setString(1, userEmail);
                        interestsStmt.setString(2, likeDoc.getString("liked_email"));
                        interestsStmt.executeUpdate();
                    }
                } else {
                    // Falls nicht als Dokumente, versuche es als Liste von Strings zu erhalten
                    List<String> likesAsStrings = userDoc.getList("likes", String.class);
                    if (likesAsStrings != null) {
                        for (String likedEmail : likesAsStrings) {
                            interestsStmt.setString(1, userEmail);
                            interestsStmt.setString(2, likedEmail);
                            interestsStmt.executeUpdate();
                        }
                    }
                }
            }
        }
        System.out.println("MongoDB Interessen erfolgreich migriert.");
    }

    private static void migrateConversations(MongoDatabase mongoDb, Connection pgConnection) throws Exception {
        System.out.println("Sammle und migriere MongoDB Konversationen...");
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");
        Set<Integer> conversationIds = new HashSet<>();

        for (Document userDoc : usersCollection.find()) {
            List<Document> messages = userDoc.getList("messages", Document.class);
            if (messages != null) {
                for (Document messageDoc : messages) {
                    Integer conversationId = messageDoc.getInteger("conversation_id");
                    if (conversationId != null) {
                        conversationIds.add(conversationId);
                    }
                }
            }
        }

        String conversationSql = "INSERT INTO conversation (conversation_id, date_of_creation) VALUES (?, NOW()) ON CONFLICT (conversation_id) DO NOTHING";
        try (PreparedStatement conversationStmt = pgConnection.prepareStatement(conversationSql)) {
            for (Integer convId : conversationIds) {
                conversationStmt.setInt(1, convId);
                conversationStmt.executeUpdate();
            }
        }
        System.out.println("MongoDB Konversationen erfolgreich migriert.");
    }

    private static void migrateMessages(MongoDatabase mongoDb, Connection pgConnection) throws Exception {
        System.out.println("Migriere MongoDB Nachrichten...");
        MongoCollection<Document> usersCollection = mongoDb.getCollection("users");

        String messageSql = "INSERT INTO message (conversation_id, sender_id, receiver_id, text, date_of_sending) VALUES (?, ?, ?, ?, ?)";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (PreparedStatement messageStmt = pgConnection.prepareStatement(messageSql)) {
            for (Document userDoc : usersCollection.find()) {
                String senderEmail = userDoc.getString("_id");
                List<Document> messages = userDoc.getList("messages", Document.class);

                if (messages != null) {
                    for (Document messageDoc : messages) {
                        Integer conversationId = messageDoc.getInteger("conversation_id");
                        String receiverEmail = messageDoc.getString("receiver_email");
                        String text = messageDoc.getString("message");
                        String timestampString = messageDoc.getString("timestamp");

                        LocalDateTime messageDate = null;
                        if (timestampString != null) {
                            try {
                                messageDate = LocalDateTime.parse(timestampString, formatter);
                            } catch (Exception e) {
                                System.err.println("Fehler beim Parsen des Datums: " + timestampString);
                                continue;
                            }
                        }

                        messageStmt.setInt(1, conversationId);
                        messageStmt.setString(2, senderEmail);
                        messageStmt.setString(3, receiverEmail);
                        messageStmt.setString(4, text);
                        messageStmt.setTimestamp(5, messageDate != null ? Timestamp.valueOf(messageDate) : null);
                        messageStmt.executeUpdate();
                    }
                }
            }
        }
        System.out.println("MongoDB Nachrichten erfolgreich migriert.");
    }
}
