package no.ntnu.stodist.database;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import no.ntnu.stodist.debugLogger.DebugLogger;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoConnection {

    private DB connection;
    private static final String url = System.getenv("MONGO_URL");
    private static final String port = System.getenv("MONGO_PORT");
    private static final String dbUser = System.getenv("USER_USERNAME");
    private static final String dbPassword = System.getenv("USER_PASSWORD");
    //    private final String url;
    //    private final String dbUser;
    //    private final String dbPassword;

    protected static final DebugLogger dbl = new DebugLogger(false);



    public MongoDatabase getMongoConnection() throws SQLException, UnknownHostException {
        dbl.log("try connect to db", "url", url, "user", dbUser, "passwd", dbPassword);
        Connection connection = null;

        MongoCredential credential = MongoCredential.createScramSha256Credential(dbUser, "admin",
                                                      dbPassword.toCharArray());

        MongoClientOptions options = new MongoClientOptions.Builder()
                .maxConnectionIdleTime(0)
                .connectTimeout(1000000000)
                .build();


        ServerAddress serverAddress     = new ServerAddress(url, Integer.parseInt(port));
        MongoClient mongoClient = new MongoClient(serverAddress, credential, options);
        return mongoClient.getDatabase("assignment2");
    }


}
