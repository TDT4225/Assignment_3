package no.ntnu.stodist.models;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import lombok.Data;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

@Data
public class TrackPoint {
    public static String collection = "trackPoint";

    private static int idCounter = 0;

    private static synchronized int getNextId() {
        idCounter = idCounter + 1;
        return idCounter;
    }

    public TrackPoint() {
        this.id = getNextId();
    }

    @BsonId
    private int id;

    @BsonProperty
    private double latitude;

    @BsonProperty
    private double longitude;

    @BsonProperty
    private Integer altitude;

    @BsonProperty
    private double dateDays;

    @BsonProperty
    private LocalDateTime dateTime;


    public Optional<Integer> getAltitude() {
        return Optional.ofNullable(altitude);
    }

    public Document toDocument(int parentId){
        return new Document("_id", this.getId())
                .append("activity_id", parentId)
                .append("latitude", this.getLatitude())
                .append("longitude", this.getLongitude())
                .append("altitude", this.getAltitude().orElse(null))
                .append("dateDays", this.getDateDays())
                .append("dateTime", Date.from(this.getDateTime().toInstant(ZoneOffset.UTC)));
    }
}
