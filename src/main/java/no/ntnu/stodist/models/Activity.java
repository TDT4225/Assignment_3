package no.ntnu.stodist.models;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import lombok.Data;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
public class Activity {
    @BsonIgnore
    public static String collection = "activity";

    @BsonIgnore
    private static int idCounter = 0;

    private static synchronized int getNextId() {
        idCounter = idCounter + 1;
        return idCounter;
    }

    public Activity() {
        this.id = getNextId();
    }

    public Activity(int id) {
        this.id = id;
    }

    @BsonId
    private int id;
    @BsonProperty
    private List<TrackPoint> trackPoints = new ArrayList<>();
    @BsonProperty
    private String transportationMode;
    @BsonProperty
    private LocalDateTime startDateTime;
    @BsonProperty
    private LocalDateTime endDateTime;

    public Optional<String> getTransportationMode() {
        return Optional.ofNullable(transportationMode);
    }


    public Document toDocument(int parentId){
        return new Document("_id", this.getId())
                .append("user_id", parentId)
                .append("trackPoints", this.getTrackPoints().stream().map(TrackPoint::getId).toList())
                //.append("trackPoints", this.getTrackPoints().stream().map(trackPoint -> trackPoint.toDocument(this.getId())).toList())
                .append("transportationMode", this.getTransportationMode().orElse(null))
                .append("startDateTime", Date.from(getStartDateTime().toInstant(ZoneOffset.UTC)))
                .append("endDateTime", Date.from(getEndDateTime().toInstant(ZoneOffset.UTC)));
    }

    public Document toDnormDocument(int parentId){
        return new Document("_id", this.getId())
                .append("user_id", parentId)
                //.append("trackPoints", this.getTrackPoints().stream().map(TrackPoint::getId).toList())
                .append("trackPoints", this.getTrackPoints().stream().map(trackPoint -> trackPoint.toDocument(this.getId())).toList())
                .append("transportationMode", this.getTransportationMode().orElse(null))
                .append("startDateTime", Date.from(getStartDateTime().toInstant(ZoneOffset.UTC)))
                .append("endDateTime", Date.from(getEndDateTime().toInstant(ZoneOffset.UTC)));
    }
}
