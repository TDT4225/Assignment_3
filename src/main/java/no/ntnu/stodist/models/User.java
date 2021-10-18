package no.ntnu.stodist.models;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import lombok.Data;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;
import java.util.stream.Collectors;

@Data
public class User {
    @BsonIgnore
    public static String collection = "user";
    @BsonIgnore
    private static int idCounter = 0;

    private static synchronized int getNextId() {
        idCounter = idCounter + 1;
        return idCounter;
    }

    public User() {
        this.id = getNextId();
    }

    @BsonId
    private int id;
    @BsonProperty
    private boolean hasLabels;
    @BsonProperty
    private List<Activity> activities;

    public Document toDocument(){
        return new Document("_id", this.getId())
                .append("hasLabels", this.hasLabels)
                .append("activities", this.getActivities().stream().map(Activity::getId).collect(Collectors.toList()));
                //.append("activities", this.getActivities().stream().map(activity -> activity.toDocument(this.getId())).toList());

    }
}
