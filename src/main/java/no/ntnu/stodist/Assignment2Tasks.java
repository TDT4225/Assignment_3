package no.ntnu.stodist;

import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCallback;
import com.mongodb.DBCollection;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Updates;
import lombok.Data;
import no.ntnu.stodist.models.Activity;
import no.ntnu.stodist.models.TrackPoint;
import no.ntnu.stodist.models.User;
import no.ntnu.stodist.simpleTable.Column;
import no.ntnu.stodist.simpleTable.SimpleTable;
import org.bson.conversions.Bson;

import java.util.*;

import org.bson.Document;
import org.bson.BsonNull;

import javax.print.Doc;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.chrono.ChronoPeriod;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.OptionalInt.empty;

public class Assignment2Tasks {



    /**
     * Calculates the haversine distance between two coordinates in km
     *
     * @param lat1 lat of the first cord
     * @param lat2 lat of the second cord
     * @param lon1 lon of the first cord
     * @param lon2 lon of the second cord
     *
     * @return Returns the distance in Km between the points
     */
    public static double haversineDistance(double lat1, double lat2, double lon1, double lon2) {

        lon1 = Math.toRadians(lon1);
        lon2 = Math.toRadians(lon2);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double dlon = lon2 - lon1;
        double dlat = lat2 - lat1;
        double a = Math.pow(Math.sin(dlat / 2), 2)
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.pow(Math.sin(dlon / 2), 2);
        double c = 2 * Math.asin(Math.sqrt(a));

        double r = 6371;
        return c * r;
    }


    /**
     * Get the overlap between two LocalDateTime ranges
     *
     * @param t1_start start of first datetime
     * @param t1_end   end of first datetime
     * @param t2_start start of second datetim
     * @param t2_end   end of second datetime
     *
     * @return Array with start and end LocalDateTime
     */
    private static LocalDateTime[] getTimeOverlap(LocalDateTime t1_start,
                                                  LocalDateTime t1_end,
                                                  LocalDateTime t2_start,
                                                  LocalDateTime t2_end
    ) {
        LocalDateTime start_overlap = t1_start.compareTo(t2_start) <= 0 ? t2_start : t1_start;
        LocalDateTime end_overlap   = t1_end.compareTo(t2_end) <= 0 ? t1_end : t2_end;
        return new LocalDateTime[]{start_overlap, end_overlap};
    }

    /**
     * Return whether two LocalDatetime ranges overlap
     *
     * @param t1_start start of first datetime
     * @param t1_end   end of first datetime
     * @param t2_start start of second datetim
     * @param t2_end   end of second datetime
     *
     * @return boolean, true if overlap, false if not
     */
    private static boolean isTimeOverlap(LocalDateTime t1_start,
                                         LocalDateTime t1_end,
                                         LocalDateTime t2_start,
                                         LocalDateTime t2_end
    ) {
        return t1_start.isBefore(t2_end) && t2_start.isBefore(t1_end);
    }
    


    public static void insertData(MongoDatabase db) throws SQLException, IOException {

        DatasetParser datasetParser = new DatasetParser();
        File          datasetDir    = new File("/datasett/Data/");

        List<User>   users = datasetParser.parseDataset(datasetDir);

        var activityCollection = db.getCollection(Activity.collection);
        var userCollection = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

        users.parallelStream().forEach(user -> {
            user.getActivities().forEach(activity -> {
                trackPointCollection.insertMany(activity.getTrackPoints().stream().map(t -> t.toDocument(activity.getId())).toList());
                activityCollection.insertOne(activity.toDocument(user.getId()));

                //activityCollection.insertOne(activity.toDnormDocument(user.getId()));
            });
            userCollection.insertOne(user.toDocument());
        });
    }
    private static SimpleTable<List<String>> makeResultSetTable(ResultSet resultSet) throws SQLException {
    return null;
    }



    public static void task1(MongoDatabase db) throws SQLException {
        var activityCollection = db.getCollection(Activity.collection);
        var userCollection = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

        long activityCount = activityCollection.countDocuments();
        long userCount = userCollection.countDocuments();
        long trackpointCount = trackPointCollection.countDocuments();

        System.out.printf("""
                          ### task 1 ###
                          num activitys = %s
                          num users     = %s
                          num tp        = %s
                          \n
                          """, activityCount,userCount,trackpointCount);


    }

    public static void task2(MongoDatabase db) {
        var activityCollection = db.getCollection(Activity.collection);
        var userCollection = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

        var agr = Arrays.asList(new Document("$project",
                                   new Document("num_activities",
                                                new Document("$size", "$activities"))),
                      new Document("$group",
                                   new Document("_id",
                                                new BsonNull())
                                           .append("avg",
                                                   new Document("$avg", "$num_activities"))
                                           .append("min",
                                                   new Document("$min", "$num_activities"))
                                           .append("max",
                                                   new Document("$max", "$num_activities"))));

        var aggregate = userCollection.aggregate(agr);
        Document document = aggregate.iterator().next();
        double avg = document.getDouble("avg");
        int max = document.getInteger("max");
        int min = document.getInteger("min");

        System.out.printf("""
                          ### task 2 ###
                          num avg = %s
                          num max = %s
                          num min = %s
                          \n
                          """, avg,max,min);

    }

    public static void task3(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

        var agr = Arrays.asList(new Document("$project",
                                             new Document("num_activities",
                                                          new Document("$size", "$activities"))),
                                new Document("$sort",
                                             new Document("num_activities", -1L)),
                                new Document("$limit", 10L));

        Iterator<Document> documents = userCollection.aggregate(agr).iterator();

        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 3");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("id",document -> document.getInteger("_id")),
                new Column<Document>("num activities",document -> document.getInteger("num_activities"))
        );
        simpleTable.display();
    }

    public static void task4(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

        /*
        db.activity.aggregate([
            {
                $addFields: {
                    days_over: {
                        $dateDiff:{
                            startDate: "$startDateTime",
                            endDate: "$endDateTime",
                            unit: "day"
                        }
                    }
                }
            },
            {
                $match: {
                    days_over: {$gte: 0}
                }
            },
            {
                $count: "days_over"
            }
        ])
         */

        var agr = Arrays.asList(new Document("$addFields",
                                             new Document("days_over",
                                                          new Document("$dateDiff",
                                                                       new Document("startDate", "$startDateTime")
                                                                               .append("endDate", "$endDateTime")
                                                                               .append("unit", "day")))),
                                new Document("$match",
                                             new Document("days_over",
                                                          new Document("$gt", 0L))),
                                new Document("$count", "days_over"));

        Iterator<Document> documents = activityCollection.aggregate(agr).iterator();
        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 4");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("activity's passing midnight",document -> document.getInteger("days_over"))
        );
        simpleTable.display();

    }

    public static void task5(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);
        String query = """
                       SELECT a.id AS duplicate_asignment_ids
                       FROM activity AS a,
                           (
                               SELECT user_id, start_date_time, end_date_time
                               FROM activity
                               GROUP BY user_id, start_date_time, end_date_time
                               HAVING COUNT(*) > 1
                           ) AS f
                       WHERE a.user_id = f.user_id
                       AND  a.start_date_time = f.start_date_time
                       AND a.end_date_time = f.end_date_time;
                       """;
                /*
        db.activity.aggregate(
        [{
            $group: {
                _id: {
                    startDateT: "$startDateTime",
                    edt: "$endDateTime",
                    uid: "$user_id"
                },
                count: {
                    $count: {}
                },
                ids: {
                    $addToSet: "$_id"
                }
            }
        }, {
            $match: {

                count: {
                    $gt: 1
                }

            }
        }]
        )
         */

        var agr = Arrays.asList(new Document("$group",
                                             new Document("_id",
                                                          new Document("startDateT", "$startDateTime")
                                                                  .append("edt", "$endDateTime")
                                                                  .append("uid", "$user_id"))
                                                     .append("count",
                                                             new Document("$count",
                                                                          new Document()))
                                                     .append("ids",
                                                             new Document("$addToSet", "$_id"))),
                                new Document("$match",
                                             new Document("count",
                                                          new Document("$gt", 1L))));

        Iterator<Document> documents = activityCollection.aggregate(agr).iterator();


        if (! documents.hasNext()) {
            System.out.println("Task 5");
            System.out.println("no results");
        } else {
            SimpleTable<Document> simpleTable = new SimpleTable<>();
            simpleTable.setTitle("Task 5");
            simpleTable.setItems(documents);
            simpleTable.setCols(
                    new Column<Document>("id",document -> document.getInteger("_id"))
            );
            simpleTable.display();
        }
    }

    public static void task6(MongoDatabase db) {
        /*
        noe som heter geo near finnes men siden implementasjonen er fucking stupid så kan den ikke
        brukes uten og endre hvordan data er lagret til MongoDB sitt BS format



[{$addFields: {
"seconds_dif": {
                $abs:{
                    $dateDiff:{
                        startDate: ISODate("2008-08-24T15:38:00"),
                        endDate: "$dateTime",
                        unit: "second"
                    }
                }
            }}}, {$match: {
  "seconds_dif": {$lte: 60}
}}]


         */



        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);


        var agr = Arrays.asList(new Document("$addFields",
                                            new Document("seconds_dif",
                                                         new Document("$abs",
                                                                      new Document("$dateDiff",
                                                                                   new Document("startDate", Date.from(Instant.parse("2008-08-24T15:38:00Z")))
                                                                                           .append("endDate", "$dateTime")
                                                                                           .append("unit", "second"))))),
                               new Document("$match",
                                            new Document("seconds_dif",
                                                         new Document("$lte", 60L))));

        Iterator<Document> itr = trackPointCollection.find().iterator();
        ArrayList<Document> tpDocuments = new ArrayList<>();

        itr.forEachRemaining(tpDocuments::add);

        double target_lat = 39.97548;
        double target_lon = 116.33031;


        List<Integer> toCloseIds = tpDocuments.parallelStream().map(document -> {
            double kmDist = haversineDistance(document.getDouble("latitude"),target_lat, document.getDouble("longitude"), target_lon);
            double mDist = kmDist * 1000;
            if (mDist <= 100){
                return OptionalInt.of(document.getInteger("activity_id"));
            } else {
                return empty();
            }
        }).filter(OptionalInt::isPresent).map(OptionalInt::getAsInt).distinct().toList();


        var infectedUsersFilter = Arrays.asList(new Document("$match",
                                                              new Document("_id",
                                                                           new Document("$in", toCloseIds))),
                                                 new Document("$group",
                                                              new Document("_id", "$user_id")),
                                                new Document("$sort",
                                                             new Document("_id", 1L)));

        var infectedUsers = activityCollection.aggregate(infectedUsersFilter).iterator();


        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 6");
        simpleTable.setItems(infectedUsers);
        simpleTable.setCols(
                new Column<Document>("infected users",document -> document.getInteger("_id"))
        );
        simpleTable.display();



    }

    public static void task7(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);


        String query = """
                       SELECT id
                       FROM user
                       WHERE id NOT IN (
                                       SELECT DISTINCT c.user_id
                                       FROM(
                                               SELECT user_id, transportation_mode
                                               FROM activity
                                               WHERE transportation_mode = 'taxi') AS c);
                       """;
        var agr = Arrays.asList(new Document("$match",
                                             new Document("transportationMode",
                                                          new Document("$not",
                                                                       new Document("$in", Arrays.asList("taxi"))))),
                                new Document("$group",
                                             new Document("_id", "$user_id")),
                                new Document("$count", "num_users"));

        Iterator<Document> documents = activityCollection.aggregate(agr).iterator();


        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 7");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("non taxi users",document -> document.getInteger("num_users"))
        );
        simpleTable.display();

    }
//
//
//    public static void task8(MongoDatabase db) {
//        var activityCollection   = db.getCollection(Activity.collection);
//        var userCollection       = db.getCollection(User.collection);
//        var trackPointCollection = db.getCollection(TrackPoint.collection);
//        String query = """
//                       SELECT transportation_mode, COUNT(DISTINCT user_id)
//                       FROM activity
//                       WHERE transportation_mode IS NOT NULL
//                       GROUP BY transportation_mode;
//                       """;
//        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
//        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
//        simpleTable.setTitle("Task 8");
//        simpleTable.display();
//    }
//
   private static void task9a(MongoDatabase db) {
       var activityCollection   = db.getCollection(Activity.collection);

       System.out.println("Task 9a");

       var agr = Arrays.asList( new Document("$group", 
                                new Document("_id", 
                                new Document("year", 
                                new Document("$year", "$startDateTime"))
                                        .append("month", 
                                new Document("$month", "$startDateTime")))
                                        .append("count", 
                                new Document("$sum", 1L))), 
                                new Document("$sort", 
                                new Document("count", -1L)), 
                                new Document("$limit", 1L)
                            );
       
       Iterator<Document> documents = activityCollection.aggregate(agr).iterator();
       SimpleTable<Document> simpleTable = new SimpleTable<>();
       simpleTable.setTitle("Task 9a");
       simpleTable.setItems(documents);
       simpleTable.setCols(
            new Column<Document>("Year", document -> document.getEmbedded(List.of("_id", "year"), Integer.class)),
            new Column<Document>("Month", document -> document.getEmbedded(List.of("_id", "month"), Integer.class)),
            new Column<Document>("Count", document -> document.getLong("count"))
        );
       simpleTable.display();
   }

   private static void task9b(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);

        var agr = Arrays.asList(new Document("$group", 
                                new Document("_id", 
                                new Document("year", 
                                new Document("$year", "$startDateTime"))
                                            .append("month", 
                                new Document("$month", "$startDateTime")))
                                        .append("count", 
                                new Document("$sum", 1L))
                                        .append("user_ids", 
                                new Document("$push", 
                                new Document("user_id", "$user_id")
                                                .append("hours", 
                                new Document("$divide", Arrays.asList(new Document("$subtract", Arrays.asList("$endDateTime", "$startDateTime")), 60L * 1000L * 60L)))))), 
                                new Document("$sort", 
                                new Document("count", -1L)), 
                                new Document("$limit", 1L), 
                                new Document("$unwind", 
                                new Document("path", "$user_ids")), 
                                new Document("$group", 
                                new Document("_id", 
                                new Document("user_id", "$user_ids.user_id"))
                                        .append("total_hours", 
                                new Document("$sum", "$user_ids.hours"))
                                        .append("num_activities", 
                                new Document("$sum", 1L))), 
                                new Document("$sort", 
                                new Document("num_activities", -1L)), 
                                new Document("$limit", 3L));

        Iterator<Document> documents = activityCollection.aggregate(agr).iterator();
        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 9b");
        simpleTable.setItems(documents);
        simpleTable.setCols(
            new Column<Document>("User ID", document -> document.getEmbedded(List.of("_id", "user_id"), Integer.class)),
            new Column<Document>("Num Activities", document -> document.getLong("num_activities")),
            new Column<Document>("Total Hours", document -> document.getDouble("total_hours"))
        );
        simpleTable.display();

        List<Document> tableData = simpleTable.getItems();

        Double timeSpentTop    = tableData.get(0).getDouble("total_hours");
        Double timeSpentSecond = tableData.get(1).getDouble("total_hours");

        int dif = timeSpentTop.compareTo(timeSpentSecond);
        if (dif > 0) {
            System.out.printf(
                    "The user with the most activities, user: %s, spent more time on their activities than the user with the second most activities\n",
                    tableData.get(0).getEmbedded(List.of("_id", "user_id"), Integer.class)
            );
        } else if (dif < 0) {
            System.out.printf(
                    "The user with the next most activities, user: %s, spent more time on their activities than the user with the second most activities\n",
                    tableData.get(1).getEmbedded(List.of("_id", "user_id"), Integer.class)
            );
        } else {
            System.out.println("The top and next top users spent the same time activitying");
        }

        int hoursTop = timeSpentTop.intValue();
        int minutesTop = (int) ((timeSpentTop - hoursTop) * 60);
        int secondsTop = (int) ((timeSpentTop - hoursTop - minutesTop/ 60.0) * 3600);
        
        System.out.printf("User: %-4s with the most activities spent     : Hours: %-3s Min: %-2s Sec: %s\n",
                            tableData.get(0).getEmbedded(List.of("_id", "user_id"), Integer.class),
                            hoursTop,
                            minutesTop,
                            secondsTop                 
        );
        
        int hoursSecond = timeSpentSecond.intValue();
        int minutesSecond = (int) ((timeSpentSecond - hoursSecond) * 60);
        int secondsSecond = (int) ((timeSpentSecond - hoursSecond - minutesSecond / 60.0) * 3600);

        System.out.printf("User: %-4s with the second most activities spent: Hours: %-3s Min: %-2s Sec: %s\n",
                            tableData.get(1).getEmbedded(List.of("_id", "user_id"), Integer.class),
                            hoursSecond,
                            minutesSecond,
                            secondsSecond                  
        );

   }

   public static void task9(MongoDatabase db) {
       task9a(db);
       task9b(db);
   }
//
//    public static void task10(MongoDatabase db) {
//        var activityCollection   = db.getCollection(Activity.collection);
//        var userCollection       = db.getCollection(User.collection);
//        var trackPointCollection = db.getCollection(TrackPoint.collection);
//        String query = """
//                       SELECT tp.*
//                       FROM (SELECT * FROM user WHERE user.id = 113) AS u # we use 113 instead of 112 because the indexes are shifted
//                       INNER JOIN (
//                           SELECT *
//                           FROM activity
//                           WHERE YEAR(start_date_time) = 2008
//                           AND transportation_mode = 'walk'
//                           ) a
//                       ON u.id = a.user_id
//                       INNER JOIN track_point tp
//                       ON a.id = tp.activity_id
//                       """;
//        ResultSet resultSet = connection.createStatement().executeQuery(query);
//
//        HashMap<Integer, Activity> activities = new HashMap<>();
//
//        while (resultSet.next()) {
//            TrackPoint trackPoint = new TrackPoint();
//            trackPoint.setLatitude(resultSet.getDouble(3));
//            trackPoint.setLongitude(resultSet.getDouble(4));
//
//            int activityId = resultSet.getInt(2);
//            if (! activities.containsKey(activityId)) {
//                activities.put(activityId, new Activity(activityId));
//            }
//            Activity parentAct = activities.get(activityId);
//            parentAct.getTrackPoints().add(trackPoint);
//        }
//
//        double totDist = activities.values().stream().parallel().map(activity -> {
//            TrackPoint keep = null;
//            double     roll = 0;
//            for (TrackPoint trackPoint : activity.getTrackPoints()) {
//                if (keep == null) {
//                    keep = trackPoint;
//                    continue;
//                }
//                roll += haversineDistance(trackPoint.getLatitude(),
//                                           keep.getLatitude(),
//                                           trackPoint.getLongitude(),
//                                           keep.getLongitude()
//                );
//                keep = trackPoint;
//
//            }
//            return roll;
//        }).reduce(Double::sum).get();
//
//        System.out.println("\tTask 10");
//        System.out.printf("User 112 have in total walked %.3f km in 2008\n", totDist);
//
//    }
//
//    public static void task11(MongoDatabase db) {
//        var activityCollection   = db.getCollection(Activity.collection);
//        var userCollection       = db.getCollection(User.collection);
//        var trackPointCollection = db.getCollection(TrackPoint.collection);
//        String query = """
//                       WITH delta_alt_tps AS (
//                           SELECT track_point.id, track_point.activity_id , LEAD(track_point.altitude) OVER (PARTITION BY track_point.activity_id ORDER BY id) - track_point.altitude AS delta_alt
//                           FROM track_point
//                           WHERE track_point.altitude != -777
//                           AND track_point.altitude IS NOT NULL
//                       ),
//                       delta_alt_act AS (
//                           SELECT delta_alt_tps.activity_id, SUM(IF(delta_alt_tps.delta_alt > 0, delta_alt_tps.delta_alt, 0)) AS altitude_gain
//                           FROM delta_alt_tps
//                           GROUP BY delta_alt_tps.activity_id)
//                       SELECT activity.user_id, (SUM(delta_alt_act.altitude_gain)/ 3.2808) AS user_altitude_gain_m
//                       FROM activity
//                                JOIN delta_alt_act ON activity.id = delta_alt_act.activity_id
//                       GROUP BY activity.user_id
//                       ORDER BY  user_altitude_gain_m DESC
//                       LIMIT 20;
//                       """;
//        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
//        SimpleTable simpleTable = makeResultSetTable(resultSet);
//        simpleTable.setTitle("Task 11");
//        simpleTable.display();
//    }
//
//    public static void task12(MongoDatabase db) {
//        var activityCollection   = db.getCollection(Activity.collection);
//        var userCollection       = db.getCollection(User.collection);
//        var trackPointCollection = db.getCollection(TrackPoint.collection);
//        String query = """
//                       WITH shifted_tps AS (
//                           SELECT
//                               track_point.activity_id,
//                               track_point.date_time,
//                               LEAD(track_point.date_time)
//                                   OVER (PARTITION BY track_point.activity_id ORDER BY track_point.id) AS shifted_time
//                           FROM
//                               track_point
//                       )
//                       SELECT activity.user_id, COUNT(*) AS num_invalid_activities
//                       FROM (
//                               SELECT shifted_tps.activity_id, MINUTE(TIMEDIFF(shifted_tps.date_time, shifted_tps.shifted_time)) AS td
//                               FROM shifted_tps
//                               WHERE MINUTE(TIMEDIFF(shifted_tps.date_time, shifted_tps.shifted_time)) > 5
//                       ) AS invalid_acts
//                       INNER JOIN activity
//                       ON invalid_acts.activity_id = activity.id
//                       GROUP BY activity.user_id
//                       ORDER BY num_invalid_activities DESC;
//                       """;
//        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
//        SimpleTable simpleTable = makeResultSetTable(resultSet);
//        simpleTable.setTitle("Task 12");
//        simpleTable.display();
//    }
}
