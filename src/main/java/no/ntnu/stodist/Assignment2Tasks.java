package no.ntnu.stodist;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import no.ntnu.stodist.models.Activity;
import no.ntnu.stodist.models.TrackPoint;
import no.ntnu.stodist.models.User;
import no.ntnu.stodist.simpleTable.Column;
import no.ntnu.stodist.simpleTable.SimpleTable;
import org.bson.Document;

import java.util.*;

import org.bson.BsonNull;

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


    public static void task8(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);
        String query = """
                       SELECT transportation_mode, COUNT(DISTINCT user_id)
                       FROM activity
                       WHERE transportation_mode IS NOT NULL
                       GROUP BY transportation_mode;
                       """;
        var agr = Arrays.asList(new Document("$group",
                                             new Document("_id",
                                                          new Document("transportationMode", "$transportationMode")
                                                                  .append("user_id", "$user_id"))
                                                     .append("cnt",
                                                             new Document("$count",
                                                                          new Document()))),
                                new Document("$group",
                                             new Document("_id", "$_id.transportationMode")
                                                     .append("cnt",
                                                             new Document("$count",
                                                                          new Document()))));

        Iterator<Document> documents = activityCollection.aggregate(agr).iterator();


        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 8");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("transportation type",document -> document.getString("_id")),
                new Column<Document>("num",document -> document.getInteger("cnt"))

        );
        simpleTable.display();
    }
//
//    private static void task9a(MongoDatabase db) {
//        var activityCollection   = db.getCollection(Activity.collection);
//        var userCollection       = db.getCollection(User.collection);
//        var trackPointCollection = db.getCollection(TrackPoint.collection);
//        String query = """
//                       SELECT COUNT(*) AS num_activites, YEAR(activity.start_date_time) AS year, MONTH(activity.start_date_time) AS month
//                       FROM activity
//                       GROUP BY year, month
//                       ORDER BY num_activities DESC
//                       LIMIT 1;
//                       """;
//        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
//        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
//        simpleTable.setTitle("Task 9a");
//        simpleTable.display();
//    }
//
//    private static void task9b(MongoDatabase db) {
//        var activityCollection   = db.getCollection(Activity.collection);
//        var userCollection       = db.getCollection(User.collection);
//        var trackPointCollection = db.getCollection(TrackPoint.collection);
//        String query = """
//                       SELECT COUNT(a.user_id) AS num_activities, a.user_id, SUM(TIMEDIFF(a.end_date_time,a.start_date_time)) AS time_spent
//                       FROM
//                            activity AS a,
//                            (SELECT COUNT(*) AS num_activites, YEAR(activity.start_date_time) AS year, MONTH(activity.start_date_time) AS month
//                             FROM activity
//                             GROUP BY year, month
//                             ORDER BY num_activities DESC
//                             LIMIT 1) AS best_t
//                       WHERE YEAR(a.start_date_time) = best_t.year
//                       AND MONTH(a.start_date_time) = best_t.month
//                       GROUP BY user_id
//                       ORDER BY COUNT(a.user_id) DESC
//                       LIMIT 2;
//                       """;
//        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
//        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
//        simpleTable.setTitle("Task 9b");
//        simpleTable.display();
//
//        List<List<String>> tabelData = simpleTable.getItems();
//
//        var timeSpentTop    = Duration.ofSeconds(Long.parseLong(tabelData.get(0).get(2)));
//        var timeSpentSecond = Duration.ofSeconds(Long.parseLong(tabelData.get(1).get(2)));
//
//        int dif = timeSpentTop.compareTo(timeSpentSecond);
//        if (dif > 0) {
//            System.out.printf(
//                    "the user with the most activities, user: %s spent more time activitying than the user with the second most activities\n",
//                    tabelData.get(0).get(0)
//            );
//        } else if (dif < 0) {
//            System.out.printf(
//                    "the user with the next most activities, user: %s spent more time activitying than the user with the second most activities\n",
//                    tabelData.get(1).get(0)
//            );
//        } else {
//            System.out.println("the top and next top users spent the same time activitying");
//        }
//
//        System.out.printf("user: %-4s with most activities spent     : Hours: %-3s Min: %-2s Sec: %s\n",
//                          tabelData.get(0).get(0),
//                          timeSpentTop.toHours(),
//                          timeSpentTop.minusHours(timeSpentTop.toHours()).toMinutes(),
//                          timeSpentTop.minusMinutes(timeSpentTop.toMinutes())
//                                      .toSeconds()
//        );
//        System.out.printf("user: %-4s with next most activities spent: Hours: %-3s Min: %-2s Sec: %s\n",
//                          tabelData.get(1).get(0),
//                          timeSpentSecond.toHours(),
//                          timeSpentSecond.minusHours(timeSpentSecond.toHours()).toMinutes(),
//                          timeSpentSecond.minusMinutes(timeSpentSecond.toMinutes())
//                                         .toSeconds()
//        );
//
//
//    }
//
//    public static void task9(MongoDatabase db) {
//        task9a(db);
//        task9b(db);
//    }
//
//

    public static void task10(MongoDatabase db)
    {
        MongoCollection<Document> activityCollection = db.getCollection("activity");
        MongoCollection<Document> trackPointCollection = db.getCollection("trackPoint");

        var agr = Arrays.asList(new Document("$match",
                        new Document("user_id", 113L)
                                .append("transportationMode", "walk")
                                .append("startDateTime",
                                        new Document("$gte",
                                                new java.util.Date(1199145600000L))
                                                .append("$lte",
                                                        new java.util.Date(1230768000000L)))),
                new Document("$group",
                        new Document("_id", 1L)
                                .append("ac_ids",
                                        new Document("$push",
                                                new Document("id", "$_id")))));

        Iterator<Document> dap = activityCollection.aggregate(agr).iterator();
        List<Integer> ids = new ArrayList<>();
        List<Document> list = (List<Document>)dap.next().get("ac_ids");
        for(Document d : list)
        {
            ids.add(d.getInteger("id"));
        }

        var agr_t = Arrays.asList(new Document("$match",
                        new Document("activity_id",
                                new Document("$in", ids))),
                new Document("$project",
                        new Document("dateDays", 1L)
                                .append("activity_id", 1L)
                                .append("latitude", 1L)
                                .append("longitude", 1L)),
                new Document("$sort",
                        new Document("dateDays", 1L)));

        double tot_dist = 0.0d;
        int cur_id = -1;
        double last_lon = 0.0d;
        double last_lat = 0.0d;

        for(Document d : trackPointCollection.aggregate(agr_t))
        {
            int ac_id = d.getInteger("activity_id");
            if(cur_id == ac_id)
            {
                tot_dist += haversineDistance(d.getDouble("latitude"), last_lat, d.getDouble("longitude"), last_lon);
            }
            cur_id = ac_id;
            last_lon = d.getDouble("longitude");
            last_lat = d.getDouble("latitude");
        }
        System.out.println("Total distance walked by user 112 in 2008");
        System.out.println(tot_dist);
    }


    public static void task11(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

/*
[{
    $setWindowFields: {
        partitionBy: "$activity_id",
        sortBy: {
            "dateDays": 1
        },
        output: {
            altitude_shift: {
                $shift: {
                    output: "$altitude",
                    by: -1,
                    default: "Not available"
                }
            }
        }
    }
}, {
    $match: {
        $expr: {
            $gt: ["$altitude", "$altitude_shift"]
        }
    }
}, {
    $addFields: {
        altitude_delta: {
            $subtract: ["$altitude", "$altitude_shift"]
        }
    }
}, {
    $group: {
        _id: "$activity_id",
        activity_alt_gain: {
            $sum: "$altitude_delta"
        }
    }
}, {
    $lookup: {
        from: 'activity',
        localField: '_id',
        foreignField: '_id',
        as: 'act_data'
    }
}, {
    $unwind: {
        path: "$act_data",
    }
}, {
    $group: {
        _id: "$act_data.user_id",
        usr_gain: {
            $sum: "$activity_alt_gain"
        }
    }
}, {
    $sort: {
        usr_gain: -1
    }
}, {
    $limit: 20
}]
 */
        var agr = Arrays.asList(new Document("$setWindowFields",
                                             new Document("partitionBy", "$activity_id")
                                                     .append("sortBy",
                                                             new Document("_id", 1L))
                                                     .append("output",
                                                             new Document("altitude_shift",
                                                                          new Document("$shift",
                                                                                       new Document("output", "$altitude")
                                                                                               .append("by", -1L)
                                                                                               .append("default", "Not available"))))),
                                new Document("$match",
                                             new Document("$expr",
                                                          new Document("$gt", Arrays.asList("$altitude", "$altitude_shift")))),
                                new Document("$addFields",
                                             new Document("altitude_delta",
                                                          new Document("$subtract", Arrays.asList("$altitude", "$altitude_shift")))),
                                new Document("$group",
                                             new Document("_id", "$activity_id")
                                                     .append("activity_alt_gain",
                                                             new Document("$sum", "$altitude_delta"))),
                                new Document("$lookup",
                                             new Document("from", "activity")
                                                     .append("localField", "_id")
                                                     .append("foreignField", "_id")
                                                     .append("as", "act_data")),
                                new Document("$unwind",
                                             new Document("path", "$act_data")),
                                new Document("$group",
                                             new Document("_id", "$act_data.user_id")
                                                     .append("usr_gain",
                                                             new Document("$sum", "$activity_alt_gain"))),
                                new Document("$sort",
                                             new Document("usr_gain", -1L)),
                                new Document("$addFields",
                                             new Document("usr_gain_m",
                                                          new Document("$multiply", Arrays.asList("$usr_gain", 0.3048d)))));

        Iterator<Document> documents = trackPointCollection.aggregate(agr).allowDiskUse(true).iterator();


        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 11");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("user",document -> document.getInteger("_id")),
                new Column<Document>("altitude gain",document -> document.getDouble("usr_gain"))

        );
        simpleTable.display();
    }

    public static void task12(MongoDatabase db) {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

        /*
        [{$setWindowFields: {
  partitionBy: "$activity_id",
  sortBy: {"dateDays": 1},
  output: {
      time_shifted: {
          $shift: {
              output: "$dateTime",
              by: -1,
              default: null
          }
      }
  }
}}, {$match: {
  time_shifted: {$ne: null}
}}, {$addFields: {
  "min_dif": {
    $divide: [
                {
                  $abs:{
                  $subtract: ["$time_shifted", "$dateTime"]
                  }
                },
                {
                  $multiply: [ 60 , 1000]
                }
            ]
}}}, {$match: {
  min_dif: {$gte: 5}
}}, {$group: {
  _id: "$activity_id",
  num_invalid: {
    $count: {}
  }
}}, {$lookup: {
  from: 'activity',
  localField: '_id',
  foreignField: '_id',
  as: 'act_data'
}}, {$unwind: {
  path: "$act_data",
}}, {$group: {
  _id: "$act_data.user_id",
  num_invalid: {
    $count: {}
  }
}}, {$sort: {
  num_invalid: -1
}}]
         */

        var agr = Arrays.asList(new Document("$setWindowFields",
                                             new Document("partitionBy", "$activity_id")
                                                     .append("sortBy",
                                                             new Document("_id", 1L))
                                                     .append("output",
                                                             new Document("time_shifted",
                                                                          new Document("$shift",
                                                                                       new Document("output", "$dateTime")
                                                                                               .append("by", -1L)
                                                                                               .append("default",
                                                                                                       new BsonNull()))))),
                                new Document("$match",
                                             new Document("time_shifted",
                                                          new Document("$ne",
                                                                       new BsonNull()))),
                                new Document("$addFields",
                                             new Document("min_dif",
                                                          new Document("$divide", Arrays.asList(new Document("$abs",
                                                                                                             new Document("$subtract", Arrays.asList("$time_shifted", "$dateTime"))),
                                                                                                new Document("$multiply", Arrays.asList(60L, 1000L)))))),
                                new Document("$match",
                                             new Document("min_dif",
                                                          new Document("$gte", 5L))),
                                new Document("$group",
                                             new Document("_id", "$activity_id")
                                                     .append("num_invalid",
                                                             new Document("$count",
                                                                          new Document()))),
                                new Document("$lookup",
                                             new Document("from", "activity")
                                                     .append("localField", "_id")
                                                     .append("foreignField", "_id")
                                                     .append("as", "act_data")),
                                new Document("$unwind",
                                             new Document("path", "$act_data")),
                                new Document("$group",
                                             new Document("_id", "$act_data.user_id")
                                                     .append("num_invalid",
                                                             new Document("$count",
                                                                          new Document()))),
                                new Document("$sort",
                                             new Document("num_invalid", -1L)));

        Iterator<Document> documents = trackPointCollection.aggregate(agr).allowDiskUse(true).iterator();


        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 12");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("user",document -> document.getInteger("_id")),
                new Column<Document>("num invalid",document -> document.getInteger("num_invalid"))

        );
        simpleTable.display();
    }
}
