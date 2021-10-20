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
import java.time.LocalDateTime;

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

    public static void task4(MongoDatabase db) throws SQLException {
        var activityCollection   = db.getCollection(Activity.collection);
        var userCollection       = db.getCollection(User.collection);
        var trackPointCollection = db.getCollection(TrackPoint.collection);

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

    public static void task5(Connection connection) throws SQLException {
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
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        resultSet.next();

        if (! resultSet.next()) {
            System.out.println("Task 5");
            System.out.println("no results");
        } else {
            SimpleTable simpleTable = makeResultSetTable(resultSet);
            simpleTable.setTitle("Task 5");
            simpleTable.display();
        }
    }

    public static void task6(Connection connection) throws SQLException {
        String query = """
                       WITH tps AS (
                           SELECT user_id, activity_id,tp.id AS act_id, lat, lon, date_time
                           FROM activity a
                                    JOIN track_point tp
                                         ON a.id = tp.activity_id)
                       SELECT DISTINCT tps.user_id AS user_1, tps2.user_id AS user_2
                       FROM tps
                       INNER JOIN tps AS tps2
                       ON tps.user_id != tps2.user_id
                       AND SECOND(ABS(TIMEDIFF(tps.date_time, tps2.date_time))) <= 60
                       AND ST_DISTANCE(POINT(tps.lat, tps.lon), POINT(tps2.lat, tps2.lon)) <= 100;                    
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 6");
        simpleTable.display();
    }

    public static void task7(Connection connection) throws SQLException {


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
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 7");
        simpleTable.display();

    }


    public static void task8(Connection connection) throws SQLException {
        String query = """
                       SELECT transportation_mode, COUNT(DISTINCT user_id)
                       FROM activity
                       WHERE transportation_mode IS NOT NULL
                       GROUP BY transportation_mode;        
                       """;
        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 8");
        simpleTable.display();
    }

    private static void task9a(Connection connection) throws SQLException {
        String query = """
                       SELECT COUNT(*) AS num_activites, YEAR(activity.start_date_time) AS year, MONTH(activity.start_date_time) AS month
                       FROM activity
                       GROUP BY year, month
                       ORDER BY num_activities DESC
                       LIMIT 1;
                       """;
        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 9a");
        simpleTable.display();
    }

    private static void task9b(Connection connection) throws SQLException {
        String query = """
                       SELECT COUNT(a.user_id) AS num_activities, a.user_id, SUM(TIMEDIFF(a.end_date_time,a.start_date_time)) AS time_spent
                       FROM
                            activity AS a,
                            (SELECT COUNT(*) AS num_activites, YEAR(activity.start_date_time) AS year, MONTH(activity.start_date_time) AS month
                             FROM activity
                             GROUP BY year, month
                             ORDER BY num_activities DESC
                             LIMIT 1) AS best_t
                       WHERE YEAR(a.start_date_time) = best_t.year
                       AND MONTH(a.start_date_time) = best_t.month
                       GROUP BY user_id
                       ORDER BY COUNT(a.user_id) DESC
                       LIMIT 2;
                       """;
        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 9b");
        simpleTable.display();

        List<List<String>> tabelData = simpleTable.getItems();

        var timeSpentTop    = Duration.ofSeconds(Long.parseLong(tabelData.get(0).get(2)));
        var timeSpentSecond = Duration.ofSeconds(Long.parseLong(tabelData.get(1).get(2)));

        int dif = timeSpentTop.compareTo(timeSpentSecond);
        if (dif > 0) {
            System.out.printf(
                    "the user with the most activities, user: %s spent more time activitying than the user with the second most activities\n",
                    tabelData.get(0).get(0)
            );
        } else if (dif < 0) {
            System.out.printf(
                    "the user with the next most activities, user: %s spent more time activitying than the user with the second most activities\n",
                    tabelData.get(1).get(0)
            );
        } else {
            System.out.println("the top and next top users spent the same time activitying");
        }

        System.out.printf("user: %-4s with most activities spent     : Hours: %-3s Min: %-2s Sec: %s\n",
                          tabelData.get(0).get(0),
                          timeSpentTop.toHours(),
                          timeSpentTop.minusHours(timeSpentTop.toHours()).toMinutes(),
                          timeSpentTop.minusMinutes(timeSpentTop.toMinutes())
                                      .toSeconds()
        );
        System.out.printf("user: %-4s with next most activities spent: Hours: %-3s Min: %-2s Sec: %s\n",
                          tabelData.get(1).get(0),
                          timeSpentSecond.toHours(),
                          timeSpentSecond.minusHours(timeSpentSecond.toHours()).toMinutes(),
                          timeSpentSecond.minusMinutes(timeSpentSecond.toMinutes())
                                         .toSeconds()
        );


    }

    public static void task9(Connection connection) throws SQLException {
        task9a(connection);
        task9b(connection);
    }

    public static void task10(MongoDatabase db) throws SQLException
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
        System.out.println(tot_dist);
    }


    public static void task11(Connection connection) throws SQLException {
        String query = """
                       WITH delta_alt_tps AS (
                           SELECT track_point.id, track_point.activity_id , LEAD(track_point.altitude) OVER (PARTITION BY track_point.activity_id ORDER BY id) - track_point.altitude AS delta_alt
                           FROM track_point
                           WHERE track_point.altitude != -777
                           AND track_point.altitude IS NOT NULL
                       ),
                       delta_alt_act AS (
                           SELECT delta_alt_tps.activity_id, SUM(IF(delta_alt_tps.delta_alt > 0, delta_alt_tps.delta_alt, 0)) AS altitude_gain
                           FROM delta_alt_tps
                           GROUP BY delta_alt_tps.activity_id)
                       SELECT activity.user_id, (SUM(delta_alt_act.altitude_gain)/ 3.2808) AS user_altitude_gain_m
                       FROM activity
                                JOIN delta_alt_act ON activity.id = delta_alt_act.activity_id
                       GROUP BY activity.user_id
                       ORDER BY  user_altitude_gain_m DESC
                       LIMIT 20;  
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 11");
        simpleTable.display();
    }

    public static void task12(Connection connection) throws SQLException {
        String query = """
                       WITH shifted_tps AS (
                           SELECT
                               track_point.activity_id,
                               track_point.date_time,
                               LEAD(track_point.date_time)
                                   OVER (PARTITION BY track_point.activity_id ORDER BY track_point.id) AS shifted_time
                           FROM
                               track_point
                       )
                       SELECT activity.user_id, COUNT(*) AS num_invalid_activities
                       FROM (
                               SELECT shifted_tps.activity_id, MINUTE(TIMEDIFF(shifted_tps.date_time, shifted_tps.shifted_time)) AS td
                               FROM shifted_tps
                               WHERE MINUTE(TIMEDIFF(shifted_tps.date_time, shifted_tps.shifted_time)) > 5
                       ) AS invalid_acts
                       INNER JOIN activity
                       ON invalid_acts.activity_id = activity.id
                       GROUP BY activity.user_id
                       ORDER BY num_invalid_activities DESC;
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 12");
        simpleTable.display();
    }
}
