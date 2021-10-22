package no.ntnu.stodist;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.AggregateIterable;
import no.ntnu.stodist.models.Activity;
import no.ntnu.stodist.models.TrackPoint;
import no.ntnu.stodist.models.User;
import no.ntnu.stodist.simpleTable.Column;
import no.ntnu.stodist.simpleTable.SimpleTable;
import org.bson.Document;

import java.time.Instant;
import java.util.*;

import org.bson.BsonNull;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Date;

import static java.util.OptionalInt.empty;


public class Assignment3Tasks {

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

    public static void insertData(MongoDatabase db) throws SQLException, IOException {

        DatasetParser datasetParser = new DatasetParser();
        File          datasetDir    = new File("/datasett/Data/");

        List<User>   users = datasetParser.parseDataset(datasetDir);

        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);
        MongoCollection<Document> userCollection = db.getCollection(User.collection);
        MongoCollection<Document> trackPointCollection = db.getCollection(TrackPoint.collection);

        users.parallelStream().forEach(user -> {
            user.getActivities().forEach(activity -> {
                trackPointCollection.insertMany(activity.getTrackPoints().stream().map(t -> t.toDocument(activity.getId())).toList());
                activityCollection.insertOne(activity.toDocument(user.getId()));

                //activityCollection.insertOne(activity.toDnormDocument(user.getId()));
            });
            userCollection.insertOne(user.toDocument());
        });
    }

    public static void task1(MongoDatabase db) throws SQLException {
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);
        MongoCollection<Document> userCollection = db.getCollection(User.collection);
        MongoCollection<Document> trackPointCollection = db.getCollection(TrackPoint.collection);

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
        MongoCollection<Document> userCollection = db.getCollection(User.collection);

        /*

        MongoDB query in JSON format:

        {
            $project: {
                num_activities: {$size: "$activities"}
            }
        },
        {
            $group: {
                _id: null,
                avg: {
                    $avg: "$num_activities"
                },
                min: {
                    $min: "$num_activities"
                },
                max: {
                    $max: "$num_activities"
                }
            }
        }

        */

        List<Document> agr = Arrays.asList(new Document("$project",
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

        AggregateIterable<Document> aggregate = userCollection.aggregate(agr);
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
        MongoCollection<Document> userCollection = db.getCollection(User.collection);

        /*

        MongoDB query in JSON format:

        {
            '$project': {
                'num_activities': {
                    '$size': '$activities'
                }
            }
        }, {
            '$sort': {
                'num_activities': -1
            }
        }, {
            '$limit': 10
        }

        */

        List<Document> agr = Arrays.asList(new Document("$project",
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
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);

        /*

        MongoDB query in JSON format:

        {
            $addFields: {
                days_over: {
                    $dateDiff:{
                        startDate: "$startDateTime",
                        endDate: "$endDateTime",
                        unit: "day"
                    }
                }

            }}, 
            {
                $match: {
                    days_over: {$eq: 1}
            }}, 
            {
                $group: {
            _   id: "$user_id",
            }}, 
            {
                $count: 'count'
            }
        }

        */

        List<Document> agr = Arrays.asList(new Document("$addFields",
                                                        new Document("days_over",
                                                                     new Document("$dateDiff",
                                                                                  new Document("startDate", "$startDateTime")
                                                                                          .append("endDate", "$endDateTime")
                                                                                          .append("unit", "day")))),
                                           new Document("$match",
                                                        new Document("days_over",
                                                                     new Document("$eq", 1L))),
                                           new Document("$group",
                                                        new Document("_id", "$user_id")),
                                           new Document("$count", "count"));

        Iterator<Document> documents = activityCollection.aggregate(agr).iterator();
        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 4");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("activity's passing midnight",document -> document.getInteger("count"))
        );
        simpleTable.display();
    }

    public static void task5(MongoDatabase db) {
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);

        /*

        MongoDB query in JSON format:

        {
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
        }

        */

        List<Document> agr = Arrays.asList(new Document("$group",
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
        
        The $geoNear operator could have been used, but the implementation is limited and would have required a
        restructuring of the storage format.

        MongoDB query in JSON format:

        {
            $addFields: {
                "seconds_dif": {
                    $abs:{
                        $dateDiff:{
                            startDate: ISODate("2008-08-24T15:38:00"),
                            endDate: "$dateTime",
                            unit: "second"
                        }
                    }
                }
            }
        }, 
            {
                $match: {
                "seconds_dif": {$lte: 60}
            }
        }

        //Second query
        {
            $match: {
                _id: {$in: toCloseIds}
            }
        },
        {
            $group: {
                _id: "$activity_obj.user_id",
            }
        },
        {
            $sort: {
                _id: 1,
            }
        }

        */

        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);
        MongoCollection<Document> trackPointCollection = db.getCollection(TrackPoint.collection);

        List<Document> agr = Arrays.asList(new Document("$addFields",
                                            new Document("seconds_dif",
                                                         new Document("$abs",
                                                                      new Document("$dateDiff",
                                                                                   new Document("startDate", Date.from(
                                                                                           Instant.parse("2008-08-24T15:38:00Z")))
                                                                                           .append("endDate", "$dateTime")
                                                                                           .append("unit", "second"))))),
                               new Document("$match",
                                            new Document("seconds_dif",
                                                         new Document("$lte", 60L))));


        Iterator<Document> itr = trackPointCollection.aggregate(agr).iterator();
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
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);
        MongoCollection<Document> userCollection = db.getCollection(User.collection);


        /*
          MongoDB query in JSON format:
        {
            '$match': {
                'transportationMode': {
                    '$in': [
                        'taxi'
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$user_id',
                'count': {
                    '$count': {}
                }
            }
        }

         */

        List<Document> agr = Arrays.asList(new Document("$match",
                        new Document("transportationMode",
                                new Document("$in", Arrays.asList("taxi")))),
                new Document("$group",
                        new Document("_id", "$user_id")
                                .append("count",
                                        new Document("$count",
                                                new Document()))));


        Iterator<Document> taxi_activities = activityCollection.aggregate(agr).iterator();
        List<Integer> user_ids = new ArrayList<>();

        while(taxi_activities.hasNext())
        {
            user_ids.add(taxi_activities.next().getInteger("_id"));
        }

        /*

        MongoDB query in JSON format:
        {
            '$match': {
                '_id': {
                    '$nin': user_ids
                }
            }
        }
        */

        var user_query = Arrays.asList(new Document("$match",
                new Document("_id",
                        new Document("$nin", user_ids))));


        var taxi_haters = userCollection.aggregate(user_query).iterator();


        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 7");
        simpleTable.setItems(taxi_haters);
        simpleTable.setCols(
                new Column<Document>("non taxi users",document -> document.getInteger("_id"))
        );
        simpleTable.display();

    }

    public static void task8(MongoDatabase db) {
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);


        /*
        MongoDB query in JSON format:
        {
            $match: {
                transportationMode: {
                    $ne: null
                }
            }
        }, {
            $group: {
                _id: {
                    transportationMode: "$transportationMode",
                    user_id: "$user_id"
                },
                cnt: {
                    $count: {}
                }
            }
        }, {
            $group: {
                _id: "$_id.transportationMode",
                cnt: {
                    $count: {}
                }
            }
        }
         */
//        List<Document> agr = Arrays.asList(
//                                new Document("$match",
//                                            new Document("transportationMode",
//                                                    new Document("$ne", new BsonNull()))),
//                                new Document("$group",
//                                             new Document("_id",
//                                                          new Document("transportationMode", "$transportationMode")
//                                                                  .append("user_id", "$user_id"))
//                                                     .append("cnt",
//                                                             new Document("$count",
//                                                                          new Document()))),
//                                new Document("$group",
//                                             new Document("_id", "$_id.transportationMode")
//                                                     .append("cnt",
//                                                             new Document("$count",
//                                                                          new Document()))));
        List<Document> agr = Arrays.asList(new Document("$group",
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

   private static void task9a(MongoDatabase db) {
       MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);

        /*

        MongoDB query in JSON format:

        {
            '$group': {
                '_id': {
                    'year': {
                        '$year': '$startDateTime'
                    }, 
                    'month': {
                        '$month': '$startDateTime'
                    }
                }, 
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }, {
            '$limit': 1
        }

        */

       List<Document> agr = Arrays.asList( new Document("$group", 
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
            new Column<Document>("Number of Activities", document -> document.getLong("count"))
        );
       simpleTable.display();
   }

   private static void task9b(MongoDatabase db) {
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);

        /*

        MongoDB query in JSON format:
        [
        {
            '$group': {
                '_id': {
                    'year': {
                        '$year': '$startDateTime'
                    }, 
                    'month': {
                        '$month': '$startDateTime'
                    }
                }, 
                'count': {
                    '$sum': 1
                }, 
                'user_ids': {
                    '$push': {
                        'user_id': '$user_id', 
                        'hours': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$endDateTime', '$startDateTime'
                                    ]
                                }, 60 * 1000 * 60
                            ]
                        }
                    }
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }, {
            '$limit': 1
        }, {
            '$unwind': {
                'path': '$user_ids'
            }
        }, {
            '$group': {
                '_id': {
                    'user_id': '$user_ids.user_id'
                }, 
                'total_hours': {
                    '$sum': '$user_ids.hours'
                }, 
                'num_activities': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'num_activities': -1
            }
        }, {
            '$limit': 3
        }
        ]

        */

        List<Document> agr = Arrays.asList(new Document("$group",
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
                    "The user with the second most activities, user: %s, spent more time on their activities than the user with the most activities\n",
                    tableData.get(1).getEmbedded(List.of("_id", "user_id"), Integer.class)
            );
        } else {
            System.out.println("The top and next top users spent the same amount of time on their activities");
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


    public static void task10(MongoDatabase db)
    {
        MongoCollection<Document> activityCollection = db.getCollection(Activity.collection);
        MongoCollection<Document> trackPointCollection = db.getCollection(TrackPoint.collection);

        /*

        MongoDB query in JSON format:

        {
            $match: {
                user_id: 113,
                transportationMode: "walk",
                startDateTime: {$gte: ISODate('2008-01-01')},
                endDateTime: {$lte: ISODate('2009-01-01')},
            }},
        {
            $group: {
                _id: 1,
                ac_ids: {
                    $push: {id: "$_id"}
                }
        }}

        */

        List<Document> agr = Arrays.asList(new Document("$match",
                        new Document("user_id", 113L)
                                .append("transportationMode", "walk")
                                .append("startDateTime",
                                        new Document("$gte",
                                                new java.util.Date(1199145600000L)))
                                .append("endDateTime",
                                        new Document("$lte",
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

        /*

        MongoDB query in JSON format:
        {
            $match: {
                activity_id: {$in: [1]}
                }
        },
        {
            $project: {
              dateDays: 1,
              activity_id: 1,
              latitude: 1,
              longitude: 1
            }
        },
        {
            $sort: {
                dateDays: 1
            }
        }
         */

        List<Document> agr_t = Arrays.asList(new Document("$match",
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
        System.out.println("################# Task 10 ################");
        System.out.println("Total distance walked by user 112 in 2008:");
        System.out.printf("%.5f km\n", tot_dist);
    }


    public static void task11(MongoDatabase db) {
        MongoCollection<Document> trackPointCollection = db.getCollection(TrackPoint.collection);

        /*

        MongoDB query in JSON format:

        {
            $setWindowFields: {
                partitionBy: "$activity_id",
                sortBy: {
                    "_id": 1
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
        },
        {
            $addFields: {
                usr_gain_m: {
                    $multiply: ["$usr_gain", 0.3048]
                }
            }
        },
        {
            $limit: 20
        }
        */

        List<Document> agr = Arrays.asList(new Document("$setWindowFields",
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
                                                          new Document("$multiply", Arrays.asList("$usr_gain", 0.3048d)))),
                                new Document("$limit", 20L));

        Iterator<Document> documents = trackPointCollection.aggregate(agr).allowDiskUse(true).iterator();
        SimpleTable<Document> simpleTable = new SimpleTable<>();
        simpleTable.setTitle("Task 11");
        simpleTable.setItems(documents);
        simpleTable.setCols(
                new Column<Document>("user",document -> document.getInteger("_id")),
                new Column<Document>("altitude gain",document -> document.getDouble("usr_gain_m"))
        );
        simpleTable.display();
    }

    public static void task12(MongoDatabase db) {
        MongoCollection<Document> trackPointCollection = db.getCollection(TrackPoint.collection);

        /*

        MongoDB query in JSON format:
        
        {
            $setWindowFields: {
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
            }}, 
            {
                $match: {
                    time_shifted: {$ne: null}
            }}, 
            {   $addFields: {
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
            }}}, 
            {
                $match: {
                    min_dif: {$gte: 5}
            }}, 
            {
                $group: {
                    _id: "$activity_id",
                    num_invalid: {
                    $count: {}
                } 
            }}, 
            {
                $lookup: {
                    from: 'activity',
                    localField: '_id',
                    foreignField: '_id',
                    as: 'act_data'
            }}, 
            {
                $unwind: {
                    path: "$act_data",
            }}, 
            {
                $group: {
                    _id: "$act_data.user_id",
                    num_invalid: {
                    $count: {}
            }
            }}, 
            {
                $sort: {
                    num_invalid: -1
            }
        }

        */

        List<Document> agr = Arrays.asList(new Document("$setWindowFields",
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
                new Column<Document>("user", document -> document.getInteger("_id")),
                new Column<Document>("num invalid", document -> document.getInteger("num_invalid"))

        );
        simpleTable.display();
    }
}
