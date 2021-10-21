package no.ntnu.stodist;

import com.mongodb.client.MongoDatabase;
import no.ntnu.stodist.database.MongoConnection;

public class main {

    public static void main(String[] args) {

        MongoConnection db_connection = new MongoConnection();
        System.out.println();
        
        try {

            MongoDatabase db = db_connection.getMongoConnection();
            //Assignment3Tasks.insertData(db);

            Assignment3Tasks.task1(db);
            Assignment3Tasks.task2(db);
            Assignment3Tasks.task3(db);
            Assignment3Tasks.task4(db);
            Assignment3Tasks.task5(db);
            //Assignment3Tasks.task6(db);
            Assignment3Tasks.task7(db);
            Assignment3Tasks.task8(db);
            Assignment3Tasks.task9(db);
            Assignment3Tasks.task10(db);
            Assignment3Tasks.task11(db);
            Assignment3Tasks.task12(db);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
