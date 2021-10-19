package no.ntnu.stodist;

import com.mongodb.DB;
import com.mongodb.client.MongoDatabase;
import no.ntnu.stodist.database.MongoConnection;

import java.sql.Connection;

public class main {

    public static void main(String[] args) {
        MongoConnection db_connection = new MongoConnection();
        System.out.println();
        try {

            MongoDatabase db = db_connection.getMongoConnection();
            //Assignment2Tasks.insertData(db);
           // Assignment2Tasks.task1(db);
//            Assignment2Tasks.task2(db);
//            Assignment2Tasks.task3(db);
//            Assignment2Tasks.task4(db);
//            Assignment2Tasks.task5(db);
            //Assignment2Tasks.task6(db);
            //Assignment2Tasks.task7(db);
            Assignment2Tasks.task8(db);
            //            Assignment2Tasks.task9(connection);
            //            Assignment2Tasks.task10(connection);
            //            Assignment2Tasks.task11(connection);
            //            Assignment2Tasks.task12(connection);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
