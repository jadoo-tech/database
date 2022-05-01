package com.mongodbclient;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import java.util.HashMap;

import java.util.Map;

/**
 * Class to connect to MongoDB Cloud services.
 * Establish connections and methods for accessing data.
 */
public class mongodbDAO {

    private MongoClient client;

    /** A hashmap of existing database name and MongoDatabase instances. */
    private static HashMap<String, MongoDatabase> databaseMap;

    private MongoDatabase currDatabase;
    private MongoCollection<Document> currCollection;


    /** Connection uri. TODO: need to convert password into config files */
    private final String uri =
            "mongodb+srv://dbnanotech22:<password>@waqasdb.iieil.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";

    /** constructor for mongodbDAO
     * fill in databaseMap with all the available database in current connection cluster. */
    public mongodbDAO() {
        try {
            client = MongoClients.create(uri);
        } catch (Exception e) {
            System.out.println("Cannot establish connection.");
            System.out.println(e.getMessage());
        }
        databaseMap = new HashMap<String, MongoDatabase>();
        getAllDatabase();
    }

    /** constructor for mongodbDAO with specified database name and collection name
     * fill in databaseMap with all the available database in current connection cluster.
     * */
    public mongodbDAO(String dbName, String collectionName) {
        try {
            client = MongoClients.create(uri);
        } catch (Exception e) {
            System.out.println("Cannot establish connection.");
            System.out.println(e.getMessage());
        }
        databaseMap = new HashMap<String, MongoDatabase>();
        getAllDatabase();
        currDatabase = client.getDatabase(dbName);
        currCollection = currDatabase.getCollection(collectionName);
    }

    /** set Database hashmap for the current client connection.
     * Add DATABASENAME and its corresponding database to database hashmap.
     * NOTE: A private method to prevent external modification of database directory available.
     */
    private void getAllDatabase() {
        MongoIterable<String> databaseNames = client.listDatabaseNames();
        for (String dbname : databaseNames) {
            MongoDatabase db = client.getDatabase(dbname);
            databaseMap.put(dbname, db);
        }
    }

    /** get current database name */
    public MongoDatabase getCurrDatabase() {
        System.out.println(currDatabase.getName());
        return currDatabase;
    }

    /** get current collection name */
    public MongoCollection getCurrCollection() {
        System.out.println(currCollection.toString());
        return currCollection;
    }

    /** set current database with DATABASENAME in current client connection.
     * return the database of interest.
     */
    public MongoDatabase setCurrDatabase(String databaseName) {
        currDatabase = databaseMap.get(databaseName);
        return currDatabase;
    }

    /** set current collection with COLLECTIONNAME in current client connection.
     * return the collection of interest.
     * NOTE: if COLLECTIONNAME does not exist in the current database, new COLLECTIONNAME collection will
     * be created when the first data is stored in that collection.
     */
    public MongoCollection setCurrCollection(String collectionName) {
        currCollection = currDatabase.getCollection(collectionName);
        return currCollection;
    }

    /** obtain and print all the data in the current collection. */
    public void readDocument() {
        FindIterable<Document> documents = currCollection.find();
        for (Document document : documents) {
            System.out.println(document.toString());
        }
    }

    /** list all database names in the current client connection. */
    public void listAllDatabases() {
        MongoIterable<String> list = client.listDatabaseNames();
        for (String name : list) {
            System.out.println(name);
        }
    }

    /** list all collection names in the current database and client connection. */
    public void listAllCollections() {
        MongoIterable<String> list = currDatabase.listCollectionNames();
        for (String name : list) {
            System.out.println(name);
        }
    }

    /** list all collection names in the specified database and client connection. */
    public void listAllCollections(String databaseName) {
        if (!databaseMap.containsKey(databaseName)) {
            System.out.println(databaseName + " does not exist.");
            return;
        }
        MongoIterable<String> list = databaseMap.get(databaseName).listCollectionNames();
        for (String name : list) {
            System.out.println(name);
        }
    }

    /** method to insert new document data to current collection */
    public void insertDocument(HashMap<String, String> document) {
        Document newDoc = new Document();
        for (Map.Entry<String, String> newData : document.entrySet()) {
            newDoc.append(newData.getKey(), newData.getValue());
        }
        currCollection.insertOne(newDoc);
    }

    /** method to insert new document data to specified EXISTING database and collection. */
    public void insertDocument(String databaseName, String collectionName, HashMap<String, String> document) {
        if (!databaseMap.containsKey(databaseName)) {
            System.out.println(databaseName + " does not exist in cluster!");
            return;
        }
        MongoDatabase db = databaseMap.get(databaseName);
        MongoCollection collection = db.getCollection(collectionName);
        Document newDoc = new Document();
        for (Map.Entry<String, String> newData : document.entrySet()) {
            newDoc.append(newData.getKey(), newData.getValue());
        }
        collection.insertOne(newDoc);
    }

    /** convert Json file to json object and update that document.
     *
     */


    /** Convert json/csv object to document and upload to db. */


    /** create a new database wi DATABASENAME.
     *  set the current database to the new Database.
     *  NOTE: if DATABASENAME does not exist in the cluster, it will automatically create a new one with getDatabase()
     *  when new data is first stored in there.
     */
    public MongoDatabase createNewDatabase(String databaseName) {
        MongoDatabase newDB = client.getDatabase(databaseName);
        databaseMap.put(databaseName, newDB);
        setCurrDatabase(databaseName);
        System.out.println("Database " + databaseName + " created successfully.");
        return newDB;
    }

    /** create new collection in specified database */
    public MongoCollection createNewCollection(String databaseName, String collectionName) {
        MongoDatabase targetDB = databaseMap.get(databaseName);
        targetDB.createCollection(collectionName);
        setCurrCollection(collectionName);
        System.out.println("Collection created successfully.");
        return targetDB.getCollection(collectionName);
    }

    /** Close the client connection. */
    public void close() {
        client.close();
    }

    public static void main(String[] args) {
        mongodbDAO dao = new mongodbDAO("sample_analytics", "accounts");

        // test code functionality
        HashMap<String, String> data = new HashMap<>();
        data.put("bestSchool", "Berkeley");
        // method 1
        dao.createNewDatabase("new_database");
        dao.createNewCollection("new_database", "new_collection");
        dao.insertDocument(data);
        // method 2
        dao.insertDocument("new_testDB", "test", data);
        dao.readDocument();
        dao.close();
    }
}
