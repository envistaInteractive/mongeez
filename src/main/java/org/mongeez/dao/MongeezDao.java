/*
 * Copyright 2011 SecondMarket Labs, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mongeez.dao;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.mongeez.MongoAuth;
import org.mongeez.commands.ChangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import javax.print.Doc;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class MongeezDao {
    private MongoDatabase db;
    private ConnectionString mongoClientURI;
    private List<ChangeSetAttribute> changeSetAttributes;
    private static final String MONGO_COMMAND_PATH = "MONGO_COMMAND_PATH";
    private static final Logger LOG = LoggerFactory.getLogger(MongeezDao.class);

    public MongeezDao(MongoClient mongo, String databaseName) {
        db = mongo.getDatabase(databaseName);
        configure();
    }

    public MongeezDao(MongoClient mongo, String databaseName, MongoAuth auth) {
        final List<MongoCredential> credentials = new LinkedList<MongoCredential>();

        if (auth != null) {
            if (auth.getAuthDb() == null || auth.getAuthDb().equals(databaseName)) {
                credentials.add(MongoCredential.createCredential(auth.getUsername(),
                        databaseName, auth.getPassword().toCharArray()));
            } else {
                credentials.add(MongoCredential.createCredential(auth.getUsername(),
                        auth.getAuthDb(), auth.getPassword().toCharArray()));
            }
        }
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
        if(!CollectionUtils.isEmpty(credentials)){
            settingsBuilder.credential(credentials.get(0));
        }
        MongoClientSettings settings = settingsBuilder.build();
        final MongoClient client = MongoClients.create(settings);
        db = client.getDatabase(databaseName);
        configure();
    }

    public MongeezDao(ConnectionString mongoClientURI) {
        final MongoClient mongo = MongoClients.create(mongoClientURI);
        db = mongo.getDatabase(mongoClientURI.getDatabase());
        this.mongoClientURI = mongoClientURI;
        configure();
    }

    private void configure() {
        addTypeToUntypedRecords();
        loadConfigurationRecord();
        dropObsoleteChangeSetExecutionIndices();
        ensureChangeSetExecutionIndex();
    }

    private void addTypeToUntypedRecords() {

//        DBObject q = new QueryBuilder().put("type").exists(false).get();
        Bson q = Filters.exists("type",false);
        BasicDBObject o = new BasicDBObject("$set", new BasicDBObject("type", RecordType.changeSetExecution.name()));
//        getMongeezCollection().update(q, o, false, true, WriteConcern.SAFE);
        UpdateOptions options = new UpdateOptions();
        options.upsert(false);
        getMongeezCollection().updateMany(q, o);
    }

    private void loadConfigurationRecord() {

//        DBObject q = new QueryBuilder().put("type").is(RecordType.configuration.name()).get();
        Bson q = Filters.eq("type",RecordType.configuration.name());
        Document configRecord = getMongeezCollection().find(q).first();
        if (configRecord == null) {
            if (getMongeezCollection().countDocuments() > 0L) {
                // We have pre-existing records, so don't assume that they support the latest features
                configRecord =
                        new Document()
                                .append("type", RecordType.configuration.name())
                                .append("supportResourcePath", false);
            } else {
                configRecord =
                        new Document()
                                .append("type", RecordType.configuration.name())
                                .append("supportResourcePath", true);
            }
//            getMongeezCollection().insert(configRecord, WriteConcern.SAFE);
            getMongeezCollection().insertOne(configRecord);
        }
        Object supportResourcePath = configRecord.get("supportResourcePath");

        changeSetAttributes = new ArrayList<ChangeSetAttribute>();
        changeSetAttributes.add(ChangeSetAttribute.file);
        changeSetAttributes.add(ChangeSetAttribute.changeId);
        changeSetAttributes.add(ChangeSetAttribute.author);
        if (Boolean.TRUE.equals(supportResourcePath)) {
            changeSetAttributes.add(ChangeSetAttribute.resourcePath);
        }
    }

    /**
     * Removes indices that were generated by versions before 0.9.3, since they're not supported by MongoDB 2.4+
     */
    private void dropObsoleteChangeSetExecutionIndices() {
        String indexName = "type_changeSetExecution_file_1_changeId_1_author_1_resourcePath_1";
        MongoCollection collection = getMongeezCollection();
        ListIndexesIterable<Document> indexes = collection.listIndexes();
        for (Document dbObject : indexes) {
            if (indexName.equals(dbObject.get("name"))) {
                collection.dropIndex(indexName);
            }
        }
    }

    private void ensureChangeSetExecutionIndex() {
        BasicDBObject keys = new BasicDBObject();
        keys.append("type", 1);
        for (ChangeSetAttribute attribute : changeSetAttributes) {
            keys.append(attribute.name(), 1);
        }
        getMongeezCollection().createIndex(keys);
    }

    public boolean wasExecuted(ChangeSet changeSet) {
        BasicDBObject query = new BasicDBObject();
        query.append("type", RecordType.changeSetExecution.name());
        for (ChangeSetAttribute attribute : changeSetAttributes) {
            query.append(attribute.name(), attribute.getAttributeValue(changeSet));
        }
        return getMongeezCollection().countDocuments(query) > 0;
    }

    private MongoCollection<Document> getMongeezCollection() {
        return db.getCollection("mongeez");
    }

    public void runScript(String code) {
        if (mongoClientURI != null) {
            runScript(mongoClientURI, code);
        } else {
            //Has been deprecated and alternatives are in the DB shell via $where
            //db.eval(code);
           ConnectionString connectionString =
                   new ConnectionString("mongodb://localhost/"+db.getName());

           mongoClientURI = connectionString;
           runScript(mongoClientURI, code);
        }
    }

    public void logChangeSet(ChangeSet changeSet) {
        Document object = new Document();
        object.append("type", RecordType.changeSetExecution.name());
        for (ChangeSetAttribute attribute : changeSetAttributes) {
            object.append(attribute.name(), attribute.getAttributeValue(changeSet));
        }
        object.append("date", DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(System.currentTimeMillis()));
        getMongeezCollection().insertOne(object);
    }

    protected void runScript(ConnectionString mongoClientURI, String code) {

        String[] params = new String[4];

        params[0] = System.getProperty(MONGO_COMMAND_PATH, "mongo");
        params[1] = mongoClientURI.getConnectionString();
        params[2] = "--quiet";
        Path tempFilePath = null;
        boolean error = false;

        try {

            tempFilePath = Files.createTempFile(UUID.randomUUID().toString(), ".js");
            Files.write(tempFilePath, code.getBytes(), StandardOpenOption.WRITE);

            params[3] = tempFilePath.toAbsolutePath().toString();

            ProcessBuilder builder = new ProcessBuilder(params);
            builder.redirectErrorStream(true);

            final Process p = builder.start();

            String line;
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));

            while (((line = input.readLine()) != null) && LOG.isInfoEnabled()) {
                LOG.info(line);
            }

            input.close();

            int result = p.waitFor();
            if (result != 0) {
                error = true;
                throw new MongoException(MessageFormat.format("Process failed execution with result code: {0} Script " +
                        "run parameters: {1}", result, params));
            }
        } catch (IOException | InterruptedException e) {
            throw MongoException.fromThrowable(e);
        } finally {
            try {
                if (!error && tempFilePath != null) {
                    Files.deleteIfExists(tempFilePath);
                }
            } catch (IOException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(MessageFormat.format("Error occured while trying to delete temp file: {0}",
                            tempFilePath), e);
                }
            }
        }
    }

}
