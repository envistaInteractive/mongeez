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
import org.apache.commons.lang3.time.DateFormatUtils;
import org.mongeez.MongoAuth;
import org.mongeez.commands.ChangeSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MongeezDao {
    private DB db;
    private MongoClientURI mongoClientURI;
    private List<ChangeSetAttribute> changeSetAttributes;
    private static final String MONGO_COMMAND_PATH = "MONGO_COMMAND_PATH";

    public MongeezDao(Mongo mongo, String databaseName) {
        db = mongo.getDB(databaseName);
        configure();
    }

    public MongeezDao(Mongo mongo, String databaseName, MongoAuth auth) {
        final List<MongoCredential> credentials = new LinkedList<MongoCredential>();

        if (auth != null) {
            if (auth.getAuthDb() == null || auth.getAuthDb().equals(databaseName)) {
                credentials.add(MongoCredential.createCredential(auth.getUsername(), databaseName, auth.getPassword().toCharArray()));
            } else {
                credentials.add(MongoCredential.createCredential(auth.getUsername(), auth.getAuthDb(), auth.getPassword().toCharArray()));
            }
        }

        final MongoClient client = new MongoClient(mongo.getServerAddressList(),  credentials);
        db = client.getDB(databaseName);
        configure();
    }

    public MongeezDao(MongoClientURI mongoClientURI) {
        final MongoClient mongo = new MongoClient(mongoClientURI);
        db = mongo.getDB(mongoClientURI.getDatabase());
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
        DBObject q = new QueryBuilder().put("type").exists(false).get();
        BasicDBObject o = new BasicDBObject("$set", new BasicDBObject("type", RecordType.changeSetExecution.name()));
        getMongeezCollection().update(q, o, false, true, WriteConcern.SAFE);
    }

    private void loadConfigurationRecord() {
        DBObject q = new QueryBuilder().put("type").is(RecordType.configuration.name()).get();
        DBObject configRecord = getMongeezCollection().findOne(q);
        if (configRecord == null) {
            if (getMongeezCollection().count() > 0L) {
                // We have pre-existing records, so don't assume that they support the latest features
                configRecord =
                        new BasicDBObject()
                                .append("type", RecordType.configuration.name())
                                .append("supportResourcePath", false);
            } else {
                configRecord =
                        new BasicDBObject()
                                .append("type", RecordType.configuration.name())
                                .append("supportResourcePath", true);
            }
            getMongeezCollection().insert(configRecord, WriteConcern.SAFE);
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
        DBCollection collection = getMongeezCollection();
        for (DBObject dbObject : collection.getIndexInfo()) {
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
        return getMongeezCollection().count(query) > 0;
    }

    private DBCollection getMongeezCollection() {
        return db.getCollection("mongeez");
    }

    public void runScript(String code) {
        if (mongoClientURI != null) {
            runScript(mongoClientURI, code);
        } else {
            db.eval(code);
        }
    }

    public void logChangeSet(ChangeSet changeSet) {
        BasicDBObject object = new BasicDBObject();
        object.append("type", RecordType.changeSetExecution.name());
        for (ChangeSetAttribute attribute : changeSetAttributes) {
            object.append(attribute.name(), attribute.getAttributeValue(changeSet));
        }
        object.append("date", DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(System.currentTimeMillis()));
        getMongeezCollection().insert(object, WriteConcern.SAFE);
    }

    protected void runScript(MongoClientURI mongoClientURI, String code) {

        String[] params = new String[4];

        params[0] = System.getProperty(MONGO_COMMAND_PATH, "mongo");
        params[1] = mongoClientURI.getURI();
        params[2] = "--eval";
        params[3] = code;

        try {
            ProcessBuilder builder = new ProcessBuilder(params);
            builder.environment();

            final Process p = builder.start();

            Thread thread = new Thread() {
                public void run() {
                    try {
                        String line;
                        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));

                        while ((line = input.readLine()) != null) {
                            System.out.println(line);
                        }

                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };

            thread.start();
            int result = p.waitFor();
            thread.join();
            if (result != 0) {
                throw new MongoException("Process failed execution with result code: " + result);
            }
        } catch (IOException | InterruptedException e) {
            throw MongoException.fromThrowable(e);
        }

    }

}
