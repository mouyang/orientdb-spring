package it.megadix.orientdb.spring;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class OrientDbTemplateTest extends OrientDbDaoSupport {

    static final String DB_PATH = "databases/OrientDbDaoSupportTest";

    @Configuration
    static class ContextConfiguration {

        ODatabaseRecord database;

        @Bean
        ODatabaseRecord database() throws Exception {
            if (database == null) {
                // delete database from previous runs (if any)
                File dbDir = new File(DB_PATH);
                if (dbDir.exists()) {
                    FileUtils.cleanDirectory(dbDir);
                }
                database = new ODatabaseDocumentTx("local:" + DB_PATH);
                if (!database.exists()) {
                    database.create();
                }
                database.getMetadata().getSchema().createClass("TransactionalTest_1");
            }
            return database;
        }
    }

    @Test
    public void test_execute() {
        Assert.assertTrue(getOrientDbTemplate().execute(new OrientDbCallback<Boolean>() {
            public Boolean doInOrientDb(ODatabaseRecord database) {
                return null != database;
            }
        }));
    }

}
