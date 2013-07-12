package it.megadix.orientdb.spring;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Service
public class TransactionalTestService extends OrientDbDaoSupport {

    private static final String THREAD_A = "Thread A";
    private static final String THREAD_B = "Thread B";
    PlatformTransactionManager transactionManager;
    TransactionTemplate template;

    @Autowired
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.template = new TransactionTemplate(transactionManager);
    }

    public void test_A(final CyclicBarrier barrier, TransactionalTest test_1) {

        TransactionCallback<Object> tcb = new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus status) {
                try {
                    assertTrue(!checkForName(THREAD_A) && !checkForName(THREAD_B));
                    // insert one document
                    assertNotNull(insertDocument(THREAD_A));
                    assertTrue(1 == countDocuments() && checkForName(THREAD_A));
                    // wait at barrier while thread B inserts another document
                    barrier.await();
                    // count must be == 1, because transactions must be isolated
                    assertTrue(1 == countDocuments() && checkForName(THREAD_A));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return null;
            }
        };

        template.execute(tcb);
    }

    public void test_B(final CyclicBarrier barrier, TransactionalTest test_1) {

        TransactionCallback<Object> tcb = new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus status) {
                try {
                    // wait at barrier while thread A inserts a document
                    assertTrue(!checkForName(THREAD_A) && !checkForName(THREAD_B));
                    barrier.await();
                    // count must be == 1, because transactions must be isolated
                    assertTrue(1 == countDocuments() && checkForName(THREAD_A));
                    // insert one document
                    assertNotNull(insertDocument(THREAD_B));
                    // count must be == 2, because transactions must be isolated
                    assertTrue(2 == countDocuments() && checkForName(THREAD_A) && checkForName(THREAD_B));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return null;
            }
        };

        template.execute(tcb);
    }

    private long countDocuments() {
        OrientDbCallback<Long> action = new OrientDbCallback<Long>() {
            @Override
            public Long doInOrientDb(ODatabaseRecord database) {
                OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
                        "select count(*) from TransactionalTest_1");
                List<ODocument> result = database.command(query).execute();
                Number count = result.get(0).field("count");
                return count.longValue();
            }
        };

        long count = getOrientDbTemplate().execute(action);

        return count;
    }

    private Boolean checkForName(final String name) {
        OrientDbCallback<Boolean> action = new OrientDbCallback<Boolean>() {
            @Override
            public Boolean doInOrientDb(ODatabaseRecord database) {
                OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
                        "select from TransactionalTest_1 where name like '" + name + "'");
                Map<String, Object> params = new HashMap<String, Object>();
                params.put("name", name);
                List<ODocument> result = database.command(query).execute();
                return 1 == result.size();
            }
        };

        return getOrientDbTemplate().execute(action);
    }

    private ODocument insertDocument(final String name) {
        OrientDbCallback<ODocument> action = new OrientDbCallback<ODocument>() {
            @Override
            public ODocument doInOrientDb(ODatabaseRecord database) {
                ODocument doc = new ODocument("TransactionalTest_1");
                doc.field("name", name);
                doc.save();
                return doc;
            }
        };

        ODocument doc = getOrientDbTemplate().execute(action);

        return doc;
    }
}