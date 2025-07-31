/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class RecoveryZkTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  private final List<StoppableIndexingThread> threads = new ArrayList<>();

  @After
  public void stopThreads() throws InterruptedException {
    for (StoppableIndexingThread t : threads) {
      t.safeStop();
    }
    for (StoppableIndexingThread t : threads) {
      t.join();
    }
    threads.clear();
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  public void test() throws Exception {

    final String collection = "recoverytest";

    CollectionAdminRequest.createCollection(collection, "conf", 1, 2)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());
    waitForState("Expected a collection with one shard and two replicas", collection, clusterShape(1, 2));
    cluster.getSolrClient().setDefaultCollection(collection);

    // start a couple indexing threads

    int[] maxDocList = new int[] {300, 700, 1200, 1350, 3000};
    int[] maxDocNightlyList = new int[] {3000, 7000, 12000, 30000, 45000, 60000};

    int maxDoc;
    if (!TEST_NIGHTLY) {
      maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];
    } else {
      maxDoc = maxDocNightlyList[random().nextInt(maxDocList.length - 1)];
    }
    log.info("Indexing {} documents", maxDoc);

    final StoppableIndexingThread indexThread
      = new StoppableIndexingThread(null, cluster.getSolrClient(), "1", true, maxDoc, 1, true);
    threads.add(indexThread);
    indexThread.start();

    final StoppableIndexingThread indexThread2
      = new StoppableIndexingThread(null, cluster.getSolrClient(), "2", true, maxDoc, 1, true);
    threads.add(indexThread2);
    indexThread2.start();

    // give some time to index...
    int[] waitTimes = new int[] {200, 2000, 3000};
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);

    // bring shard replica down
    DocCollection state = getCollectionState(collection);
    Replica leader = state.getLeader("shard1");
    Replica replica = getRandomReplica(state.getSlice("shard1"), (r) -> leader != r);

    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    jetty.stop();
    log.info("Replica on shard is down");

    // wait a moment - lets allow some docs to be indexed so replication time is non 0
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);

    // bring shard replica up
    log.info("Starting replica on shard");
    jetty.start();

    // make sure replication can start
    Thread.sleep(4000);

    // stop indexing threads
    indexThread.safeStop();
    indexThread2.safeStop();

    indexThread.join();
    indexThread2.join();

    new UpdateRequest()
        .commit(cluster.getSolrClient(), collection);

    cluster.getSolrClient().waitForState(collection, 120, TimeUnit.SECONDS, clusterShape(1, 2));

    // test that leader and replica have same doc count
    log.info("Checking shard consistency after recovery");
    state = getCollectionState(collection);
    log.info("Shard state after recovery: {}", state);
    assertShardConsistency(state.getSlice("shard1"), true);
    log.info("Shard replica recovery test passed");
    //printClasspath();

  }

  public void printClasspath() {
    // 使用系统临时目录
    String tempDir = System.getProperty("java.io.tmpdir");
    File outputFile = new File(tempDir, "class_path_output.txt");

    try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
      writer.println("Classpath: ");
      String classpathStr = System.getProperty("java.class.path");
      String[] classpathEntries = classpathStr.split(System.getProperty("path.separator"));

      for (String entry : classpathEntries) {
        writer.println(entry);
        log.info(entry);
      }
      log.info("Classpath written to: {}", outputFile.getAbsolutePath());

    } catch (IOException e) {
      System.err.println("error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private void assertShardConsistency(Slice shard, boolean expectDocs) throws Exception {
    List<Replica> replicas = shard.getReplicas(r -> r.getState() == Replica.State.ACTIVE);
    long[] numCounts = new long[replicas.size()];
    int i = 0;
    for (Replica replica : replicas) {
      try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl())
          .withHttpClient(cluster.getSolrClient().getHttpClient()).build()) {
        log.info("Checking replica: {}", replica.getName());
        numCounts[i] = client.query(new SolrQuery("*:*").add("distrib", "false")).getResults().getNumFound();
        log.info("Replica {} has {} documents", replica.getName(), numCounts[i]);
        i++;
      }
    }
    for (int j = 1; j < replicas.size(); j++) {
      if (numCounts[j] != numCounts[j - 1])
        fail("Mismatch in counts between replicas");  // TODO improve this!
      if (numCounts[j] == 0 && expectDocs)
        fail("Expected docs on shard " + shard.getName() + " but found none");
    }
  }

}
