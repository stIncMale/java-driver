/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Gauge;
import com.google.common.util.concurrent.Uninterruptibles;
import org.assertj.core.api.Condition;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Mockito.*;
import static org.scassandra.http.client.ConnectionsClient.CloseType.CLOSE;
import static org.testng.Assert.fail;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;

import static com.datastax.driver.core.Assertions.assertThat;

public class HostConnectionPoolTest extends ScassandraTestBase.PerClassCluster {


    @BeforeMethod(groups={"short", "long"})
    public void reinitializeCluster() {
        cluster.close();
        try {
            cluster = createClusterBuilder().build();
            session = cluster.connect();
        } catch(Exception e) {
            cluster.close();
            fail("Error recreating cluster", e);
        }
    }

    /**
     * Ensures that if a fixed-sized pool has filled its core connections that borrowConnection will timeout instead
     * of creating a new connection.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void fixed_size_pool_should_fill_its_core_connections_and_then_timeout() throws ConnectionException, TimeoutException {
        HostConnectionPool pool = createPool(2, 2);

        assertThat(pool.connections.size()).isEqualTo(2);
        List<Connection> coreConnections = newArrayList(pool.connections);

        for (int i = 0; i < 256; i++) {
            Connection connection = pool.borrowConnection(100, MILLISECONDS);
            assertThat(coreConnections).contains(connection);
        }

        boolean timedOut = false;
        try {
            pool.borrowConnection(100, MILLISECONDS);
        } catch (TimeoutException e) {
            timedOut = true;
        }
        assertThat(timedOut).isTrue();
    }

    /**
     * Ensures that if a variable-sized pool has filled up to its maximum connections that borrowConnection will
     * timeout instead of creating a new connection.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void variable_size_pool_should_fill_its_connections_and_then_timeout() throws ConnectionException, TimeoutException {
        HostConnectionPool pool = createPool(1, 2);

        assertThat(pool.connections.size()).isEqualTo(1);
        List<Connection> coreConnections = newArrayList(pool.connections);

        //
        for (int i = 0; i < 128; i++) {
            Connection connection = pool.borrowConnection(100, MILLISECONDS);
            assertThat(coreConnections).contains(connection);
        }

        // Borrow more and ensure the connection returned is a non-core connection.
        for (int i = 0; i < 128; i++) {
            Connection connection = pool.borrowConnection(100, MILLISECONDS);
            assertThat(coreConnections).doesNotContain(connection);
        }

        boolean timedOut = false;
        try {
            pool.borrowConnection(100, MILLISECONDS);
        } catch (TimeoutException e) {
            timedOut = true;
        }
        assertThat(timedOut).isTrue();
    }

    /**
     * Ensures that if the core connection pool is full that borrowConnection will create and use a new connection.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_add_extra_connection_when_core_full() throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20);

        HostConnectionPool pool = createPool(1, 2);
        Connection core = pool.connections.get(0);

        // Fill core connection
        for (int i = 0; i < 128; i++)
            assertThat(
                pool.borrowConnection(100, MILLISECONDS)
            ).isEqualTo(core);

        // Reaching 128 on the core connection should have triggered the creation of an extra one
        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
    }

    /**
     * Ensures that a trashed connection that has not been timed out should be resurrected into the connection pool if
     * borrowConnection is called and a new connection is needed.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "long")
    public void should_resurrect_trashed_connection_within_idle_timeout() throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20);

        HostConnectionPool pool = createPool(1, 2);
        Connection connection1 = pool.connections.get(0);

        for (int i = 0; i < 101; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(connection1);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        Connection connection2 = pool.connections.get(1);

        assertThat(connection1.inFlight.get()).isEqualTo(101);
        assertThat(connection2.inFlight.get()).isEqualTo(0);

        // Go back under the capacity of 1 connection
        for (int i = 0; i < 51; i++)
            pool.returnConnection(connection1);

        assertThat(connection1.inFlight.get()).isEqualTo(50);
        assertThat(connection2.inFlight.get()).isEqualTo(0);

        // Given enough time, one connection gets trashed (and the implementation picks the first one)
        TimeUnit.SECONDS.sleep(20);
        assertThat(pool.connections).containsExactly(connection2);
        assertThat(pool.trash).containsExactly(connection1);

        // Now borrow enough to go just under the 1 connection threshold
        for (int i = 0; i < 50; i++)
            pool.borrowConnection(100, MILLISECONDS);

        assertThat(pool.connections).containsExactly(connection2);
        assertThat(pool.trash).containsExactly(connection1);
        assertThat(connection1.inFlight.get()).isEqualTo(50);
        assertThat(connection2.inFlight.get()).isEqualTo(50);

        // Borrowing one more time should resurrect the trashed connection
        pool.borrowConnection(100, MILLISECONDS);
        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(pool.connections).containsExactly(connection2, connection1);
        assertThat(pool.trash).isEmpty();
        assertThat(connection1.inFlight.get()).isEqualTo(50);
        assertThat(connection2.inFlight.get()).isEqualTo(51);
    }

    /**
     * Ensures that a trashed connection that has been timed out should not be resurrected into the connection pool if
     * borrowConnection is called and a new connection is needed.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "long")
    public void should_not_resurrect_trashed_connection_after_idle_timeout() throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20);

        HostConnectionPool pool = createPool(1, 2);
        Connection connection1 = pool.connections.get(0);

        for (int i = 0; i < 101; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(connection1);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);
        Connection connection2 = pool.connections.get(1);

        assertThat(connection1.inFlight.get()).isEqualTo(101);
        assertThat(connection2.inFlight.get()).isEqualTo(0);

        // Go back under the capacity of 1 connection
        for (int i = 0; i < 51; i++)
            pool.returnConnection(connection1);

        assertThat(connection1.inFlight.get()).isEqualTo(50);
        assertThat(connection2.inFlight.get()).isEqualTo(0);

        // Given enough time, one connection gets trashed (and the implementation picks the first one)
        TimeUnit.SECONDS.sleep(20);
        assertThat(pool.connections).containsExactly(connection2);
        assertThat(pool.trash).containsExactly(connection1);

        // Return trashed connection down to 0 inFlight
        for (int i = 0; i < 50; i++)
            pool.returnConnection(connection1);
        assertThat(connection1.inFlight.get()).isEqualTo(0);

        // Give enough time for trashed connection to be cleaned up from the trash:
        TimeUnit.SECONDS.sleep(30);
        assertThat(pool.connections).containsExactly(connection2);
        assertThat(pool.trash).isEmpty();
        assertThat(connection1.isClosed()).isTrue();

        // Fill the live connection to go over the threshold where a second one is needed
        for (int i = 0; i < 101; i++)
            assertThat(pool.borrowConnection(100, TimeUnit.MILLISECONDS)).isEqualTo(connection2);
        assertThat(connection2.inFlight.get()).isEqualTo(101);
        TimeUnit.MILLISECONDS.sleep(100);

        // Borrow again to get the new connection
        Connection connection3 = pool.borrowConnection(100, MILLISECONDS);
        assertThat(connection3)
            .isNotEqualTo(connection2) // should not be the full connection
            .isNotEqualTo(connection1); // should not be the previously trashed one
    }

    /**
     * Ensures that a trashed connection that has been timed out should not be closed until it has 0 in flight requests.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "long")
    public void should_not_close_trashed_connection_until_no_in_flight()
        throws ConnectionException, TimeoutException, InterruptedException {
        cluster.getConfiguration().getPoolingOptions()
            .setIdleTimeoutSeconds(20);

        HostConnectionPool pool = createPool(1, 2);
        Connection connection1 = pool.connections.get(0);

        // Fill core connection enough to trigger creation of another one
        for (int i = 0; i < 110; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(connection1);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);

        // Return enough times to get back under the threshold where one connection is enough
        for (int i = 0; i < 50; i++)
            pool.returnConnection(connection1);

        // Give enough time for one connection to be trashed. Due to the implementation, this will be the first one.
        // It still has in-flight requests so should not get closed.
        TimeUnit.SECONDS.sleep(30);
        assertThat(pool.trash).containsExactly(connection1);
        assertThat(connection1.inFlight.get()).isEqualTo(60);
        assertThat(connection1.isClosed()).isFalse();

        // Consume all inFlight requests on the trashed connection.
        while (connection1.inFlight.get() > 0) {
            pool.returnConnection(connection1);
        }

        // Sleep enough time for the connection to be consider idled and closed.
        TimeUnit.SECONDS.sleep(30);

        // The connection should be now closed.
        // The trashed connection should be closed and not in the pool or trash.
        assertThat(connection1.isClosed()).isTrue();
        assertThat(pool.connections).doesNotContain(connection1);
        assertThat(pool.trash).doesNotContain(connection1);
    }

    /**
     * Ensures that if a connection that has less than the minimum available stream ids is returned to the pool that
     * the connection is put in the trash.
     *
     * @since 2.0.10, 2.1.6
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     */
    @Test(groups = "short")
    public void should_trash_on_returning_connection_with_insufficient_streams()
        throws ConnectionException, TimeoutException, InterruptedException {
        HostConnectionPool pool = createPool(1, 2);
        Connection core = pool.connections.get(0);

        for (int i = 0; i < 128; i++)
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core);

        TimeUnit.MILLISECONDS.sleep(100);
        assertThat(pool.connections).hasSize(2);

        // Grab the new non-core connection and replace it with a spy.
        Connection extra1 = spy(pool.connections.get(1));
        pool.connections.set(1, extra1);

        // Borrow 10 times to ensure pool is utilized.
        for (int i = 0; i < 10; i++) {
            assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
        }

        // Return connection, it should not be trashed since it still has 9 inflight requests.
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(2);

        // stub the maxAvailableStreams method to return 0, indicating there are no remaining streams.
        // this should cause the connection to be replaced and trashed on returnConnection.
        doReturn(0).when(extra1).maxAvailableStreams();

        // On returning of the connection, should detect that there are no available streams and trash it.
        pool.returnConnection(extra1);
        assertThat(pool.connections).hasSize(1);
    }

    /**
     * Ensures that if a connection on a host is lost but other connections remain intact in the Pool that the
     * host is not marked down.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups="short")
    public void should_keep_host_up_when_connection_lost() {
        HostConnectionPool pool = createPool(2, 2);
        Connection core0 = pool.connections.get(0);
        Connection core1 = pool.connections.get(1);

        // Drop a connection and ensure the host stays up.
        serverClient.disableListener();
        connectionsClient.closeConnection(CLOSE, ((InetSocketAddress)core0.channel.localAddress()));

        // connection 0 should be down, while connection 1 and the Host should remain up.
        assertThat(core0.isClosed()).isTrue();
        assertThat(core1.isClosed()).isFalse();
        assertThat(pool.connections).doesNotContain(core0);
        assertThat(cluster).host(1).hasState(Host.State.UP);
    }


    /**
     * Ensures that if all connections on a host are closed that the host is marked
     * down and the control connection is notified of that fact and re-established
     * itself.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups="short")
    public void should_mark_host_down_when_no_connections_remaining() {
        Cluster cluster = this.createClusterBuilder().build();
        try {
            cluster.init();
            HostConnectionPool pool = createPool(cluster, 8,8);
            // copy list to track these connections.
            List<Connection> connections = newArrayList(pool.connections);

            // Drop all connections.
            serverClient.disableListener();
            connectionsClient.closeConnections(CLOSE, host.getAddress());

            // Ensure all connections are closed.
            assertThat(connections).are(new Condition<Connection>() {

                @Override
                public boolean matches(Connection connection) {
                    return connection.isClosed();
                }
            });
            // The host should be marked down
            assertThat(cluster).host(1).hasState(Host.State.DOWN);

            // TODO validate control connection closed and eventually reconnects
            // when it can connect.
        } finally {
            cluster.close();
        }
    }


    /**
     * Ensures that if a connection on a host is lost that brings the number of active connections in a pool
     * under core connection count that up to core connections are re-established, but only after the
     * next reconnect schedule has elapsed.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups="short")
    public void should_create_new_connections_when_connection_lost_and_under_core_connections() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
            .withSocketOptions(new SocketOptions()
                .setConnectTimeoutMillis(readTimeout)
                .setReadTimeoutMillis(reconnectInterval))
            .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            HostConnectionPool pool = createPool(cluster, 3, 3);
            Connection core0 = pool.connections.get(0);
            Connection core1 = pool.connections.get(1);
            Connection core2 = pool.connections.get(2);

            // Drop two core connections.
            // Disable new connections initially and we'll eventually reenable it.
            serverClient.disableListener();
            connectionsClient.closeConnection(CLOSE, ((InetSocketAddress)core0.channel.localAddress()));
            connectionsClient.closeConnection(CLOSE, ((InetSocketAddress)core2.channel.localAddress()));

            // Since we have a connection left the host should remain up.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(pool.connections).hasSize(1);

            // The borrowed connection should be the open one.
            Connection borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isEqualTo(core1);

            // Should not have tried to create a new core connection since reconnection time had not elapsed.
            verify(factory, never()).open(any(HostConnectionPool.class));

            // Sleep to elapse the Reconnection Policy.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Attempt to borrow connection, this should trigger ensureCoreConnections thus spawning a new connection.
            borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isEqualTo(core1);

            // Should have tried to open up to core connections as result of borrowing a connection past reconnect time and not being at core.
            verify(factory, timeout(reconnectInterval).times(1)).open(any(HostConnectionPool.class));

            // Sleep as the reconnect timer should have been reset when connections fail.
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);

            // Enable listening so new connections succeed.
            serverClient.enableListener();
            // Sleep to elapse the Reconnection Policy.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Try to borrow a connection, since listening is now enabled the spun up connection task should succeed and the pool should grow.
            pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            verify(factory, timeout(readTimeout).times(2)).open(any(HostConnectionPool.class));

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);
            assertThat(pool.connections).hasSize(3);

            // Borrowed connection should be a newly spawned connection since the other one has some inflight requests.
            borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isNotEqualTo(core0).isNotEqualTo(core1).isNotEqualTo(core2);
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that if a connection on a host is lost and the number of remaining connections is at
     * core connection count that no connections are re-established until after there are enough
     * inflight requests to justify creating one and the reconnection interval has elapsed.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_not_schedule_reconnect_when_connection_lost_and_at_core_connections() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
            .withSocketOptions(new SocketOptions()
                .setConnectTimeoutMillis(readTimeout)
                .setReadTimeoutMillis(reconnectInterval))
            .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection core0 = pool.connections.get(0);

            // Create enough inFlight requests to spawn another connection.
            for (int i = 0; i < 101; i++)
                assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(core0);

            TimeUnit.MILLISECONDS.sleep(100);
            assertThat(pool.connections).hasSize(2);

            // Reset factory mock as we'll be checking for new open() invokes later.
            reset(factory);

            // Grab the new non-core connection.
            Connection extra1 = pool.connections.get(1);

            // Drop a connection and disable listening.
            connectionsClient.closeConnection(CLOSE, ((InetSocketAddress)core0.channel.localAddress()));
            serverClient.disableListener();
            // Lets return connection all inFlight connections for that closed host since the requests
            // would fail when connection is closed and thus decrement.
            while (core0.inFlight.get() > 0)
                pool.returnConnection(core0);

            assertThat(cluster).host(1).hasState(Host.State.UP);

            // The borrowed connection should be the open one that should not be core.
            Connection borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isEqualTo(extra1);
            assertThat(pool.connections).hasSize(1);


            // Create enough inFlight requests to fill connection.
            while(extra1.inFlight.get() < 100) {
                assertThat(pool.borrowConnection(100, MILLISECONDS)).isEqualTo(extra1);
            }

            // A new connection should never have been spawned since we didn't max out core.
            verify(factory, after(readTimeout).never()).open(any(HostConnectionPool.class));

            // Borrow another connection, since we exceed max another connection should be opened.
            borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isEqualTo(extra1);

            // After some time the a connection should attempt to be opened (but will fail).
            verify(factory, timeout(readTimeout)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(1);

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout * 2, TimeUnit.MILLISECONDS);
            // Reconnecting failed since listening was enabled.
            assertThat(pool.connections).hasSize(1);

            // Re enable listening then wait for reconnect.
            serverClient.enableListener();
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Borrow another connection, since we exceed max another connection should be opened.
            borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isEqualTo(extra1);

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);
            // Reconnecting should have exceeded and pool will have grown.
            assertThat(pool.connections).hasSize(2);

            // Borrowed connection should be the newly spawned connection since the other one has some inflight requests.
            borrowed = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
            assertThat(borrowed).isNotEqualTo(core0).isNotEqualTo(extra1);
        } finally {
            cluster.close();
        }
    }

    private HostConnectionPool createPool(int coreConnections, int maxConnections) {
        return createPool(cluster, coreConnections, maxConnections);
    }

    private HostConnectionPool createPool(Cluster cluster, int coreConnections, int maxConnections) {
        cluster.getConfiguration().getPoolingOptions()
            .setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections)
            .setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnections);
        Session session = cluster.connect();
        Host host = TestUtils.findHost(cluster, 1);

        // Replace the existing pool with a spy pool and return it.
        SessionManager sm = ((SessionManager)session);
        HostConnectionPool pool = sm.pools.get(host);
        return pool;
    }

    /**
     * <p>
     * This test uses a table named "Java349" with 1000 column and performs asynchronously 100k insertions. While the
     * insertions are being executed, the number of opened connection is monitored.
     * <p/>
     * If at anytime, the number of opened connections is negative, this test will fail.
     *
     * @since 2.0.6, 2.1.1
     * @jira_ticket JAVA-349
     * @test_category connection:connection_pool
     */
    @Test(groups = "long", enabled = false /* this test causes timeouts on Jenkins */)
    public void open_connections_metric_should_always_be_positive() throws InterruptedException {
        // Track progress in a dedicated thread
        int numberOfInserts = 100 * 1000;
        final CountDownLatch pendingInserts = new CountDownLatch(numberOfInserts);
        ExecutorService progressReportExecutor = Executors.newSingleThreadExecutor();
        final Runnable progressReporter = new Runnable() {
            @Override
            public void run() {
                pendingInserts.countDown();
            }
        };

        // Track opened connections in a dedicated thread every one second
        final AtomicBoolean negativeOpenConnectionCountSpotted = new AtomicBoolean(false);
        final Gauge<Integer> openConnections = cluster.getMetrics().getOpenConnections();
        ScheduledExecutorService openConnectionsWatcherExecutor = Executors.newScheduledThreadPool(1);
        final Runnable openConnectionsWatcher = new Runnable() {
            @Override
            public void run() {
                Integer value = openConnections.getValue();
                if (value < 0) {
                    System.err.println("Negative value spotted for openConnection metric: " + value);
                    negativeOpenConnectionCountSpotted.set(true);
                }
            }
        };
        openConnectionsWatcherExecutor.scheduleAtFixedRate(openConnectionsWatcher, 1, 1, SECONDS);

        // Insert 100k lines in a newly created 1k columns table
        PreparedStatement insertStatement = session.prepare(generateJava349InsertStatement());
        for (int key = 0; key < numberOfInserts; key++) {
            ResultSetFuture future = session.executeAsync(insertStatement.bind(key));
            future.addListener(progressReporter, progressReportExecutor);
        }

        // Wait for all inserts to happen and stop connections and progress tracking
        pendingInserts.await();
        openConnectionsWatcherExecutor.shutdownNow();
        progressReportExecutor.shutdownNow();

        if (negativeOpenConnectionCountSpotted.get()) {
            fail("Negative value spotted for open connection count");
        }
    }

    private String generateJava349InsertStatement() {
        StringBuilder sb = new StringBuilder("INSERT INTO Java349 (mykey");
        for (int i = 0; i < 1000; i++) {
            sb.append(", column").append(i);
        }
        sb.append(") VALUES (?");
        for (int i = 0; i < 1000; i++) {
            sb.append(", ").append(i);
        }
        sb.append(");");

        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
            .withQuery(sb.toString())
            .withVariableTypes(PrimitiveType.INT)
            .build();
        primingClient.prime(preparedStatementPrime);
        return sb.toString();
    }
}
