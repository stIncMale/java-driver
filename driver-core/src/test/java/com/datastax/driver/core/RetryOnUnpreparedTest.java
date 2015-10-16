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

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RetryOnUnpreparedTest {

    /**
     * Validates that if the response to a query is repeatedly 'UNPREPARED' that the
     * driver only tries re-preparing the the prepared statement associated with that
     * query once.
     *
     * This scenario could possibly happen if Cassandra nodes do not generate the same
     * prepared id consistently across nodes.  The test starts 2 nodes with different
     * encodings causing cassandra to generate different prepared statement ids when
     * the query text contains abnormal characters such as
     * {@link java.lang.Character#MAX_VALUE}.
     *
     * @jira_ticket JAVA-955
     * @test_category prepared_statements
     */
    @Test(groups = "long")
    public void should_not_enter_prepare_loop_on_unprepared() throws Exception {
        CCMBridge ccm = CCMBridge.builder("retry-on-unprepared").notStarted().withNodes(2).build();

        try {
            // Ensure these charsets are supported on this OS and fail if they aren't.
            assertThat(Charset.isSupported("UTF-8")).isTrue();
            assertThat(Charset.isSupported("ISO-8859-1")).isTrue();

            // Start node 1 with UTF-8, node 2 with ISO-8859-1.
            ccm.start(1, "-Dfile.encoding=UTF-8");
            ccm.start(2, "-Dfile.encoding=ISO-8859-1");

            Cluster cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withQueryOptions(new QueryOptions().setReprepareOnUp(false))
                .build();

            try {
                Session session = cluster.connect();
                session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, "simple", 2));
                session.execute(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, "simple.tbl1"));

                // Stop node 2 so we know the statement is prepared on node1.
                ccm.stop(2);

                PreparedStatement prepared = session.prepare("select * from simple.tbl1 where k='" + Character.MAX_VALUE + "'");
                ResultSet result = session.execute(prepared.bind());
                assertThat(result.getAvailableWithoutFetching()).isEqualTo(0);
                assertThat(result.getExecutionInfo().getTriedHosts()).containsOnly(TestUtils.findHost(cluster, 1));

                // Start node 2 and stop node 1 so we know the statement is executed on node2.
                ccm.start(2, "-Dfile.encoding=ISO-8859-1");
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "2", cluster);
                ccm.stop(1);

                try {
                    result = session.executeAsync(prepared.bind()).get(10, TimeUnit.SECONDS);
                    assertThat(result.getAvailableWithoutFetching()).isEqualTo(0);
                    assertThat(result.getExecutionInfo().getTriedHosts()).containsOnly(TestUtils.findHost(cluster, 2));
                } catch (TimeoutException e) {
                    fail("Timed out when executing query, possible prepare loop?", e);
                }
            } finally {
                cluster.close();
            }
        } finally {
            ccm.remove();
        }
    }
}
