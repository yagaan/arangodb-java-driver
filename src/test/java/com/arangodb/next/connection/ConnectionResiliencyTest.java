/*
 * DISCLAIMER
 *
 * Copyright 2016 ArangoDB GmbH, Cologne, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

package com.arangodb.next.connection;

import deployments.ProxiedContainerDeployment;
import deployments.ProxiedHost;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.arangodb.next.connection.ConnectionTestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * @author Michele Rastelli
 */
@Testcontainers
class ConnectionResiliencyTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionResiliencyTest.class);

    private final ImmutableConnectionConfig.Builder config;

    @Container
    private static final ProxiedContainerDeployment deployment = ProxiedContainerDeployment.ofSingleServer();

    ConnectionResiliencyTest() {
        config = ConnectionConfig.builder();
    }

    @BeforeEach
    void restore() {
        deployment.getProxiedHosts().forEach(it -> {
            it.enableProxy();
            it.getProxy().setConnectionCut(false);
        });
    }

    @ParameterizedTest
    @EnumSource(ArangoProtocol.class)
    void requestTimeout(ArangoProtocol protocol) {
        HostDescription host = deployment.getHosts().get(0);
        ConnectionConfig testConfig = config.timeout(2000).build();
        ArangoConnection connection = new ArangoConnectionFactory(testConfig, protocol, DEFAULT_SCHEDULER_FACTORY)
                .create(host, deployment.getAuthentication()).block();
        assertThat(connection).isNotNull();

        performRequest(connection);

        deployment.getProxiedHosts().forEach(it -> it.getProxy().setConnectionCut(true));
        Throwable thrown = catchThrowable(() -> performRequest(connection));
        assertThat(Exceptions.unwrap(thrown)).isInstanceOf(TimeoutException.class);

        deployment.getProxiedHosts().forEach(it -> it.getProxy().setConnectionCut(false));
        performRequest(connection);

        connection.close().block();
    }


    @ParameterizedTest
    @EnumSource(ArangoProtocol.class)
    void VstConnectionTimeout(ArangoProtocol protocol) {
        HostDescription host = deployment.getHosts().get(0);
        ConnectionConfig testConfig = config.timeout(2000).build();
        deployment.getProxiedHosts().forEach(it -> it.getProxy().setConnectionCut(true));
        Throwable thrown = catchThrowable(() ->
                new ArangoConnectionFactory(testConfig, protocol, DEFAULT_SCHEDULER_FACTORY)
                        .create(host, deployment.getAuthentication()).block());
        assertThat(Exceptions.unwrap(thrown)).isInstanceOf(TimeoutException.class);
    }

    @ParameterizedTest
    @EnumSource(ArangoProtocol.class)
    void closeConnection(ArangoProtocol protocol) {
        HostDescription host = deployment.getHosts().get(0);
        ConnectionConfig testConfig = config.build();
        ArangoConnection connection = new ArangoConnectionFactory(testConfig, protocol, DEFAULT_SCHEDULER_FACTORY)
                .create(host, deployment.getAuthentication()).block();
        assertThat(connection).isNotNull();
        performRequest(connection);
        connection.close().block();
    }

    @Test
    void closeConnectionTwice() {
        HostDescription host = deployment.getHosts().get(0);
        ConnectionConfig testConfig = config.build();
        ArangoConnection connection = new ArangoConnectionFactory(testConfig, ArangoProtocol.VST, DEFAULT_SCHEDULER_FACTORY)
                .create(host, deployment.getAuthentication()).block();
        assertThat(connection).isNotNull();

        Mono<ArangoResponse> response = connection.execute(versionRequest);
        connection.close().block();

        Throwable thrown = catchThrowable(response::block);
        assertThat(Exceptions.unwrap(thrown)).isInstanceOf(IOException.class).hasMessageContaining("Connection closed");
    }

    @ParameterizedTest
    @EnumSource(ArangoProtocol.class)
    void requestWhenDisconnected(ArangoProtocol protocol) {
        HostDescription host = deployment.getHosts().get(0);
        ConnectionConfig testConfig = config.build();
        ArangoConnection connection = new ArangoConnectionFactory(testConfig, protocol, DEFAULT_SCHEDULER_FACTORY)
                .create(host, deployment.getAuthentication()).block();
        assertThat(connection).isNotNull();
        deployment.getProxiedHosts().forEach(ProxiedHost::disableProxy);
        Throwable thrown = catchThrowable(() -> performRequest(connection));
        assertThat(Exceptions.unwrap(thrown)).isInstanceOfAny(IOException.class, TimeoutException.class);
        connection.close().block();
    }

    @ParameterizedTest
    @EnumSource(ArangoProtocol.class)
    void reconnect(ArangoProtocol protocol) {
        HostDescription host = deployment.getHosts().get(0);
        ConnectionConfig testConfig = config
                .timeout(5000)
                .build();
        ArangoConnection connection = new ArangoConnectionFactory(testConfig, protocol, DEFAULT_SCHEDULER_FACTORY)
                .create(host, deployment.getAuthentication()).block();
        assertThat(connection).isNotNull();

        performRequest(connection);

        for (int i = 0; i < 100; i++) {
            performRequest(connection);
            deployment.getProxiedHosts().forEach(ProxiedHost::disableProxy);
            Throwable thrown = catchThrowable(() -> performRequest(connection));
            assertThat(Exceptions.unwrap(thrown)).isInstanceOfAny(IOException.class, TimeoutException.class);
            deployment.getProxiedHosts().forEach(ProxiedHost::enableProxy);
            performRequest(connection, 1);
        }

        connection.close().block();
    }

}