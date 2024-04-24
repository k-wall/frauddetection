/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
	private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionJob.class);

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.enableCheckpointing(10000)) {

			String brokers = args[0];
			String groupId = args[1];
			LOG.info("KWDEBUG FraudDetectionJob main running {} {}", brokers, groupId);
			Map<String, String> tlsConfig = new HashMap<>();
			tlsConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
			tlsConfig.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            tlsConfig.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, getRequiredEnv("TLS_CLUSTER_CA_CRT"));
			tlsConfig.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
			tlsConfig.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, getRequiredEnv("TLS_USER_CRT"));
			tlsConfig.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, getRequiredEnv("TLS_USER_KEY"));

			KafkaSourceBuilder<Transaction> sourceBuilder = KafkaSource.<Transaction>builder();
			for (Map.Entry<String, String> props : tlsConfig.entrySet()) {
				sourceBuilder.setProperty(props.getKey(), props.getValue());
			}

			KafkaSource<Transaction> source = sourceBuilder
					.setTopics("transactions")
					.setBootstrapServers(brokers)
					.setGroupId(groupId)
					.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
					.setValueOnlyDeserializer(new DeserializationSchema<Transaction>() {

						@Override
						public Transaction deserialize(byte[] message) throws IOException {
							return OBJECT_MAPPER.readValue(message, Transaction.class);
						}

						@Override
						public boolean isEndOfStream(Transaction nextElement) {
							return false;
						}

						@Override
						public TypeInformation<Transaction> getProducedType() {
							return TypeInformation.of(Transaction.class);
						}
					})
					.setProperty("commit.offsets.on.checkpoint", "true")
					.setProperty("client.id.prefix", "fraud-source")
					.build();


			KafkaSinkBuilder<Alert> sinkBuilder = KafkaSink.<Alert>builder()
					.setBootstrapServers(brokers);
			for (Map.Entry<String, String> props : tlsConfig.entrySet()) {
				sinkBuilder.setProperty(props.getKey(), props.getValue());
			}

			KafkaSink<Alert> sink = sinkBuilder
					.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
					.setRecordSerializer(KafkaRecordSerializationSchema.builder()
							.setTopic("alerts")
							.setValueSerializationSchema((SerializationSchema<Alert>) element -> {
								try {
									return OBJECT_MAPPER.writeValueAsBytes(element);
								} catch (IOException e) {
									throw new UncheckedIOException(e);
								}
							})
							.build()
					)
					.build();

			DataStream<Transaction> transactions = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "transactions");

			DataStream<Alert> alerts = transactions
					.keyBy(Transaction::getAccountId)
					.process(new FraudDetector())
					.name("fraud-detector");

			alerts.sinkTo(sink);

			LOG.info("KWDEBUG FraudDetectionJob executing");

			env.execute("Fraud Detection");
		}
	}

	private static String getRequiredEnv(String name) {
        return Objects.requireNonNull(System.getenv(name), "env var " + name + " not present");
	}
}
