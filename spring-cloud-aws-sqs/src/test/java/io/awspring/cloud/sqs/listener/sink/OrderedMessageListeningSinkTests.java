/*
 * Copyright 2013-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.listener.sink;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import io.awspring.cloud.sqs.listener.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipeline;
import io.awspring.cloud.sqs.observation.MessageObservationDocumentation;
import io.awspring.cloud.sqs.observation.MessageObservationDocumentation.HighCardinalityKeyNames;
import io.awspring.cloud.sqs.observation.MessagingOperationType;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Tests for {@link OrderedMessageSink}.
 *
 * @author Tomaz Fernandes
 * @author Mariusz Sondecki
 */
class OrderedMessageListeningSinkTests {

	@Test
	void shouldEmitInOrder() {
		TestObservationRegistry registry = TestObservationRegistry.create();
		int numberOfMessagesToEmit = 1000;
		List<Message<Integer>> messagesToEmit = IntStream.range(0, numberOfMessagesToEmit)
				.mapToObj(index -> MessageBuilder.withPayload(index).build()).collect(toList());
		List<Message<Integer>> received = new ArrayList<>(numberOfMessagesToEmit);
		AbstractMessageProcessingPipelineSink<Integer> sink = new OrderedMessageSink<>();
		sink.setObservationRegistry(registry);
		sink.setTaskExecutor(Runnable::run);
		sink.setMessagePipeline(getMessageProcessingPipeline(received));
		sink.start();
		sink.emit(messagesToEmit, MessageProcessingContext.create()).join();
		sink.stop();
		assertThat(received).containsSequence(messagesToEmit);
		TestObservationRegistryAssert.assertThat(registry)
				.hasNumberOfObservationsWithNameEqualTo("sqs.single.message.polling.process", numberOfMessagesToEmit)
				.forAllObservationsWithNameEqualTo("sqs.single.message.polling.process",
						observationContextAssert -> observationContextAssert
								.hasHighCardinalityKeyValueWithKey(HighCardinalityKeyNames.MESSAGE_ID.asString())
								.hasLowCardinalityKeyValue(
										MessageObservationDocumentation.LowCardinalityKeyNames.OPERATION.asString(),
										MessagingOperationType.SINGLE_POLLING_PROCESS.getValue()));
	}

	private MessageProcessingPipeline<Integer> getMessageProcessingPipeline(List<Message<Integer>> received) {
		return new MessageProcessingPipeline<>() {
			@Override
			public CompletableFuture<Message<Integer>> process(Message<Integer> message,
					MessageProcessingContext<Integer> context) {
				received.add(message);
				return CompletableFuture.completedFuture(message);
			}
		};
	}

}
