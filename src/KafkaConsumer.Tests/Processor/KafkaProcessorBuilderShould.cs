using KafkaConsumer.MessageHandler;
using KafkaConsumer.Processor;
using KafkaConsumer.TopicPartitionQueue;
using Moq;
using System;
using Xunit;

namespace KafkaConsumer.Tests.Processor
{
	public class KafkaProcessorBuilderShould
	{
		private readonly Mock<ITopicPartitionQueueSelector<string, string>> _topicPartitionQueueSelector;
		private readonly Mock<IMessageHandler<string, string>> _messageHandler;
		private readonly KafkaProcessorBuilder<string, string> _kafkaProcessorBuilder;

		public KafkaProcessorBuilderShould()
		{
			_messageHandler = new Mock<IMessageHandler<string, string>>();
			_topicPartitionQueueSelector = new Mock<ITopicPartitionQueueSelector<string, string>>();

			_kafkaProcessorBuilder = new KafkaProcessorBuilder<string, string>(_topicPartitionQueueSelector.Object);
		}

		[Fact]
		public void ThrowIfConsumerConfigNotSet()
		{
			// arrange
			var topic = "topic";

			_kafkaProcessorBuilder
				.FromTopic(topic)
				.WithHandlerFactory(_ => _messageHandler.Object);

			// act
			var exception = Assert.Throws<InvalidOperationException>(
				() => _kafkaProcessorBuilder.Build());

			// assert
			Assert.Equal("'consumerConfig' must be set!", exception.Message);
		}

		[Fact]
		public void ThrowIfTopicNotSet()
		{
			// arrange
			var consumerConfig = new Confluent.Kafka.ConsumerConfig();
			
			_kafkaProcessorBuilder
				.WithConfig(consumerConfig)
				.WithHandlerFactory(_ => _messageHandler.Object);

			// act
			var exception = Assert.Throws<InvalidOperationException>(
				() => _kafkaProcessorBuilder.Build());

			// assert
			Assert.Equal("'topic' must be set!", exception.Message);
		}

		[Fact]
		public void ThrowIfHandlerFactoryNotSet()
		{
			// arrange
			var consumerConfig = new Confluent.Kafka.ConsumerConfig();
			var topic = "topic";

			_kafkaProcessorBuilder
				.WithConfig(consumerConfig)
				.FromTopic(topic);

			// act
			var exception = Assert.Throws<InvalidOperationException>(
				() => _kafkaProcessorBuilder.Build());

			// assert
			Assert.Equal("'handlerFactory' must be set!", exception.Message);
		}

		[Fact]
		public void ThrowIfConsumerConfigAlreadySet()
		{
			// arrange
			var consumerConfig = new Confluent.Kafka.ConsumerConfig();

			_kafkaProcessorBuilder.WithConfig(consumerConfig);

			// act

			var exception = Assert.Throws<InvalidOperationException>(
				() => _kafkaProcessorBuilder.WithConfig(consumerConfig));

			// assert
			Assert.Equal("'consumerConfig' was already set!", exception.Message);
		}

		[Fact]
		public void ThrowIfTopicAlreadySet()
		{
			// arrange
			var topic = "topic";

			_kafkaProcessorBuilder.FromTopic(topic);

			// act

			var exception = Assert.Throws<InvalidOperationException>(
				() => _kafkaProcessorBuilder.FromTopic(topic));

			// assert
			Assert.Equal("'topic' was already set!", exception.Message);
		}

		[Fact]
		public void ThrowIfHandlerFactoryAlreadySet()
		{
			// arrange
			_kafkaProcessorBuilder.WithHandlerFactory(_ => _messageHandler.Object);

			// act

			var exception = Assert.Throws<InvalidOperationException>(
				() => _kafkaProcessorBuilder.WithHandlerFactory(_ => _messageHandler.Object));

			// assert
			Assert.Equal("'handlerFactory' was already set!", exception.Message);
		}
	}
}
