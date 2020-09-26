using Confluent.Kafka;
using KafkaProcessor.MessageHandler;
using KafkaProcessor.Processor;
using KafkaProcessor.TopicPartitionQueue;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace KafkaProcessor.Tests.Processor
{
    public class KafkaProcessorBuilderShould
    {
        private readonly Mock<ITopicPartitionQueueSelector<string, string>> _topicPartitionQueueSelector;
        private readonly Mock<IMessageHandler<string, string>> _messageHandler;
        private readonly KafkaProcessorBuilder<string, string> _kafkaProcessorBuilder;
        private readonly Mock<IDeserializer<string>> _keyDeserializer;
        private readonly Mock<IDeserializer<string>> _valueDeserializer;
        private readonly Func<IEnumerable<TopicPartition>, IEnumerable<(TopicPartition, IMessageHandler<string, string>)>> _createHandlers;

        public KafkaProcessorBuilderShould()
        {
            _createHandlers = (partitions) => partitions.Select(p => (p, _messageHandler.Object));
            _keyDeserializer = new Mock<IDeserializer<string>>();
            _valueDeserializer = new Mock<IDeserializer<string>>();
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
            var consumerConfig = new ConsumerConfig();
            
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
        public void ThrowIfCreateHandlersNotSet()
        {
            // arrange
            var consumerConfig = new ConsumerConfig();
            var topic = "topic";

            _kafkaProcessorBuilder
                .WithConfig(consumerConfig)
                .FromTopic(topic);

            // act
            var exception = Assert.Throws<InvalidOperationException>(
                () => _kafkaProcessorBuilder.Build());

            // assert
            Assert.Equal("'createHandlers' must be set!", exception.Message);
        }

        [Fact]
        public void ThrowIfConsumerConfigAlreadySet()
        {
            // arrange
            var consumerConfig = new ConsumerConfig();

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
        public void ThrowIfCreateHandlersAlreadySet()
        {
            // arrange
            _kafkaProcessorBuilder.SetCreateHandlers(_createHandlers);

            // act
            var exception = Assert.Throws<InvalidOperationException>(
                () => _kafkaProcessorBuilder.SetCreateHandlers(_createHandlers));

            // assert
            Assert.Equal("'createHandlers' was already set!", exception.Message);
        }

        [Fact]
        public void ThrowIfKeyDeserializerAlreadySet()
        {
            // arrange
            _kafkaProcessorBuilder.WithKeyDeserializer(_keyDeserializer.Object);

            // act
            var exception = Assert.Throws<InvalidOperationException>(
                () => _kafkaProcessorBuilder.WithKeyDeserializer(_keyDeserializer.Object));

            // assert
            Assert.Equal("'keyDeserializer' was already set!", exception.Message);
        }

        [Fact]
        public void ThrowIfValueDeserializerAlreadySet()
        {
            // arrange
            _kafkaProcessorBuilder.WithValueDeserializer(_valueDeserializer.Object);

            // act
            var exception = Assert.Throws<InvalidOperationException>(
                () => _kafkaProcessorBuilder.WithValueDeserializer(_valueDeserializer.Object));

            // assert
            Assert.Equal("'valueDeserializer' was already set!", exception.Message);
        }
    }
}
