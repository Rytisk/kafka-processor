using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaProcessor.Processor;
using KafkaProcessor.Processor.Config;
using KafkaProcessor.Tests.Extensions;
using KafkaProcessor.TopicPartitionQueue;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace KafkaProcessor.Tests.Processor
{
    public class KafkaProcessorShould
    {
        private readonly Mock<IConsumer<string, string>> _consumer;
        private readonly Mock<ITopicPartitionQueue<string, string>> _topicPartitionQueue;
        private readonly Mock<ITopicPartitionQueueSelector<string, string>> _topicPartitionQueueSelector;
        private readonly KafkaProcessor<string, string> _kafkaProcessor;
        private readonly ProcessorConfig _config;

        public KafkaProcessorShould()
        {
            _topicPartitionQueue = new Mock<ITopicPartitionQueue<string, string>>();
            _consumer = new Mock<IConsumer<string, string>>();
            _topicPartitionQueueSelector = new Mock<ITopicPartitionQueueSelector<string, string>>();
            _config = new ProcessorConfig
            {
                Topic = "test-topic"
            };

            _kafkaProcessor = new KafkaProcessor<string, string>(
                _consumer.Object,
                _topicPartitionQueueSelector.Object,
                Options.Create(_config));
        }

        [Fact]
        public async Task EnqueueConsumedResultToTopicPartitionQueue()
        {
            // arrange
            var cr = DataGenerator.ConsumeResult;
            var message = DataGenerator.GenerateMessage(_consumer.Object);
            var cts = new CancellationTokenSource();

            _consumer
                .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Callback<CancellationToken>(_ => cts.Cancel())
                .Returns(cr);

            _topicPartitionQueueSelector
                .Setup(t => t.Select(cr.TopicPartition.IsActual()))
                .Returns(_topicPartitionQueue.Object);

            // act
            await _kafkaProcessor.ProcessMessagesAsync(cts.Token);

            // assert
            _topicPartitionQueue.Verify(
                tpq => tpq.EnqueueAsync(message.IsActual()),
                Times.Once());
        }

        [Fact]
        public async Task CloseConsumerAfterStoppingToProcessMessages()
        {
            // arrange
            var cr = DataGenerator.ConsumeResult;
            var message = DataGenerator.GenerateMessage(_consumer.Object);
            var cts = new CancellationTokenSource();

            _consumer
                .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Callback<CancellationToken>(_ => cts.Cancel())
                .Returns(cr);

            _topicPartitionQueueSelector
                .Setup(t => t.Select(cr.TopicPartition.IsActual()))
                .Returns(_topicPartitionQueue.Object);

            // act
            await _kafkaProcessor.ProcessMessagesAsync(cts.Token);

            // assert
            _consumer.Verify(
                c => c.Close(), 
                Times.Once());
        }

        [Fact]
        public async Task CloseConsumerIfExceptionWasThrown()
        {
            // arrange
            _consumer
                .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Throws<NullReferenceException>();

            // act
            await Assert.ThrowsAsync<NullReferenceException>(
                () => _kafkaProcessor.ProcessMessagesAsync(CancellationToken.None));

            // assert
            _consumer.Verify(
                c => c.Close(),
                Times.Once());
        }
        
        [Fact]
        public async Task SubscribeToTopic()
        {
            // arrange
            var cr = DataGenerator.ConsumeResult;
            var message = DataGenerator.GenerateMessage(_consumer.Object);
            var cts = new CancellationTokenSource();

            _consumer
                .Setup(c => c.Consume(It.IsAny<CancellationToken>()))
                .Callback<CancellationToken>(_ => cts.Cancel())
                .Returns(cr);

            _topicPartitionQueueSelector
                .Setup(t => t.Select(cr.TopicPartition.IsActual()))
                .Returns(_topicPartitionQueue.Object);

            // act
            await _kafkaProcessor.ProcessMessagesAsync(cts.Token);

            // assert
            _consumer.Verify(
                c => c.Subscribe(_config.Topic),
                Times.Once());
        }
    }
}
