using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaConsumer.MessageHandler;
using KafkaConsumer.Tests.Extensions;
using KafkaConsumer.TopicPartitionQueue;
using Moq;
using Xunit;

namespace KafkaConsumer.Tests.TopicPartitionQueue
{
    public class TopicPartitionQueueShould
    {
        private readonly Mock<IMessageHandler<string, string>> _messageHandler;
        private readonly TopicPartitionQueue<string, string> _topicPartitionQueue;
        private readonly Mock<IConsumer<string, string>> _consumer;

        public TopicPartitionQueueShould()
        {
            _consumer = new Mock<IConsumer<string, string>>();
            _messageHandler = new Mock<IMessageHandler<string, string>>();

            _topicPartitionQueue = new TopicPartitionQueue<string, string>(_messageHandler.Object, 1000);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(500)]
        [InlineData(1001)]
        public async Task EnqueueAndInvokeMessageHandler(int count)
        {
            // arrange
            var messages = DataGenerator.GenerateMessages(_consumer.Object, count).ToList();

            _messageHandler
                .Setup(m => m.HandleAsync(It.IsAny<MessageHandler.Message<string, string>>()))
                .Returns(Task.Delay(10));

            // act
            foreach (var message in messages)
            {
                var isEnqueued = await _topicPartitionQueue.TryEnqueueAsync(message);

                Assert.True(isEnqueued);
            }

            await _topicPartitionQueue.CompleteAsync();

            // assert
            MessageHandlerVerifier.Verify(_messageHandler, messages);
        }

        [Fact]
        public async Task PropagateErrorsFromMessageHandlerOnComplete()
        {
            // arrange
            var message = DataGenerator.GenerateMessage(_consumer.Object);

            _messageHandler
                .Setup(m => m.HandleAsync(message.IsActual()))
                .Throws<InvalidOperationException>();

            // act
            await EnqueueWhileSuccessful(message);

            // assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => _topicPartitionQueue.CompleteAsync());

            _messageHandler.Verify(
                m => m.HandleAsync(message.IsActual()),
                Times.Once());
        }

        [Fact]
        public async Task PropagateErrorsFromMessageHandlerOnAbort()
        {
            // arrange
            var message = DataGenerator.GenerateMessage(_consumer.Object);

            _messageHandler
                .Setup(m => m.HandleAsync(message.IsActual()))
                .Throws<InvalidOperationException>();

            // act
            await EnqueueWhileSuccessful(message);

            // assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => _topicPartitionQueue.AbortAsync());

            _messageHandler.Verify(
                m => m.HandleAsync(message.IsActual()),
                Times.Once());
        }

        private async Task EnqueueWhileSuccessful(MessageHandler.Message<string, string> message)
		{
            while (await _topicPartitionQueue.TryEnqueueAsync(message)) { }
        }
    }
}