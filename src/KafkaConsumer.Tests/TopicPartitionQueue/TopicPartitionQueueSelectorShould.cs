using KafkaConsumer.Exceptions;
using KafkaConsumer.MessageHandler;
using KafkaConsumer.Tests.Extensions;
using KafkaConsumer.TopicPartitionQueue;
using Moq;
using Xunit;

namespace KafkaConsumer.Tests.TopicPartitionQueue
{
	public class TopicPartitionQueueSelectorShould
	{
		private readonly Mock<ITopicPartitionQueueFactory<string, string>> _topicPartitionQueueFactory;
		private readonly Mock<ITopicPartitionQueue<string, string>> _topicPartitionQueue;
		private readonly Mock<IMessageHandler<string, string>> _messageHandler;
		private readonly TopicPartitionQueueSelector<string, string> _topicPartitionQueueSelector;

		public TopicPartitionQueueSelectorShould()
		{
			_messageHandler = new Mock<IMessageHandler<string, string>>();
			_topicPartitionQueue = new Mock<ITopicPartitionQueue<string, string>>();
			_topicPartitionQueueFactory = new Mock<ITopicPartitionQueueFactory<string, string>>();

			_topicPartitionQueueSelector = new TopicPartitionQueueSelector<string, string>(
				_topicPartitionQueueFactory.Object,
				1000);
		}

		[Fact]
		public void SelectTopicPartitionQueue()
		{
			// arrange
			var tp = DataGenerator.TopicPartition;

			_topicPartitionQueueFactory
				.Setup(tpqf => tpqf.Create(
					It.IsAny<IMessageHandler<string, string>>(),
					It.IsAny<int>()))
				.Returns(_topicPartitionQueue.Object);

			_topicPartitionQueueSelector.AddQueue(tp, _messageHandler.Object);

			// act
			var topicParitionQueue = _topicPartitionQueueSelector.Select(tp);

			// assert
			topicParitionQueue.IsEquivalentTo(_topicPartitionQueue.Object);
		}

		[Fact]
		public void AddQueues()
		{
			// arrange
			var tps = DataGenerator.GenerateTopicPartitions(10);

			_topicPartitionQueueFactory
				.Setup(tpqf => tpqf.Create(
					It.IsAny<IMessageHandler<string, string>>(),
					It.IsAny<int>()))
				.Returns(_topicPartitionQueue.Object);

			// act
			foreach (var tp in tps)
			{
				_topicPartitionQueueSelector.AddQueue(tp, _messageHandler.Object);
			}

			// assert
			foreach (var tp in tps)
			{
				var tpq = _topicPartitionQueueSelector.Select(tp);

				tpq.IsEquivalentTo(_topicPartitionQueue.Object);
			}
		}

		[Fact]
		public void RemoveQueues()
		{
			// arrange
			var tps = DataGenerator.GenerateTopicPartitions(10);

			_topicPartitionQueueFactory
				.Setup(tpqf => tpqf.Create(
					It.IsAny<IMessageHandler<string, string>>(),
					It.IsAny<int>()))
				.Returns(_topicPartitionQueue.Object);

			foreach (var tp in tps)
			{
				_topicPartitionQueueSelector.AddQueue(tp, _messageHandler.Object);
			}

			// act
			_topicPartitionQueueSelector.Remove(tps);

			// assert
			foreach (var tp in tps)
			{
				Assert.Throws<TopicPartitionQueueException>(
					() => _topicPartitionQueueSelector.Select(tp));
			}
		}

		[Fact]
		public void AbortQueuesOnRemove()
		{
			// arrange
			var tp = DataGenerator.TopicPartition;

			_topicPartitionQueueFactory
				.Setup(tpqf => tpqf.Create(
					It.IsAny<IMessageHandler<string, string>>(),
					It.IsAny<int>()))
				.Returns(_topicPartitionQueue.Object);
			
			_topicPartitionQueueSelector.AddQueue(tp, _messageHandler.Object);

			// act
			_topicPartitionQueueSelector.Remove(new []{ tp });

			// assert
			_topicPartitionQueue.Verify(tpq => tpq.AbortAsync(), Times.Once());
		}

		[Fact]
		public void ThrowIfTopicPartitionNotFound()
		{
			// arrange
			var tp = DataGenerator.TopicPartition;

			// act
			var exception = Assert.Throws<TopicPartitionQueueException>(
				() => _topicPartitionQueueSelector.Select(tp));

			// assert
			exception.TopicPartition.IsEquivalentTo(tp);
		}
	}
}