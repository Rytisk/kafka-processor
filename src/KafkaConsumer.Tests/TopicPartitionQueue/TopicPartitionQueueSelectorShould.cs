using KafkaConsumer.Exceptions;
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
		private readonly TopicPartitionQueueSelector<string, string> _topicPartitionQueueSelector;

		public TopicPartitionQueueSelectorShould()
		{
			_topicPartitionQueue = new Mock<ITopicPartitionQueue<string, string>>();
			_topicPartitionQueueFactory = new Mock<ITopicPartitionQueueFactory<string, string>>();

			_topicPartitionQueueSelector = new TopicPartitionQueueSelector<string, string>(
				_topicPartitionQueueFactory.Object);
		}

		[Fact]
		public void SelectTopicPartitionQueue()
		{
			// arrange
			var tp = DataGenerator.TopicPartition;

			_topicPartitionQueueFactory
							.Setup(tpqf => tpqf.Create())
							.Returns(_topicPartitionQueue.Object);

			_topicPartitionQueueSelector.Fill(new[] { tp });

			// act
			var topicParitionQueue = _topicPartitionQueueSelector.Select(tp);

			// assert
			topicParitionQueue.IsEquivalentTo(_topicPartitionQueue.Object);
		}

		[Fact]
		public void FillQueues()
		{
			// arrange
			var tps = DataGenerator.GenerateTopicPartitions(10);

			_topicPartitionQueueFactory
				.Setup(tpqf => tpqf.Create())
				.Returns(_topicPartitionQueue.Object);

			// act
			_topicPartitionQueueSelector.Fill(tps);

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
				.Setup(tpqf => tpqf.Create())
				.Returns(_topicPartitionQueue.Object);

			_topicPartitionQueueSelector.Fill(tps);

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