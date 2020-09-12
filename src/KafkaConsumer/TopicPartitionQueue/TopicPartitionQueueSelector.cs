using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaConsumer.Exceptions;
using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueueSelector<TKey, TValue> : ITopicPartitionQueueSelector<TKey, TValue>
	{
		private readonly Dictionary<TopicPartition, ITopicPartitionQueue<TKey, TValue>> _queues;
		private readonly ITopicPartitionQueueFactory<TKey, TValue> _topicPartitionQueueFactory;
		private readonly int _queueCapacity;

		public TopicPartitionQueueSelector(
			ITopicPartitionQueueFactory<TKey, TValue> topicPartitionQueueFactory,
			int queueCapacity)
		{
			_queues = new Dictionary<TopicPartition, ITopicPartitionQueue<TKey, TValue>>();

			_queueCapacity = queueCapacity;
			_topicPartitionQueueFactory = topicPartitionQueueFactory;
		}

		public ITopicPartitionQueue<TKey, TValue> Select(TopicPartition topicPartition)
		{
			return _queues.TryGetValue(topicPartition, out var topicPartitionQueue)
				? topicPartitionQueue
				: throw new TopicPartitionQueueException(
					topicPartition,
					$"TopicPartitionQueue not found for {topicPartition}");
		}

		public void AddQueue(TopicPartition topicPartition, IMessageHandler<TKey, TValue> messageHandler)
		{
			_queues.Add(topicPartition, _topicPartitionQueueFactory.Create(messageHandler, _queueCapacity));
		}

		public void Remove(IEnumerable<TopicPartition> topicPartitions)
		{
			//TODO: Abort vs Complete
			Task.WaitAll(_queues.Select(q => q.Value.CompleteAsync()).ToArray());

			foreach (var tp in topicPartitions)
			{
				_queues.Remove(tp);
			}
		}
	}
}