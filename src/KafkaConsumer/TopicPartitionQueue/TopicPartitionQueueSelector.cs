using System.Collections.Generic;
using Confluent.Kafka;
using KafkaConsumer.Exceptions;
using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueueSelector<TKey, TValue> : ITopicPartitionQueueSelector<TKey, TValue>
	{
		private readonly Dictionary<TopicPartition, ITopicPartitionQueue<TKey, TValue>> _queues;
		private readonly ITopicPartitionQueueFactory<TKey, TValue> _topicPartitionQueueFactory;

		public TopicPartitionQueueSelector(ITopicPartitionQueueFactory<TKey, TValue> topicPartitionQueueFactory)
		{
			_queues = new Dictionary<TopicPartition, ITopicPartitionQueue<TKey, TValue>>();
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
			_queues.Add(topicPartition, _topicPartitionQueueFactory.Create(messageHandler));
		}

		public void Remove(IEnumerable<TopicPartition> topicPartitions)
		{
			foreach (var tp in topicPartitions)
			{
				//TODO: finalize each TopicPartitionQueue?

				_queues.Remove(tp);
			}
		}
	}
}