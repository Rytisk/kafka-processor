using System.Collections.Generic;
using Confluent.Kafka;
using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueueSelector<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Select(TopicPartition topicPartition);
		void AddQueue(TopicPartition topicPartition, IMessageHandler<TKey, TValue> messageHandler);
		void Remove(IEnumerable<TopicPartition> topicPartitions);
	}
}