using System.Collections.Generic;
using Confluent.Kafka;
using KafkaProcessor.MessageHandler;

namespace KafkaProcessor.TopicPartitionQueue
{
	public interface ITopicPartitionQueueSelector<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Select(TopicPartition topicPartition);
		void AddQueue(TopicPartition topicPartition, IMessageHandler<TKey, TValue> messageHandler);
		void Remove(IEnumerable<TopicPartition> topicPartitions);
	}
}