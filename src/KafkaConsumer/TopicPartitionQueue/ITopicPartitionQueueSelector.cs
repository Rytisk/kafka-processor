using System.Collections.Generic;
using Confluent.Kafka;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueueSelector<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Select(TopicPartition topicPartition);
		void Fill(IEnumerable<TopicPartition> topicPartitions);
		void Remove(IEnumerable<TopicPartition> topicPartitions);
	}
}