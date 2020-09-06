namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueueFactory<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Create();
	}
}