using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueueFactory<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Create(
			IMessageHandler<TKey, TValue> messageHandler,
			int queueCapacity);
	}
}