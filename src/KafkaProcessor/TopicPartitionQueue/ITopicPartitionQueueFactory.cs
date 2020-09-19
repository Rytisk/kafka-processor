using KafkaProcessor.MessageHandler;

namespace KafkaProcessor.TopicPartitionQueue
{
	public interface ITopicPartitionQueueFactory<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Create(
			IMessageHandler<TKey, TValue> messageHandler,
			int queueCapacity);
	}
}