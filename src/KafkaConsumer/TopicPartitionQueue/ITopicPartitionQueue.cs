using KafkaConsumer.MessageHandler;
using System.Threading.Tasks;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueue<TKey, TValue>
	{
		Task<bool> TryEnqueueAsync(Message<TKey, TValue> consumeResult);

		Task CompleteAsync();
	}
}