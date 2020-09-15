using KafkaConsumer.MessageHandler;
using System.Threading.Tasks;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueue<TKey, TValue>
	{
		Task EnqueueAsync(Message<TKey, TValue> message);
		Task<bool> TryEnqueueAsync(Message<TKey, TValue> message);
		Task CompleteAsync();
		Task AbortAsync();
	}
}