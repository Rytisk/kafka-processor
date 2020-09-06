using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueue<TKey, TValue>
	{
		Task EnqueueAsync(ConsumeResult<TKey, TValue> consumeResult);
	}
}