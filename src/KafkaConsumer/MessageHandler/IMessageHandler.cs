using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsumer.MessageHandler
{
	public interface IMessageHandler<TKey, TValue>
	{
		Task HandleAsync(Message<TKey, TValue> message);
	}
}