using System.Threading.Tasks;

namespace KafkaConsumer.MessageHandler
{
	public interface IMessageHandler<TKey, TValue>
	{
		Task HandleAsync(Message<TKey, TValue> message);
	}
}