using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer.Processor
{
	public interface IKafkaProcessor<TKey, TValue>
	{
		Task ProcessMessagesAsync(CancellationToken cancellationToken);
	}
}