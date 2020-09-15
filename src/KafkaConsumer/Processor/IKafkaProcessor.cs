using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer.Processor
{
	public interface IKafkaProcessor<TKey, TValue> : IDisposable
	{
		Task ProcessMessagesAsync(CancellationToken cancellationToken);
	}
}