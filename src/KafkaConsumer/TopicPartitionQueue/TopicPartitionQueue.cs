using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueue<TKey, TValue> : ITopicPartitionQueue<TKey, TValue>
	{
		private readonly IMessageHandler<TKey, TValue> _messageHandler;

		private readonly BufferBlock<Message<TKey, TValue>> _bufferBlock;
		private readonly ActionBlock<Message<TKey, TValue>> _actionBlock;

		public TopicPartitionQueue(IMessageHandler<TKey, TValue> messageHandler)
		{
			_messageHandler = messageHandler;

			_bufferBlock = new BufferBlock<Message<TKey, TValue>>(
				new DataflowBlockOptions 
				{
					BoundedCapacity = 1000
				});

			_actionBlock = new ActionBlock<Message<TKey, TValue>>(
				_messageHandler.HandleAsync,
				new ExecutionDataflowBlockOptions 
				{
					BoundedCapacity = 1
				});

			_bufferBlock.LinkTo(_actionBlock);

			PropagateErrors(_actionBlock, _bufferBlock);
		}

		public async Task CompleteAsync()
		{
			_actionBlock.Complete();

			await _actionBlock.Completion;
		}

		public async Task<bool> TryEnqueueAsync(Message<TKey, TValue> consumeResult) =>
			await _bufferBlock.SendAsync(consumeResult);

		private static void PropagateErrors(IDataflowBlock from, IDataflowBlock to)
		{
			from.Completion.ContinueWith((t) =>
			{
				if (t.IsFaulted)
				{
					to.Fault(from.Completion.Exception.InnerException);
				}
			});
		}
	}
}