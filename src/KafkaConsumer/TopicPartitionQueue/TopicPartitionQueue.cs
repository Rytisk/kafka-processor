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

			_bufferBlock.LinkTo(_actionBlock, new DataflowLinkOptions
			{
				PropagateCompletion = true
			});

			PropagateErrors(_actionBlock, _bufferBlock);
		}

		public async Task CompleteAsync()
		{
			_bufferBlock.Complete();

			await _bufferBlock.Completion;
		}

		public async Task AbortAsync()
		{
			_actionBlock.Complete();

			await _actionBlock.Completion;
		}

		public async Task<bool> TryEnqueueAsync(Message<TKey, TValue> message) =>
			await _bufferBlock.SendAsync(message);

		private static void PropagateErrors(IDataflowBlock from, IDataflowBlock to)
		{
			from.Completion.ContinueWith(task =>
			{
				if (task.IsFaulted)
				{
					to.Fault(task.Exception.InnerException);
				}
			});
		}
	}
}