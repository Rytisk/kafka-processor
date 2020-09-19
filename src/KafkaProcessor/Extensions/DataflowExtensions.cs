using System.Threading.Tasks.Dataflow;

namespace KafkaProcessor.Extensions
{
    public static class DataflowExtensions
    {
        public static void PropagateErrorsTo(this IDataflowBlock from, IDataflowBlock to)
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