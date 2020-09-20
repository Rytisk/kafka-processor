using Confluent.Kafka;
using KafkaProcessor.MessageHandler;
using KafkaProcessor.Processor;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProcessor.Extensions
{
	public class KafkaBackgroundService<TKey, TValue> : BackgroundService
	{
		private readonly IHostApplicationLifetime _hostApplicationLifetime;
		private readonly ILogger<KafkaBackgroundService<TKey, TValue>> _logger;
        private readonly IServiceScope _scope;
        private readonly Type _handlerType;
        private readonly ConsumerConfig _consumerConfig;
		private readonly string _topic;

		public KafkaBackgroundService(
			IHostApplicationLifetime hostApplicationLifetime,
			ILogger<KafkaBackgroundService<TKey, TValue>> logger,
			IOptions<ConsumerConfig> consumerConfig,
			IServiceScope scope,
			Type handlerType,
			string topic)
		{
			_hostApplicationLifetime = hostApplicationLifetime;
			_consumerConfig = consumerConfig.Value;
			_logger = logger;
			_scope = scope;
            _handlerType = handlerType;
			_topic = topic;
        }

		protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
		{
			try
			{
				using var kafkaProcessor = KafkaProcessorBuilder<TKey, TValue>
					.CreateDefault()
					.WithConfig(_consumerConfig)
					.WithHandlerFactory(
						tp => (IMessageHandler<TKey, TValue>)_scope.ServiceProvider.GetService(_handlerType))
					.FromTopic(_topic)
					.Build();

				await kafkaProcessor.ProcessMessagesAsync(stoppingToken);
			}
			catch (Exception ex)
			{
				_logger.LogCritical(ex, ex.Message);

				throw;
			}
			finally
			{
				_hostApplicationLifetime.StopApplication();
			}
		});

        public override void Dispose()
        {
			_scope.Dispose();

            base.Dispose();
        }
    
	}
}
