using Confluent.Kafka;
using KafkaProcessor.Processor;
using KafkaProcessor.Sample.MessageHandlers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProcessor.Sample
{
	public class KafkaBackgroundService : BackgroundService
	{
		private readonly IHostApplicationLifetime _hostApplicationLifetime;
		private readonly ILogger<KafkaBackgroundService> _logger;
		private readonly ConsumerConfig _consumerConfig;

		public KafkaBackgroundService(
			IHostApplicationLifetime hostApplicationLifetime,
			ILogger<KafkaBackgroundService> logger,
			IOptions<ConsumerConfig> consumerConfig)
		{
			_hostApplicationLifetime = hostApplicationLifetime;
			_consumerConfig = consumerConfig.Value;
			_logger = logger;
		}

		protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
		{
			try
			{
				using var kafkaProcessor = KafkaProcessorBuilder<string, string>
					.CreateDefault()
					.WithConfig(_consumerConfig)
					.WithHandlerFactory(tp => new SimpleMessageHandler(tp.Topic, tp.Partition))
					.FromTopic("test-topic")
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
	}
}
