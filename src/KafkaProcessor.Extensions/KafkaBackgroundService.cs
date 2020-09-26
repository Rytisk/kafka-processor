using Confluent.Kafka;
using KafkaProcessor.MessageHandler;
using KafkaProcessor.Processor;
using KafkaProcessor.Processor.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProcessor.Extensions
{
    internal class KafkaBackgroundService<TKey, TValue, THandler> : BackgroundService 
        where THandler : IMessageHandler<TKey, TValue>
    {
        private readonly IHostApplicationLifetime _hostApplicationLifetime;
        private readonly ILogger<KafkaBackgroundService<TKey, TValue, THandler>> _logger;
        private readonly ProcessorConfig _processorConfig;
        private readonly ScopedHandlerFactory<TKey, TValue> _handlerFactory;
        
        public KafkaBackgroundService(
            IHostApplicationLifetime hostApplicationLifetime,
            ILogger<KafkaBackgroundService<TKey, TValue, THandler>> logger,
            IOptions<ProcessorConfig> processorConfig,
            ScopedHandlerFactory<TKey, TValue> handlerFactory)
        {
            _hostApplicationLifetime = hostApplicationLifetime;
            _processorConfig = processorConfig.Value;
            _logger = logger;
            _handlerFactory = handlerFactory;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(async () =>
        {
            try
            {
                using var kafkaProcessor = KafkaProcessorBuilder<TKey, TValue>
                    .CreateDefault()
                    .WithProcessorConfig(_processorConfig)
                    .SetCreateHandlers(OnCreateHandlers)
                    .SetRemoveHandlers(OnRemoveHandlers)
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

        private void OnRemoveHandlers(IEnumerable<TopicPartitionOffset> partitions)
        {
            _handlerFactory.Close();
        }

        private IEnumerable<(TopicPartition, IMessageHandler<TKey, TValue>)> OnCreateHandlers(IEnumerable<TopicPartition> partitions)
        {
            foreach (var partition in partitions)
            {
                yield return (partition, _handlerFactory.Create<THandler>(partition));
            }
        }
    }
}
