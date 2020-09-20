using System;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaProcessor.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaService<TKey, TValue>(
            this IServiceCollection services,
            Type handler, IConfiguration configuration, string topic)
        {
			services.AddScoped(handler);

            services.AddSingleton<IHostedService>(sp => 
            {
                var config = Options.Create(
                    configuration
                        .GetSection("Consumer")
                        .Get<ConsumerConfig>());

                var logger = sp.GetService<ILogger<KafkaBackgroundService<TKey, TValue>>>();
                var lifetime = sp.GetService<IHostApplicationLifetime>();
                var scope = sp.CreateScope();

                return new KafkaBackgroundService<TKey, TValue>(
                    lifetime, 
                    logger, 
                    config,
                    scope,
                    handler,
                    topic);
            });


            return services;
        }
    }
}