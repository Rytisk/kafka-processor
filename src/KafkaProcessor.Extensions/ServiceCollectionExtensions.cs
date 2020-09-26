using KafkaProcessor.MessageHandler;
using KafkaProcessor.Processor.Config;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaProcessor.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaProcessor<TKey, TValue, THandler>(
            this IServiceCollection services,
            ProcessorConfig config) where THandler : class, IMessageHandler<TKey, TValue>
        {
            services.AddScoped<THandler>();

            services.AddSingleton<IHostedService>(sp => 
            {
                var logger = sp.GetService<ILogger<KafkaBackgroundService<TKey, TValue, THandler>>>();
                var lifetime = sp.GetService<IHostApplicationLifetime>();

                return new KafkaBackgroundService<TKey, TValue, THandler>(
                    lifetime, 
                    logger, 
                    Options.Create(config),
                    new ScopedHandlerFactory<TKey, TValue>(sp));
            });


            return services;
        }
    }
}