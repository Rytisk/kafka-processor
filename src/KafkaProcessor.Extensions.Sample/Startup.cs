using KafkaProcessor.Extensions.Sample.MessageHandlers;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using KafkaProcessor.Processor.Config;

namespace KafkaProcessor.Extensions.Sample
{
    public class Startup
    {
        private readonly IConfiguration _configuration;
        public Startup(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddKafkaProcessor<string, string, SimpleMessageHandler>(
                _configuration
                    .GetSection("TestProcessor")
                    .Get<ProcessorConfig>());

            services.AddKafkaProcessor<string, string, AnotherMessageHandler>(
                _configuration
                    .GetSection("AnotherProcessor")
                    .Get<ProcessorConfig>());
        }

        public void Configure(IApplicationBuilder app)
        {
            app.UseRouting();
        }
    }
}
