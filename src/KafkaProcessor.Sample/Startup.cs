using Confluent.Kafka;
using KafkaProcessor.Sample.MessageHandlers;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using KafkaProcessor.Extensions;
using KafkaProcessor.MessageHandler;

namespace KafkaProcessor.Sample
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
			services.AddKafkaService<string, string>(
				typeof(SimpleMessageHandler),
				_configuration,
				"test-topic");

			services.AddKafkaService<string, string>(
				typeof(SimpleMessageHandler),
				_configuration,
				"another-topic");
		}

		public void Configure(IApplicationBuilder app)
		{
			app.UseRouting();
		}
	}
}
