using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

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
			services.Configure<ConsumerConfig>(
				_configuration.GetSection("Consumer"));

			services.AddHostedService<KafkaBackgroundService>();
		}

		public void Configure(IApplicationBuilder app)
		{
			app.UseRouting();
		}
	}
}
