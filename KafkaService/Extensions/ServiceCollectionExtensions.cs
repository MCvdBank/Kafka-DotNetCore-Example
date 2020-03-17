using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaService.Extensions
{
	public static class ServiceCollectionExtensions
	{
		/// <summary>
		/// Configure and bind all Kafka related services and configurations.
		/// </summary>
		/// <param name="services">Service collection to extend</param>
		/// <param name="config"><see cref="IConfiguration"/> from which to obtain settings</param>
		/// <param name="producerConfigSectionName">Section name in appsettings that contains Kafka producer configuration (default is "producer")</param>
		/// <param name="consumerConfigSectionName">Section name in appsettings that contains Kafka consumer configuration (default is "consumer")</param>
		/// <returns>Returns <see cref="IServiceCollection"/></returns>
		public static IServiceCollection AddKafkaServices(this IServiceCollection services,
		                                                  IConfiguration config,
		                                                  string producerConfigSectionName = "producer",
		                                                  string consumerConfigSectionName = "consumer")
		{
			// Bind producer and consumer to values in appsetting.json and register for dependency injection
			services.Configure<ProducerConfig>(config.GetSection(producerConfigSectionName));
			services.Configure<ConsumerConfig>(config.GetSection(consumerConfigSectionName));

			return services;
		}
	}
}
