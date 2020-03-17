using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaService.Services;
using KafkaWebApi.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaWebApi.Services
{
	public class BookingProcessingService : BackgroundService
	{
		private readonly ILogger _logger;
		private readonly ConsumerConfig _consumerConfig;
		private readonly ProducerConfig _producerConfig;

		public BookingProcessingService(ILogger<BookingProcessingService> logger,
		                                IOptions<ConsumerConfig> consumerConfig,
		                                IOptions<ProducerConfig> producerConfig)
		{
			_logger = logger;
			_producerConfig = producerConfig.Value;
			_consumerConfig = consumerConfig.Value;
		}

		protected override async Task ExecuteAsync(CancellationToken stoppingToken)
		{
			_logger.LogInformation("Booking Processing Service Started");

			// Magic line to yield control of this background service back to the rest of the startup process
			await Task.Yield();

			// Create new consumer and producer
			var kafkaConsumerService = new KafkaConsumerService(_logger, _consumerConfig, KafkaTopics.TOPIC_BOOKING_REQUEST);
			var kafkaProducerService = new KafkaProducerService(_logger, _producerConfig, KafkaTopics.TOPIC_BOOKING_CONFIRMATION);

			while (!stoppingToken.IsCancellationRequested)
			{
				try
				{
					// Wait for a new booking request
					var bookingRequest = kafkaConsumerService.WaitAndRead<MovieBooking>(stoppingToken);
					if (bookingRequest != null)
					{
						_logger.LogInformation($"Processing new booking: {bookingRequest}");

						// Create new booking confirmation based on a request
						var bookingConfirmation = new BookingConfirmation(bookingRequest);

						// Publish a booking confirmation to a Kafka stream
						await kafkaProducerService.Write(bookingConfirmation);
					}
				}
				catch (Exception e)
				{
					_logger.LogError($"Error in processing : {e.Message}");
				}
			}
		}
	}
}
