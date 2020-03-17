using System;
using System.Threading;
using Confluent.Kafka;
using KafkaService.Services;
using Microsoft.Extensions.Logging;

namespace KafkaConsoleClient
{
	class Program
	{
		static void Main(string[] args)
		{
			CancellationTokenSource token = new CancellationTokenSource();
			Console.CancelKeyPress += (_, e) =>
			{
				e.Cancel = true;
				token.Cancel();
			};

			// Create consumer config
			var conf = new ConsumerConfig
			{
				GroupId = "kafka_test_cli",
				BootstrapServers = "localhost:9092",
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnablePartitionEof = true,
			};

			// Create a logger
			var loggerFactory = LoggerFactory.Create(builder =>
			{
				builder
					.AddConsole()
					.AddDebug();
			});
			ILogger<Program> logger = loggerFactory.CreateLogger<Program>();
			logger.LogInformation("Consumer CLI started");

			// Create Kafka consumer and consume messages
			var kafkaConsumerService = new KafkaConsumerService(logger, conf, "topic_booking_request");
			try
			{
				while (!token.IsCancellationRequested)
				{
					var bookingRequest = kafkaConsumerService.WaitAndRead(token.Token);
					if (bookingRequest != null)
					{
						Console.WriteLine($"Consumed message '{bookingRequest}'");
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}
	}
}
