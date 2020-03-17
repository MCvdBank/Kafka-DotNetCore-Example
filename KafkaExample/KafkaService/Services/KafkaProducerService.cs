using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaService.Services
{
	/// <summary>
	/// Wrapper service that provides Kafka producer functionality
	/// </summary>
	public class KafkaProducerService
	{
		private readonly ILogger _logger;
		private readonly IProducer<string, string> _producer;
		private readonly string _topicName;

		#region ctor

		/// <summary>
		/// Constructor that sets up a Kafka producer
		/// </summary>
		/// <param name="logger">Log interface to use for logging</param>
		/// <param name="config"><see cref="ProducerConfig"/> required for configuration</param>
		/// <param name="topicName">Kafka topic to publish to</param>
		public KafkaProducerService(ILogger logger, ProducerConfig config, string topicName)
		{
			_logger = logger;

			// Configure producer
			_topicName = topicName;
			_producer = new ProducerBuilder<string, string>(config)
			            .SetErrorHandler((_, e) => { _logger.LogError("Producer Error:" + e); })
			            .Build();
		}

		#endregion ctor

		#region Public methods

		/// <summary>
		/// Writes an object to a Kafka stream
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="messageObject">Object to serialize and to write to stream</param>
		/// <returns>Returns a <see cref="Task"/> when finished</returns>
		public async Task Write<T>(T messageObject)
		{
			// Serialize message
			var message = JsonConvert.SerializeObject(messageObject);

			await Write(message);
		}

		/// <summary>
		/// Writes a string message to a Kafka stream
		/// </summary>
		/// <param name="message">Message to write to stream</param>
		/// <returns>Returns a <see cref="Task"/> when finished</returns>
		public async Task Write(string message)
		{
			// Create message to be sent
			var msg = new Message<string, string>
			{
				Key = Guid.NewGuid().ToString(),
				Value = message,
				Headers = new Headers(),
				Timestamp = Timestamp.Default
			};

			try
			{
				// Publish a message on the stream
				var deliveryResult = await _producer.ProduceAsync(_topicName, msg);

				_logger.LogInformation($"KAFKA: Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
			}
			catch (Exception e)
			{
				_logger.LogError($"Producer Exception: {e.Message}");
			}
		}

		#endregion Public methods
	}
}
