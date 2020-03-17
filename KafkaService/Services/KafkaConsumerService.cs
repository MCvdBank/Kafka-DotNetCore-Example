using System;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaService.Services
{
	/// <summary>
	/// Wrapper service that provides Kafka consumer functionality
	/// </summary>
	public class KafkaConsumerService
	{
		private readonly ILogger _logger;
		private readonly ConsumerConfig _config;
		private readonly IConsumer<string, string> _consumer;

		#region ctor

		/// <summary>
		/// Constructor that sets up a Kafka consumer
		/// </summary>
		/// <param name="logger">Log interface to use for logging</param>
		/// <param name="config"><see cref="ConsumerConfig"/> required for configuration</param>
		/// <param name="topicName">Kafka topic to subscribe to</param>
		public KafkaConsumerService(ILogger logger, ConsumerConfig config, string topicName)
		{
			_logger = logger;
			_config = config;

			// Configure consumer and subscribe to topic 
			_consumer = new ConsumerBuilder<string, string>(_config)
			            .SetErrorHandler((_, e) => _logger.LogError($"Consumer Error: {e.Reason}"))
			            .SetLogHandler((_, e) => _logger.LogDebug($"Consumer Log: {e.Message}"))
			            .SetOffsetsCommittedHandler((_, e) => _logger.LogDebug($"Consumer Offset Committed: {e.Error}"))
			            .SetPartitionsAssignedHandler((_, e) => _logger.LogDebug($"Consumer Partition Assigned"))
			            .SetPartitionsRevokedHandler((_, e) => _logger.LogDebug($"Consumer Partition Revoked"))
			            .Build();
			_consumer.Subscribe(topicName);
		}

		#endregion ctor

		#region Public methods

		/// <summary>
		/// Blocks until a message has been published to a Kafka stream, and return the consumed message
		/// </summary>
		/// <typeparam name="T">Object type as which to return message as</typeparam>
		/// <param name="stoppingToken">Cancellation token</param>
		/// <returns>Returns a message object that was consumed from a Kafka stream</returns>
		public T WaitAndRead<T>(CancellationToken stoppingToken) where T : class
		{
			var result = WaitAndRead(stoppingToken);
			return result != null ? JsonConvert.DeserializeObject<T>(result) : null;
		}

		/// <summary>
		/// Blocks until a message has been published to a Kafka stream, and return the consumed message
		/// </summary>
		/// <param name="stoppingToken">Cancellation token</param>
		/// <returns>Returns a message that was consumed from a Kafka stream</returns>
		public string WaitAndRead(CancellationToken stoppingToken)
		{
			try
			{
				// Consume the next available message on the stream
				// This call will block until a new message is available
				var consumeResult = _consumer.Consume(stoppingToken);

				// If enablepartioneof is enabled, only use the message if EOF has not been reached (message value will be NULL otherwise)
				if (((_config.EnablePartitionEof ?? false) && !consumeResult.IsPartitionEOF) ||
				    !(_config.EnablePartitionEof ?? false))
				{
					_logger.LogInformation($"KAFKA: Consumed new message on topic '{consumeResult.TopicPartitionOffset}'");

					// Only commit if Auto Commit is disabled
					if (!(_config.EnableAutoCommit ?? true))
					{
						_consumer.Commit();
					}

					return consumeResult.Value;
				}

				_logger.LogInformation($"KAFKA: EOF reached at offset {consumeResult.Offset}");
				return null;
			}
			catch (OperationCanceledException)
			{
				_logger.LogInformation($"KAFKA: Closing consumer");
				_consumer.Close();
			}
			catch (Exception e)
			{
				_logger.LogError($"Consumer Exception: {e.Message}");
			}

			return null;
		}

		#endregion Public methods
	}
}
