using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaService.Services;
using KafkaWebApi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaWebApi.Controllers
{
	[Route("api/[controller]")]
	public class BookingController : ControllerBase
	{
		private readonly ILogger _logger;
		private readonly ProducerConfig _config;

		public BookingController(ILogger<BookingController> logger, IOptions<ProducerConfig> config)
		{
			_logger = logger;
			_config = config.Value;
		}

		[HttpPost]
		public async Task<ActionResult> PostAsync([FromBody] MovieBooking booking)
		{
			if (!ModelState.IsValid)
			{
				return BadRequest(ModelState);
			}

			_logger.LogInformation($"New movie booking received: {booking}");

			// Publish a new booking to the stream
			var kafkaProducerService = new KafkaProducerService(_logger, _config, KafkaTopics.TOPIC_BOOKING_REQUEST);
			await kafkaProducerService.Write(booking);

			return Ok("Your booking is being processed");
		}
	}
}
