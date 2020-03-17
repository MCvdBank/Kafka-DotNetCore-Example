using System;

namespace KafkaWebApi.Models
{
	public class MovieBooking
	{
		public string MovieName { get; set; }
		public int Cinema { get; set; }
		public DateTimeOffset ScreeningTime { get; set; }

		/// <summary>
		/// ToString override
		/// </summary>
		/// <returns>Returns string representation of the movie booking</returns>
		public override string ToString()
		{
			return $"[Movie booking: \"{MovieName}\" in Cinema {Cinema} at {ScreeningTime}]";
		}
	}
}
