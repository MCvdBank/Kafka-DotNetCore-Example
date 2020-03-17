using System;
using System.Security.Cryptography;

namespace KafkaWebApi.Models
{
	public class BookingConfirmation : MovieBooking
	{
		public Guid BookingReferenceNumber { get; set; }
		public double Price { get; set; }
		public int SeatNumber { get; set; }

		/// <summary>
		/// Copy constructor that generates a new booking confirmation
		/// </summary>
		/// <param name="movieBooking">Base <see cref="MovieBooking"/> to create booking confirmation from</param>
		public BookingConfirmation(MovieBooking movieBooking)
		{
			// Copy base class properties
			MovieName = movieBooking.MovieName;
			Cinema = movieBooking.Cinema;
			ScreeningTime = movieBooking.ScreeningTime;

			// Generate random values for booking confirmation
			BookingReferenceNumber = Guid.NewGuid();
			Price = RandomNumberGenerator.GetInt32(10, 50);
			SeatNumber = RandomNumberGenerator.GetInt32(1, 100);
		}

		/// <summary>
		/// ToString override
		/// </summary>
		/// <returns>Returns string representation of the booking confirmation</returns>
		public override string ToString()
		{
			return $"[Movie booking confirmation: Reference number is {BookingReferenceNumber} for {MovieName}. Final price is {Price}. Your seat is {SeatNumber} in Cinema {Cinema}]";
		}
	}
}
