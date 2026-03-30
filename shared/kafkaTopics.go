package shared

const (
	TopicReservation           = "Reservations"
	TopicReservationSuccessful = "Reservation-successful"
	TopicPayment               = "Payment"
	TopicPaymentSuccessful     = "Payment-Successful"
	TopicPaymentFailure        = "Payment-Failed"
	TopicDeadLetterQueue       = "Dead-Letter-Queue"
	TopicFlashSaleEnded        = "Flash-sale-ended"

	TopicReservationGroup           = "Reservation-group"
	TopicReservationSuccessfulGroup = "InsertionToSQL-group"
	TopicPaymentGroup               = "Payment-group"
	TopicPaymentSuccessfulGroup     = "UpdateToSQL-group"
	TopicPaymentFailureGroup        = "Rollback-group"
	TopicFlashSaleEndedGroup        = "Flash-sale-ended-group"
)

/*
Topic ownership — who produces and who consumes each topic:

PRODUCERS:
  - API/Producer        writes -> Reservation, Payment, Payment-Failure

CONSUMERS:
  - Reservation Worker          reads: Reservation             writes: Reservation-Successful
  - Payment Worker              reads: Payment                 writes: Payment-Successful, Payment-Failure
  - Reservation Persistence     reads: Reservation-Successful, Payment-Successful
  - Rollback Worker             reads: Payment-Failure          writes: Reservation
*/
