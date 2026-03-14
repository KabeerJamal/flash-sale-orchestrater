package shared

const (
	TopicReservation           = "Reservations"
	TopicReservationSuccessful = "Reservation-successful"
	TopicPayment               = "Payment"
	TopicPaymentSuccessful     = "Payment-Successful"
	TopicPaymentFailure        = "Payment-Failure"

	TopicReservationGroup           = "Reservation-group"
	TopicReservationSuccessfulGroup = "InsertionToSQL-group"
	TopicPaymentGroup               = "Payment-group"
	TopicPaymentSuccessfulGroup     = "UpdateToSQL-group"
	TopicRollbackGroup              = "Rollback-group"
)

/*
Topic ownership — who produces and who consumes each topic:

PRODUCERS:
  - API/Producer        → Reservation, Payment, Payment-Failure

CONSUMERS:
  - Reservation Worker          reads: Reservation             writes: Reservation-Successful
  - Payment Worker              reads: Payment                 writes: Payment-Successful, Payment-Failure
  - Reservation Persistence     reads: Reservation-Successful, Payment-Successful
  - Rollback Worker             reads: Payment-Failure          writes: Reservation
*/
