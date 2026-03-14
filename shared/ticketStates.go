package shared

const (
	Pending               = "PENDING"
	SuccessfulReservation = "SUCCESSFUL_RESERVATION"
	WaitingList           = "WAITING_LIST"
	Failed                = "FAILED"
	SoldOut               = "SOLD_OUT"
	Paid                  = "PAID"
)

// To find where each state is set, search codebase for rdb.Set
