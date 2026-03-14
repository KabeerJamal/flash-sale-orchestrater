package shared

const (
	WaitListQueue   = "waitlist_queue"
	TotalPaid       = "total_paid"
	FlashSaleTimers = "flash_sale_timers"
	Reservations    = "reservation"
)

// To find where each queue is called, search codebase for rdb.LPop and rdb.RPush
