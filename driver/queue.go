package driver

// queue carries work from one agent to another. The sender of a closure never touches
// the target's state: the closure runs on the target's own goroutine at the start of
// its duty cycle, so every piece of agent state still has exactly one owner.
type queue chan func()

func newQueue() queue { return make(queue, 1024) }

// post schedules f. It never blocks; it reports false if the queue is full (the
// target is not running, or is hopelessly behind), in which case f is dropped.
func (q queue) post(f func()) bool {
	select {
	case q <- f:
		return true
	default:
		return false
	}
}

// drain runs what is queued and returns how many closures ran.
func (q queue) drain() int {
	n := 0
	for {
		select {
		case f := <-q:
			f()
			n++
		default:
			return n
		}
	}
}
