package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/cnc"
)

// MediaDriver owns a driver directory: the CnC file, the log buffers, and the agents
// that serve them.
type MediaDriver struct {
	cfg       Config
	cnc       *cnc.File
	conductor *Conductor
	runners   []*runner
	closeOnce sync.Once
}

// Open prepares a driver in cfg.Dir without starting its agents; tests then drive
// Conductor().DoWork themselves. If the directory holds the files of a driver that
// is still alive, it fails with ErrActiveDriver; files of a dead driver are removed.
func Open(cfg Config) (*MediaDriver, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	if err := ensureDir(cfg.Dir); err != nil {
		return nil, err
	}
	if err := clearStale(cfg); err != nil {
		return nil, err
	}
	now := cfg.Clock()
	f, err := cnc.Create(cfg.Dir, cnc.Layout{
		ToDriverCapacity: cfg.ToDriverCapacity, ToClientsCapacity: cfg.ToClientsCapacity,
		MaxCounters: cfg.MaxCounters, ClientTimeoutNs: int64(cfg.ClientLivenessTimeout),
		StartNs: now, PID: int64(os.Getpid()),
	})
	if err != nil {
		return nil, err
	}
	cond, err := newConductor(cfg, f)
	if err != nil {
		f.Close()
		return nil, err
	}
	cond.now, cond.lastHeartbeat = now, now
	cond.cmds.SetConsumerHeartbeat(now)
	f.Publish() // clients may attach from here on
	return &MediaDriver{cfg: cfg, cnc: f, conductor: cond}, nil
}

// clearStale removes the previous contents of the directory, unless a live driver owns them.
func clearStale(cfg Config) error {
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if f, err := cnc.Open(cfg.Dir); err == nil {
		ring, rerr := f.ToDriver()
		age := time.Duration(0)
		if rerr == nil {
			age = time.Duration(cfg.Clock() - ring.ConsumerHeartbeat())
		}
		f.Close()
		if rerr == nil && age < cfg.DriverTimeout {
			return fmt.Errorf("%w (%s, heartbeat %v ago)", ErrActiveDriver, cfg.Dir, age)
		}
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(cfg.Dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Start runs the agents, each on its own goroutine.
func (d *MediaDriver) Start() {
	d.runners = append(d.runners, startRunner(d.conductor, d.cfg.Idle(), d.cfg.Clock))
}

// Launch is Open followed by Start.
func Launch(cfg Config) (*MediaDriver, error) {
	d, err := Open(cfg)
	if err != nil {
		return nil, err
	}
	d.Start()
	return d, nil
}

// Conductor returns the control-plane agent, for tests that drive it by hand.
func (d *MediaDriver) Conductor() *Conductor { return d.conductor }

// Dir returns the driver directory clients connect to.
func (d *MediaDriver) Dir() string { return d.cfg.Dir }

// Close stops the agents and releases the shared memory. Clients that still have
// the files mapped can keep reading them; they notice the missing heartbeat.
func (d *MediaDriver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		for _, r := range d.runners {
			r.close()
		}
		d.conductor.OnClose()
		err = d.cnc.Close()
		if d.cfg.DeleteDirOnClose {
			cnc.Remove(d.cfg.Dir)
		}
	})
	return err
}
