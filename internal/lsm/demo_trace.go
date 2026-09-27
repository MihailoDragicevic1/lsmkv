package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	demoCrashPointAfterWALSyncBeforeMemtable  = "afterWALSyncBeforeMemtable"
	demoCrashPointAfterFlushSSTBeforeManifest = "afterFlushSSTBeforeManifest"

	demoPausePointBeforeFlushImmutable = "beforeFlushImmutable"
	demoResumeFlushSignal              = "resume-flush.signal"
)

// runDemoCrashPoint terminates the current process only when the requested
// point matches the LSMKV_DEMO_CRASH_AT environment variable.
//
// The normal engine path is unchanged when LSMKV_DEMO_CRASH_AT is unset.
func runDemoCrashPoint(point string) {
	if os.Getenv("LSMKV_DEMO_CRASH_AT") != point {
		return
	}

	fmt.Fprintf(os.Stderr, "DEMO CRASH: point=%s\n", point)
	os.Exit(86)
}

// waitDemoPauseBeforeFlush blocks a background flush only when the explicit
// demo pause environment variable is enabled. The caller resumes it by
// creating <data_dir>/resume-flush.signal.
func waitDemoPauseBeforeFlush(dataDir string) {
	if os.Getenv("LSMKV_DEMO_PAUSE_AT") != demoPausePointBeforeFlushImmutable {
		return
	}

	signalPath := filepath.Join(dataDir, demoResumeFlushSignal)

	fmt.Printf(
		"DEMO PAUSE: point=%s; create this file to resume: %s\n",
		demoPausePointBeforeFlushImmutable,
		signalPath,
	)

	for {
		if _, err := os.Stat(signalPath); err == nil {
			if err := os.Remove(signalPath); err != nil {
				fmt.Fprintf(
					os.Stderr,
					"DEMO PAUSE ERROR: remove resume signal %s: %v\n",
					signalPath,
					err,
				)
			}

			fmt.Println("DEMO RESUME: background flush continues.")
			return
		} else if !os.IsNotExist(err) {
			fmt.Fprintf(
				os.Stderr,
				"DEMO PAUSE ERROR: check resume signal %s: %v\n",
				signalPath,
				err,
			)
			return
		}

		time.Sleep(20 * time.Millisecond)
	}
}
