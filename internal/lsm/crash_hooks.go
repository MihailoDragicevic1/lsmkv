package lsm

// crashHooks holds named injection points used exclusively in tests.
//
// In production every entry is nil — zero overhead, no allocations.
// Tests set a hook via SetCrashHook and clear it with ClearCrashHook.
//
// Named points:
//
//	"afterSSTRename"         — after output SST is durably renamed, before manifest save
//	"midCompactBeforeManifest" — alias for the same point (compact path)
//	"walTailCrash"           — after WAL append, before fsync acknowledgement
var crashHooks = map[string]func(){}

// SetCrashHook registers fn under name. Calling the hook is the test's
// responsibility to simulate a crash — typically a panic recovered by the test.
func SetCrashHook(name string, fn func()) {
	crashHooks[name] = fn
}

// ClearCrashHook removes the hook registered under name. Tests must always
// call this in a defer so subsequent sub-tests run cleanly.
func ClearCrashHook(name string) {
	delete(crashHooks, name)
}

// runCrashHook fires the named hook if one is registered. No-op otherwise.
func runCrashHook(name string) {
	if fn := crashHooks[name]; fn != nil {
		fn()
	}
}
