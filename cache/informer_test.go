package cache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	etcd "go.withmatt.com/connect-etcd"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
	"go.withmatt.com/connect-etcd/types/etcdserverpb/etcdserverpbconnect"
	"go.withmatt.com/connect-etcd/types/mvccpb"
)

// fakeWatchHandler stubs only Watch; the embedded nil interface panics if
// anything else is called.
type fakeWatchHandler struct {
	etcdserverpbconnect.WatchHandler
	watch func(context.Context, *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error
}

func (f *fakeWatchHandler) Watch(ctx context.Context, stream *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error {
	return f.watch(ctx, stream)
}

// TestInformerStreamCancelsWatch ensures that Informer.stream cleans up its
// Watch stream completely.
func TestInformerStreamCancelsWatch(t *testing.T) {
	t.Parallel()

	// release allows the test to clean up all of its resources, even if
	// the code under test fails to clean up its stream request.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	etcdAddr, watchCanceled := startFakeWatchCancelingEtcd(t, release)

	cl := etcd.NewClient(etcd.Config{
		Endpoints: []string{etcdAddr},
	})
	inf := &Informer{Client: cl, Key: []byte("test-key")}

	// Make sure the test doesn't take forever.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Informer.stream should quickly return the cancelation error.
	const wantErr = `informer: stream canceled: ""`
	if err := inf.stream(ctx); err == nil || err.Error() != wantErr {
		t.Fatalf("stream() error = %v, want %q", err, wantErr)
	}

	// Now check that the server says we cleaned up.
	select {
	case <-watchCanceled:
		// All good!
	case <-ctx.Done():
		t.Fatalf("server-side watch RPC was not canceled after stream() returned (%v)", ctx.Err())
	}
}

// startFakeWatchCancelingEtcd creates a fake etcd server that acts like the
// requested watch is starting at a version that was compacted, so it will be
// canceled immediately. The server accepts the watch create, then cancels the
// established watch stream with no reason, like real etcd does.
//
// Returns the server address and a chan that signals when the stream is
// cleaned up.
func startFakeWatchCancelingEtcd(t *testing.T, release <-chan struct{}) (string, <-chan struct{}) {
	watchCanceled := make(chan struct{})

	path, handler := etcdserverpbconnect.NewWatchHandler(&fakeWatchHandler{
		watch: func(ctx context.Context, stream *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error {
			req, err := stream.Receive()
			if err != nil {
				return err
			}
			watchId := req.GetCreateRequest().WatchId

			// Mimic etcd's behavior when a watch is started from a
			// revision that has already been compacted.

			// Tell the client that the watch is OK.
			if err := stream.Send(&etcdserverpb.WatchResponse{
				WatchId: watchId,
				Created: true,
				Header:  &etcdserverpb.ResponseHeader{Revision: 42},
			}); err != nil {
				return err
			}

			// Now, in place of any revisions, cancel the stream.
			if err := stream.Send(&etcdserverpb.WatchResponse{
				WatchId:  watchId,
				Canceled: true,
			}); err != nil {
				return err
			}

			// Make sure that the client closes the stream. If the
			// client doesn't do this, a real etcd server would
			// keep the request going and could run out of memory
			// if enough canceled streams accumlate.
			select {
			case <-ctx.Done():
				// Note: This will panic if more than one
				// request is made. The handler assumes only
				// one request.
				close(watchCanceled)
			case <-release:
			}
			return ctx.Err()
		},
	})

	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))

	t.Cleanup(func() {
		srv.Close()
	})

	return srv.Listener.Addr().String(), watchCanceled
}

// fakeKVHandler stubs only Range; the embedded nil interface panics if
// anything else is called.
type fakeKVHandler struct {
	etcdserverpbconnect.KVHandler
	rangeFunc func(context.Context, *connect.Request[etcdserverpb.RangeRequest]) (*connect.Response[etcdserverpb.RangeResponse], error)
}

func (f *fakeKVHandler) Range(ctx context.Context, req *connect.Request[etcdserverpb.RangeRequest]) (*connect.Response[etcdserverpb.RangeResponse], error) {
	return f.rangeFunc(ctx, req)
}

// TestInformerRun ensures that Run resumes its watch without missing changes.
// When etcd has compacted the revision that the watch would resume from, Run
// lists the keys again until a list succeeds, sends the changes that it
// missed, and watches from the listed revision. When the watch's connection
// drops partway through etcd's catch-up, Run watches from after the last event
// that it received. Once Run returns, its watch goroutine has exited.
func TestInformerRun(t *testing.T) {
	// Not parallel, so that no other test's watch goroutine is running
	// when it checks that Run's has exited.

	kv := func(key string, modRevision int64) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), ModRevision: modRevision}
	}

	// The fake etcd plays out this history of keys a to d:
	//
	//  1. The first list, at revision 10, finds a@2, b@3 and c@4, so Run
	//     adds all three and watches from 11.
	//  2. Before that watch starts, b is updated at 15, d is added at 16, c
	//     is deleted at 17, and etcd compacts at revision 18. So etcd
	//     cancels the watch, and Run lists the keys again.
	//  3. That list fails, so Run tries again.
	//  4. The next list, at revision 20, finds a@2, b@15 and d@16. Run
	//     updates b, adds d, and deletes c at the list's revision, 20, since
	//     the list can't tell when c was deleted. It sends nothing for a,
	//     which hasn't changed, and watches from 21.
	//  5. Meanwhile b is updated at 21, d at 22, and a is deleted at 23, and
	//     etcd is at revision 25. It sends the watch these changes in two
	//     batches, with 25 in each one's header: first b and d, then a. The
	//     connection drops after the first batch.
	//  6. Run watches again from 23, after the last event it received
	//     rather than after the header's 25, so it gets the deletion of a.
	//  7. The test cancels Run, which must leave no watch goroutine behind.
	//
	// Step 5's batches.
	batches := [][]*mvccpb.Event{
		{{Type: mvccpb.Event_PUT, Kv: kv("b", 21)}, {Type: mvccpb.Event_PUT, Kv: kv("d", 22)}},
		{{Type: mvccpb.Event_DELETE, Kv: kv("a", 23)}},
	}
	var mu sync.Mutex
	kvs := []*mvccpb.KeyValue{kv("a", 2), kv("b", 3), kv("c", 4)}
	revision := int64(10)
	var rangeCalls int
	var watchStarts []int64

	kvPath, kvHandler := etcdserverpbconnect.NewKVHandler(&fakeKVHandler{
		rangeFunc: func(ctx context.Context, req *connect.Request[etcdserverpb.RangeRequest]) (*connect.Response[etcdserverpb.RangeResponse], error) {
			mu.Lock()
			defer mu.Unlock()
			rangeCalls++
			// Step 3: the second list fails.
			if rangeCalls == 2 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("etcd is unavailable"))
			}
			return connect.NewResponse(&etcdserverpb.RangeResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: revision},
				Kvs:    kvs,
			}), nil
		},
	})
	watchPath, watchHandler := etcdserverpbconnect.NewWatchHandler(&fakeWatchHandler{
		watch: func(ctx context.Context, stream *connect.BidiStream[etcdserverpb.WatchRequest, etcdserverpb.WatchResponse]) error {
			req, err := stream.Receive()
			if err != nil {
				return err
			}
			create := req.GetCreateRequest()

			mu.Lock()
			watchStarts = append(watchStarts, create.StartRevision)
			watches := len(watchStarts)
			if watches == 1 {
				// Step 2: etcd's keys change before the first watch
				// starts.
				kvs = []*mvccpb.KeyValue{kv("a", 2), kv("b", 15), kv("d", 16)}
				revision = 20
			}
			header := &etcdserverpb.ResponseHeader{Revision: revision}
			mu.Unlock()

			if err := stream.Send(&etcdserverpb.WatchResponse{
				WatchId: create.WatchId,
				Created: true,
				Header:  header,
			}); err != nil {
				return err
			}

			if create.StartRevision < 18 {
				// Step 2: etcd has compacted the start revision, so like
				// etcd, cancel the watch in a second message.
				if err := stream.Send(&etcdserverpb.WatchResponse{
					WatchId:         create.WatchId,
					Header:          header,
					Canceled:        true,
					CompactRevision: 18,
				}); err != nil {
					return err
				}
			} else {
				// Steps 5 and 6: send the events from the start revision
				// on, and drop the second watch's connection after its
				// first batch.
				for _, batch := range batches {
					var events []*mvccpb.Event
					for _, ev := range batch {
						if ev.Kv.ModRevision >= create.StartRevision {
							events = append(events, ev)
						}
					}
					if len(events) == 0 {
						continue
					}
					if err := stream.Send(&etcdserverpb.WatchResponse{
						WatchId: create.WatchId,
						Header:  &etcdserverpb.ResponseHeader{Revision: 25},
						Events:  events,
					}); err != nil {
						return err
					}
					if watches == 2 {
						return connect.NewError(connect.CodeUnavailable, errors.New("connection lost"))
					}
				}
			}

			<-ctx.Done()
			return ctx.Err()
		},
	})

	mux := http.NewServeMux()
	mux.Handle(kvPath, kvHandler)
	mux.Handle(watchPath, watchHandler)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	t.Cleanup(srv.Close)

	var eventsMu sync.Mutex
	var events []string
	deletedA := make(chan struct{})
	record := func(op string) func(*mvccpb.KeyValue) {
		return func(kv *mvccpb.KeyValue) {
			eventsMu.Lock()
			defer eventsMu.Unlock()
			events = append(events, fmt.Sprintf("%s %s@%d", op, kv.Key, kv.ModRevision))
			if op == "delete" && string(kv.Key) == "a" {
				close(deletedA)
			}
		}
	}
	inf := &Informer{
		// Don't let the client retry step 3's failed list itself, so that
		// Run sees the failure.
		Client: etcd.NewClient(etcd.Config{
			Endpoints:    []string{srv.Listener.Addr().String()},
			RetryOptions: etcd.NoRetry,
		}),
		Key:        []byte("a"),
		RangeEnd:   []byte("z"),
		AddFunc:    record("add"),
		UpdateFunc: record("update"),
		DeleteFunc: record("delete"),
	}

	// Make sure the test doesn't take forever.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Wait for the last change, step 6's deletion of a.
	runErr := make(chan error, 1)
	go func() { runErr <- inf.Run(ctx) }()
	select {
	case <-deletedA:
	case <-ctx.Done():
		eventsMu.Lock()
		defer eventsMu.Unlock()
		t.Fatalf("watch events never arrived (%v); events: %q", ctx.Err(), events)
	}
	// Step 7. The check finds the watch goroutine by its function's name,
	// so first make sure that it finds it while the watch runs.
	watching := func() (bool, string) {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		return strings.Contains(stacks, "cache.(*Informer).stream"), stacks
	}
	if ok, stacks := watching(); !ok {
		t.Fatalf("no watch goroutine found while Run is running:\n%s", stacks)
	}
	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}
	for start := time.Now(); ; time.Sleep(10 * time.Millisecond) {
		ok, stacks := watching()
		if !ok {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("watch goroutine still running after Run returned:\n%s", stacks)
		}
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	// Each list sends its events in map order, so sort those.
	want := [][]string{
		// Step 1: the first list adds every key.
		{"add a@2", "add b@3", "add c@4"},
		// Step 4: the second successful list adds d, deletes c and
		// updates b, but sends nothing for a.
		{"add d@16", "delete c@20", "update b@15"},
		// Steps 5 and 6: the watch updates b and d, then deletes a, each
		// once, in order.
		{"update b@21", "update d@22", "delete a@23"},
	}
	if len(events) != 9 {
		t.Fatalf("events = %q, want %q", events, want)
	}
	got := [][]string{events[:3], events[3:6], events[6:]}
	slices.Sort(got[0])
	slices.Sort(got[1])
	for n := range want {
		if !slices.Equal(got[n], want[n]) {
			t.Fatalf("events = %q, want %q", got, want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	// Steps 1, 3 and 4. The dropped connection doesn't list again.
	if rangeCalls != 3 {
		t.Errorf("Range calls = %d, want 3", rangeCalls)
	}
	// After step 1's list, after step 4's, and in step 6 after the last
	// event received.
	if want := []int64{11, 21, 23}; !slices.Equal(watchStarts, want) {
		t.Errorf("watch start revisions = %v, want %v", watchStarts, want)
	}

	// After step 6, the cache matches etcd: a and c are gone, and b and d
	// are at their latest revisions.
	for key, want := range map[string]int64{"a": 0, "b": 21, "c": 0, "d": 22} {
		kv, ok := inf.Get([]byte(key))
		switch {
		case want == 0 && ok:
			t.Errorf("Get(%q) = %v, want no key", key, kv)
		case want != 0 && (!ok || kv.ModRevision != want):
			t.Errorf("Get(%q) = %v, %v, want revision %d", key, kv, ok, want)
		}
	}
}
