package cache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
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
// that it received.
func TestInformerRun(t *testing.T) {
	t.Parallel()

	kv := func(key string, modRevision int64) *mvccpb.KeyValue {
		return &mvccpb.KeyValue{Key: []byte(key), ModRevision: modRevision}
	}

	// The fake etcd starts with a, b and c at revision 10. When the first
	// watch starts, it has missed an update to b, the deletion of c and the
	// addition of d, and revision 18 has been compacted. The first list after
	// that fails.
	//
	// Like etcd, it sends the changes after the list to a watch that is
	// behind in batches, each with its current revision, 25. The connection
	// of the first watch after the list drops after the first batch.
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
				// Like etcd, cancel the watch in a second message.
				if err := stream.Send(&etcdserverpb.WatchResponse{
					WatchId:         create.WatchId,
					Header:          header,
					Canceled:        true,
					CompactRevision: 18,
				}); err != nil {
					return err
				}
			} else {
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
		// Don't let the client retry the failed list itself.
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

	runErr := make(chan error, 1)
	go func() { runErr <- inf.Run(ctx) }()
	select {
	case <-deletedA:
	case <-ctx.Done():
		eventsMu.Lock()
		defer eventsMu.Unlock()
		t.Fatalf("watch events never arrived (%v); events: %q", ctx.Err(), events)
	}
	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	// Each sync sends its events in map order.
	want := [][]string{
		{"add a@2", "add b@3", "add c@4"},
		{"add d@16", "delete c@20", "update b@15"},
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
	if rangeCalls != 3 {
		t.Errorf("Range calls = %d, want 3", rangeCalls)
	}
	if want := []int64{11, 21, 23}; !slices.Equal(watchStarts, want) {
		t.Errorf("watch start revisions = %v, want %v", watchStarts, want)
	}

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
