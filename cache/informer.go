package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	client "go.withmatt.com/connect-etcd"
	"go.withmatt.com/connect-etcd/internal/retry"
	"go.withmatt.com/connect-etcd/types/etcdserverpb"
	"go.withmatt.com/connect-etcd/types/mvccpb"
)

type Informer struct {
	Client   *client.Client
	Key      []byte
	RangeEnd []byte

	ListPageSize int64

	AddFunc    func(*mvccpb.KeyValue)
	UpdateFunc func(*mvccpb.KeyValue)
	DeleteFunc func(*mvccpb.KeyValue)

	cacheLock sync.RWMutex
	cache     map[string]*mvccpb.KeyValue

	streamsAttempted atomic.Int64
	lastRevision     atomic.Int64
	totalKeys        uint64

	syncDoneMu sync.Mutex
	syncDone   chan error
}

func nextKey(key []byte) []byte {
	// this is the key + NULL
	end := make([]byte, len(key)+1)
	copy(end, key)
	return end
}

func (i *Informer) LastRevision() int64 {
	return i.lastRevision.Load()
}

func (i *Informer) StreamsAttempted() int64 {
	return i.streamsAttempted.Load()
}

func (i *Informer) addFunc(kv *mvccpb.KeyValue) {
	i.totalKeys++
	if i.AddFunc != nil {
		i.AddFunc(kv)
	}
}

func (i *Informer) updateFunc(kv *mvccpb.KeyValue) {
	if i.UpdateFunc != nil {
		i.UpdateFunc(kv)
	}
}

func (i *Informer) deleteFunc(kv *mvccpb.KeyValue) {
	i.totalKeys--
	if i.DeleteFunc != nil {
		i.DeleteFunc(kv)
	}
}

func (i *Informer) Get(key []byte) (*mvccpb.KeyValue, bool) {
	i.cacheLock.RLock()
	defer i.cacheLock.RUnlock()

	kv, ok := i.cache[string(key)]
	return kv, ok
}

func (i *Informer) List() [][]byte {
	i.cacheLock.RLock()
	defer i.cacheLock.RUnlock()

	l := make([][]byte, 0, len(i.cache))
	for _, v := range i.cache {
		l = append(l, v.Key)
	}
	return l
}

type CompactedError struct {
	Reason            string
	CompactedRevision int64
	RequestedRevision int64
}

func (e *CompactedError) Error() string {
	return fmt.Sprintf("informer: stream canceled: %q (compacted revision %d > start revision %d)", e.Reason, e.CompactedRevision, e.RequestedRevision)
}

func streamHeaderCanceledError(reason string, compactedRevision, startRevision int64) error {
	if compactedRevision > startRevision {
		return &CompactedError{
			Reason:            reason,
			CompactedRevision: compactedRevision,
			RequestedRevision: startRevision,
		}
	}
	return fmt.Errorf("informer: stream canceled: %s", reason)
}

const defaultPageSize = 100

// load lists the keys at the current revision into a new cache, and swaps it
// for the old one. It calls AddFunc, UpdateFunc and DeleteFunc, in no
// particular order, for the differences between the two, so that consumers
// end up with the new cache, and the watch continues from the listed revision.
func (i *Informer) load(ctx context.Context) error {
	l := i.Client.Logger()

	start := time.Now()
	cache := make(map[string]*mvccpb.KeyValue)

	pageSize := i.ListPageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	startKey := i.Key

	var lastRevision int64
	var limit int64
	var serializable bool

	for {
		if l.CheckDebug() {
			l.Debug("Range",
				"key", string(startKey),
				"revision", lastRevision,
			)
		}

		// when lastRevision is 0, this is the first page we're requesting.
		// for the first page, we want to do a small strongly consistent read
		// (not serializable means linearizable read). We want to make a
		// smaller read against the primary node to ensure we have a good
		// starting state for pagination. Once we have a revision, we can
		// serve the rest of the pages from a replica with an explicit
		// revision.
		if lastRevision == 0 {
			limit = min(pageSize, defaultPageSize)
			serializable = false
		} else {
			limit = pageSize
			serializable = true
		}

		resp, err := i.Client.KV().Range(ctx, connect.NewRequest(&etcdserverpb.RangeRequest{
			SortOrder:    etcdserverpb.RangeRequest_ASCEND,
			SortTarget:   etcdserverpb.RangeRequest_KEY,
			Key:          startKey,
			RangeEnd:     i.RangeEnd,
			Limit:        limit,
			Revision:     lastRevision,
			Serializable: serializable,
		}))
		if err != nil {
			return err
		}
		msg := resp.Msg

		if lastRevision == 0 {
			lastRevision = msg.Header.Revision
		}

		for _, kv := range msg.Kvs {
			cache[string(kv.Key)] = kv
		}

		if !msg.More {
			break
		}

		startKey = nextKey(msg.Kvs[len(msg.Kvs)-1].Key)
	}

	// Swap the caches and send out the differences under the lock, as the
	// watch does, so that readers never see the new cache before the
	// callbacks have.
	i.cacheLock.Lock()
	defer i.cacheLock.Unlock()

	old := i.cache
	i.cache = cache
	i.lastRevision.Store(lastRevision)

	for key, kv := range cache {
		prev, ok := old[key]
		switch {
		case !ok:
			i.addFunc(kv)
		case prev.ModRevision != kv.ModRevision:
			i.updateFunc(kv)
		}
	}
	for key, kv := range old {
		if _, ok := cache[key]; !ok {
			// Like a watch's DELETE event, pass only the key and a
			// revision at which it was deleted.
			i.deleteFunc(&mvccpb.KeyValue{Key: kv.Key, ModRevision: lastRevision})
		}
	}

	if l.CheckInfo() {
		l.Info(
			"informer sync finished",
			"total_keys", i.totalKeys,
			"revision", lastRevision,
			"duration", time.Since(start),
		)
	}

	return nil
}

func (i *Informer) WaitForCacheSync(ctx context.Context) error {
	i.syncDoneMu.Lock()
	if i.syncDone == nil {
		i.syncDone = make(chan error)
	}
	i.syncDoneMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-i.syncDone:
		return err
	}
}

func (i *Informer) Run(ctx context.Context) error {
	i.syncDoneMu.Lock()
	if i.syncDone == nil {
		i.syncDone = make(chan error)
	}
	i.syncDoneMu.Unlock()

	l := i.Client.Logger()

	if err := i.load(ctx); err != nil {
		i.syncDone <- err
		close(i.syncDone)
		return err
	}

	close(i.syncDone)

	// we explicitly want the stream to retry forever reconnecting
	// outside of the Unary interceptor retry intervals
	resync := false
	return retry.Forever(ctx, l,
		1*time.Second, 0.5,
		func() error {
			// etcd has compacted the revision that the watch would
			// resume from, so list the keys again and watch from there.
			if resync {
				if err := i.load(ctx); err != nil {
					return err
				}
			}
			err := i.stream(ctx)
			var compacted *CompactedError
			resync = errors.As(err, &compacted)
			return err
		},
	)
}

func (i *Informer) stream(ctx context.Context) error {
	// Ensure the watch stream is fully shut down when this func returns.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	watchId := rand.Int64()
	l := i.Client.Logger()

	stream := i.Client.Watch().Watch(ctx)
	defer stream.CloseRequest()

	i.streamsAttempted.Add(1)

	startRevision := i.lastRevision.Load() + 1

	if err := stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				WatchId:       watchId,
				StartRevision: startRevision,
				Key:           i.Key,
				RangeEnd:      i.RangeEnd,

				ProgressNotify: true,
			},
		},
	}); err != nil {
		return err
	}

	msg, err := stream.Receive()
	if err != nil {
		return err
	}

	if msg.WatchId != watchId {
		return errors.New("informer: unexpected watch id, aborting stream")
	}

	if !msg.Created {
		return errors.New("informer: unexpected watch message, expected CreateResponse")
	}

	if msg.Canceled {
		return streamHeaderCanceledError(msg.CancelReason, msg.CompactRevision, startRevision)
	}

	if l.CheckDebug() {
		l.Debug("stream started",
			"watch_id", watchId,
			"cluster_id", msg.Header.ClusterId,
			"member_id", msg.Header.MemberId,
			"revision", msg.Header.Revision,
			"raft_term", msg.Header.RaftTerm,
		)
	}
	lastRevision := msg.Header.Revision

	errCh := make(chan error)

	go func() {
		defer close(errCh)

		for {
			msg, err := stream.Receive()
			if err != nil {
				errCh <- err
				return
			}

			if msg.WatchId != watchId {
				errCh <- errors.New("informer: unexpected watch id, aborting stream")
				return
			}

			if msg.Canceled {
				// etcd confirms a watch that starts at a compacted
				// revision, then cancels it in a later message.
				if msg.CompactRevision != 0 {
					errCh <- &CompactedError{
						Reason:            msg.CancelReason,
						CompactedRevision: msg.CompactRevision,
						RequestedRevision: startRevision,
					}
					return
				}
				errCh <- fmt.Errorf("informer: stream canceled: %q", msg.CancelReason)
				return
			}

			if l.CheckDebug() {
				l.Debug("receive message",
					"watch_id", watchId,
					"cluster_id", msg.Header.ClusterId,
					"member_id", msg.Header.MemberId,
					"revision", msg.Header.Revision,
					"raft_term", msg.Header.RaftTerm,
					"last_revision", lastRevision,
					"events", len(msg.Events),
					"fragment", msg.Fragment,
				)
			}

			if msg.Header.Revision < lastRevision {
				errCh <- fmt.Errorf("informer: older revision observed: %d -> %d", lastRevision, msg.Header.Revision)
				return
			}

			for _, event := range msg.Events {
				i.cacheLock.Lock()
				switch event.Type {
				case mvccpb.Event_PUT:
					_, ok := i.cache[string(event.Kv.Key)]
					i.cache[string(event.Kv.Key)] = event.Kv
					if ok {
						i.updateFunc(event.Kv)
					} else {
						i.addFunc(event.Kv)
					}
				case mvccpb.Event_DELETE:
					delete(i.cache, string(event.Kv.Key))
					i.deleteFunc(event.Kv)
				}
				i.cacheLock.Unlock()
			}

			lastRevision = msg.Header.Revision
			// etcd sends a watch that is behind in batches of up to 1000
			// revisions, each with its current revision. Resume after the
			// last event rather than the header's revision, in case the
			// rest of the batches never arrive. This relies on the watch
			// not asking for fragments, which can split a revision.
			resumeRevision := lastRevision
			if n := len(msg.Events); n > 0 {
				resumeRevision = msg.Events[n-1].Kv.ModRevision
			}
			i.lastRevision.Store(resumeRevision)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
