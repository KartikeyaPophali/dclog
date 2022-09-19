package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/kartpop/dclog/api/v1"
	"github.com/kartpop/dclog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	testFuncs := map[string]func(t *testing.T, client api.LogClient, config *Config){
		"produce/consume to/from log succeeds": testProduceConsume,
		"produce/consume stream succeeds":      testProduceConsumeStream,
		"consume past log boundary fails":      testConsumePastBoundary,
	}
	for testCase, fn := range testFuncs {
		t.Run(testCase, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil) // TODO: why have 2nd param if nil is always passed
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	// test Produce
	ctx := context.Background()
	record := &api.Record{
		Value: []byte("hello world"),
	}
	proreq := &api.ProduceRequest{
		Record: record,
	}
	prores, err := client.Produce(ctx, proreq)
	require.NoError(t, err)

	// test Consume
	conreq := &api.ConsumeRequest{
		Offset: prores.Offset,
	}
	conres, err := client.Consume(ctx, conreq)
	require.NoError(t, err)
	require.Equal(t, record.Value, conres.Record.Value)
	require.Equal(t, prores.Offset, conres.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	proreq := &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	}
	prores, err := client.Produce(ctx, proreq)
	require.NoError(t, err)

	conreq := &api.ConsumeRequest{
		Offset: prores.Offset + 1,
	}
	conres, err := client.Consume(ctx, conreq)
	if conres != nil {
		t.Fatal("consume not nil")
	}
	goterr := grpc.Code(err)
	wanterr := grpc.Code(api.ErrorOffsetOutOfRange{}.GRPCStatus().Err())
	if goterr != wanterr {
		t.Fatalf("got err: %v, want err: %v", goterr, wanterr)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Value: []byte("hey")},
		{Value: []byte("good day!")},
		{Value: []byte("bye.")},
	}

	offsets := []uint64{}
	recmap := map[uint64]*api.Record{}

	prodstream, err := client.ProduceStream(ctx)
	require.NoError(t, err)
	for _, rec := range records {
		err = prodstream.Send(&api.ProduceRequest{Record: rec})
		require.NoError(t, err)
		res, err := prodstream.Recv()
		require.NoError(t, err)
		offsets = append(offsets, res.Offset)
		recmap[res.Offset] = rec
	}

	constream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: offsets[0]})
	require.NoError(t, err)
	for i := 0; i < len(offsets); i++ {
		res, err := constream.Recv()
		require.NoError(t, err)
		record, ok := recmap[res.Record.Offset]
		if !ok {
			t.Fatalf("did not find offset %v in log, but should be present", res.Record.Offset)
		}
		require.Equal(t, record.Value, res.Record.Value)
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, config *Config, teardown func()) {
	t.Helper()

	// setup client
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	clientConn, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)
	client = api.NewLogClient(clientConn)

	// setup server
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)
	config = &Config{
		CommitLog: clog,
	}
	if fn != nil { // fn is always nil, seems unnecessary
		fn(config)
	}
	server, err := NewGRPCServer(config)
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	return client, config, func() {
		server.Stop()
		clientConn.Close()
		listener.Close()
		clog.Remove()
	}
}
