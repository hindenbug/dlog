package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/hindenbug/dlog/api/log/v1"
	"github.com/hindenbug/dlog/internal/auth"
	"github.com/hindenbug/dlog/internal/config"
	"github.com/hindenbug/dlog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"unauthorized requests fails":                         testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), clientOptions...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)

		return conn, client, clientOptions
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() { server.Serve(l) }()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		//clog.Remove()
	}

}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	record := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: record})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, record.Value, consume.Record.Value)
	require.Equal(t, record.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("hello world")},
	})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})

	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	record := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != record {
		t.Fatalf("got err: %v, record: %v", got, record)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value: []byte("second message"), Offset: 1,
	}}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err := stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, record: %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})

		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value: record.Value, Offset: uint64(i),
			})
		}
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)

	if produce != nil {
		t.Fatalf("prioduce response should be nil")
	}

	statusCode, expectedCode := status.Code(err), codes.PermissionDenied
	if statusCode != expectedCode {
		t.Fatalf("got code: %d, expected: %d", statusCode, expectedCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}

	statusCode, expectedCode = status.Code(err), codes.PermissionDenied
	if statusCode != expectedCode {
		t.Fatalf("got code: %d, expected: %d", statusCode, expectedCode)
	}
}
