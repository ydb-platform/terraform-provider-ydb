package externaldatasource

import (
	"context"
	"fmt"
	"net"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/stretchr/testify/require"
	discovery "github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	queryv1 "github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	scheme "github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	tablev1 "github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

const wireToken = "synthetic-caller-'\\-token"

type wireDB struct {
	mu             sync.Mutex
	addr           string
	secrets        map[string]bool
	exists         bool
	properties     map[string]string
	queries        []string
	loseReply      string
	describeStatus Ydb.StatusIds_StatusCode
	listStatus     Ydb.StatusIds_StatusCode
	listPaths      []string
	pathEntryType  Ydb_Scheme.Entry_Type
}

func newWireDB(t *testing.T) *wireDB {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &wireDB{addr: lis.Addr().String(), secrets: map[string]bool{}}
	server := grpc.NewServer()
	discovery.RegisterDiscoveryServiceServer(server, &wireDiscovery{f: f})
	queryv1.RegisterQueryServiceServer(server, &wireQuery{f: f})
	tablev1.RegisterTableServiceServer(server, &wireTable{f: f})
	scheme.RegisterSchemeServiceServer(server, &wireScheme{f: f})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return f
}

func wireOperation(m proto.Message) *Ydb_Operations.Operation {
	a, _ := anypb.New(m)
	return &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: a}
}

type wireDiscovery struct {
	discovery.UnimplementedDiscoveryServiceServer
	f *wireDB
}

func (s *wireDiscovery) ListEndpoints(context.Context, *Ydb_Discovery.ListEndpointsRequest) (*Ydb_Discovery.ListEndpointsResponse, error) {
	host, port, _ := net.SplitHostPort(s.f.addr)
	p, _ := strconv.Atoi(port)
	return &Ydb_Discovery.ListEndpointsResponse{Operation: wireOperation(&Ydb_Discovery.ListEndpointsResult{Endpoints: []*Ydb_Discovery.EndpointInfo{{Address: host, Port: uint32(p), NodeId: 1}}})}, nil
}

type wireQuery struct {
	queryv1.UnimplementedQueryServiceServer
	f *wireDB
}

func (s *wireQuery) CreateSession(context.Context, *Ydb_Query.CreateSessionRequest) (*Ydb_Query.CreateSessionResponse, error) {
	return &Ydb_Query.CreateSessionResponse{Status: Ydb.StatusIds_SUCCESS, SessionId: "ydb://session?node_id=1&id=test", NodeId: 1}, nil
}

func (s *wireQuery) DeleteSession(context.Context, *Ydb_Query.DeleteSessionRequest) (*Ydb_Query.DeleteSessionResponse, error) {
	return &Ydb_Query.DeleteSessionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (s *wireQuery) AttachSession(_ *Ydb_Query.AttachSessionRequest, stream queryv1.QueryService_AttachSessionServer) error {
	if err := stream.Send(&Ydb_Query.SessionState{Status: Ydb.StatusIds_SUCCESS}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

var wireProperty = regexp.MustCompile(`([A-Z_]+) = "([^"]*)"`)

func (s *wireQuery) ExecuteQuery(req *Ydb_Query.ExecuteQueryRequest, stream queryv1.QueryService_ExecuteQueryServer) error {
	f := s.f
	f.mu.Lock()
	defer f.mu.Unlock()
	q := req.GetQueryContent().GetText()
	f.queries = append(f.queries, q)
	code := Ydb.StatusIds_SUCCESS
	switch {
	case strings.HasPrefix(q, "DROP EXTERNAL DATA SOURCE"):
		f.exists = false
	case strings.Contains(q, "EXTERNAL DATA SOURCE"):
		if f.exists && !strings.HasPrefix(q, "CREATE OR REPLACE") {
			code = Ydb.StatusIds_ALREADY_EXISTS
			break
		}
		f.exists = true
		f.properties = map[string]string{}
		for _, kv := range wireProperty.FindAllStringSubmatch(q, -1) {
			f.properties[kv[1]] = kv[2]
		}
		if f.properties["AUTH_METHOD"] == "IAM" && f.properties["RESOURCE_ID"] == "" {
			f.properties["RESOURCE_ID"] = "resolved-target-cloud"
		}
	}
	if f.loseReply != "" && strings.Contains(q, f.loseReply) {
		f.loseReply = ""
		return status.Error(codes.Unavailable, "response lost after submission")
	}
	return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{Status: code})
}

type wireTable struct {
	tablev1.UnimplementedTableServiceServer
	f *wireDB
}

func (s *wireTable) DescribeExternalDataSource(context.Context, *Ydb_Table.DescribeExternalDataSourceRequest) (*Ydb_Table.DescribeExternalDataSourceResponse, error) {
	f := s.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.exists || f.describeStatus != Ydb.StatusIds_STATUS_CODE_UNSPECIFIED {
		code := f.describeStatus
		if code == Ydb.StatusIds_STATUS_CODE_UNSPECIFIED {
			code = Ydb.StatusIds_SCHEME_ERROR
		}
		return &Ydb_Table.DescribeExternalDataSourceResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: code}}, nil
	}
	src, loc := f.properties["SOURCE_TYPE"], f.properties["LOCATION"]
	props := map[string]string{}
	for k, v := range f.properties {
		props[k] = v
	}
	return &Ydb_Table.DescribeExternalDataSourceResponse{Operation: wireOperation(&Ydb_Table.DescribeExternalDataSourceResult{Self: &Ydb_Scheme.Entry{Name: "eds"}, SourceType: &src, Location: &loc, Properties: props})}, nil
}

type wireScheme struct {
	scheme.UnimplementedSchemeServiceServer
	f *wireDB
}

func (s *wireScheme) ListDirectory(_ context.Context, req *Ydb_Scheme.ListDirectoryRequest) (*Ydb_Scheme.ListDirectoryResponse, error) {
	f := s.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listPaths = append(f.listPaths, req.GetPath())
	if f.listStatus != Ydb.StatusIds_STATUS_CODE_UNSPECIFIED {
		return &Ydb_Scheme.ListDirectoryResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: f.listStatus}}, nil
	}
	children := []*Ydb_Scheme.Entry{}
	if f.exists {
		entryType := f.pathEntryType
		if entryType == Ydb_Scheme.Entry_TYPE_UNSPECIFIED {
			entryType = Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE
		}
		children = append(children, &Ydb_Scheme.Entry{Name: "eds", Type: entryType})
	}
	for p := range f.secrets {
		children = append(children, &Ydb_Scheme.Entry{Name: path.Base(p)})
	}
	return &Ydb_Scheme.ListDirectoryResponse{Operation: wireOperation(&Ydb_Scheme.ListDirectoryResult{Self: &Ydb_Scheme.Entry{Name: "owner"}, Children: children})}, nil
}

func wireCallback(context.Context) (auth.YdbCredentials, error) {
	return auth.YdbCredentials{Token: wireToken}, nil
}

// YDB's real Describe RPC maps both missing paths and generic scheme errors to
// SCHEME_ERROR. Only a successful exact-path absence check may remove IAM state.
func TestYDBIAMReadSchemeErrorExactAbsence(t *testing.T) {
	for _, tc := range []struct {
		name, auth                   string
		exists                       bool
		describeStatus, listStatus   Ydb.StatusIds_StatusCode
		entryType                    Ydb_Scheme.Entry_Type
		wantError, wantID, wantProbe bool
	}{
		{name: "missing IAM EDS", auth: "IAM", wantProbe: true},
		{name: "ambiguous scheme error with existing path", auth: "IAM", exists: true, describeStatus: Ydb.StatusIds_SCHEME_ERROR, wantError: true, wantID: true, wantProbe: true},
		{name: "scheme error with wrong-type path", auth: "IAM", exists: true, describeStatus: Ydb.StatusIds_SCHEME_ERROR, entryType: Ydb_Scheme.Entry_TABLE, wantError: true, wantID: true, wantProbe: true},
		{name: "missing path but listing denied", auth: "IAM", listStatus: Ydb.StatusIds_UNAUTHORIZED, wantError: true, wantID: true, wantProbe: true},
		{name: "Describe denied", auth: "IAM", describeStatus: Ydb.StatusIds_UNAUTHORIZED, wantError: true, wantID: true},
		{name: "legacy scheme absence behavior", auth: "NONE", listStatus: Ydb.StatusIds_UNAUTHORIZED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWireDB(t)
			f.exists = tc.exists
			f.describeStatus = tc.describeStatus
			f.listStatus = tc.listStatus
			f.pathEntryType = tc.entryType
			cfg := iamConfig()
			cfg["auth_method"] = tc.auth
			cfg["connection_string"] = fmt.Sprintf("grpc://%s/?database=/owner", f.addr)
			d := schema.TestResourceDataRaw(t, ResourceSchema(), cfg)
			id := cfg["connection_string"].(string) + "?path=eds"
			d.SetId(id)
			ctx, cancel := context.WithTimeout(context.Background(), 3_000_000_000)
			defer cancel()
			diags := ResourceReadFunc(wireCallback)(ctx, d, nil)
			require.Equal(t, tc.wantError, diags.HasError(), "%v", diags)
			if tc.wantID {
				require.Equal(t, id, d.Id())
			} else {
				require.Empty(t, d.Id())
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			require.Empty(t, f.queries, "Read must not issue bootstrap or EDS mutations")
			if tc.wantProbe {
				require.Equal(t, []string{"/owner"}, f.listPaths)
			} else {
				require.Empty(t, f.listPaths)
			}
		})
	}
}
