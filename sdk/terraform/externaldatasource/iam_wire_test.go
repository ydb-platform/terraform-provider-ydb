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
	schemev1 "github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	tablev1 "github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/terraform-provider-ydb/sdk/terraform/auth"
)

func TestIAMCreateUsesCallerToken(t *testing.T) {
	f := newIAMWireDB(t)
	const token = "synthetic-caller-'\\-token"
	d := iamWireData(t, f)
	calls := 0
	cb := func(context.Context) (auth.YdbCredentials, error) {
		calls++
		return auth.YdbCredentials{Token: token}, nil
	}
	diags := ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	require.NotEmpty(t, d.Id())
	require.Equal(t, 1, calls, "transport and bootstrap must use one credential snapshot")

	f.mu.Lock()
	defer f.mu.Unlock()
	require.True(t, f.exists)
	require.Empty(t, f.secrets, "the caller token secret must be removed after the EDS is created")
	require.Equal(t, "IAM", f.properties["AUTH_METHOD"])
	require.Equal(t, "runtime-sa", f.properties["SERVICE_ACCOUNT_ID"])
	secretPath := f.properties["INITIAL_TOKEN_SECRET_PATH"]
	require.NotEmpty(t, secretPath)
	var created *iamWireRequest
	for i := range f.requests {
		r := &f.requests[i]
		if strings.Contains(r.query.GetQueryContent().GetText(), "CREATE SECRET `"+secretPath+"`") {
			created = r
			break
		}
	}
	require.NotNil(t, created, "the EDS must reference the secret created for this request")
	require.Equal(t, Ydb.StatusIds_SUCCESS, created.status)
	require.Equal(t, token, created.ticket)
	value := iamWireSecretValue.FindStringSubmatch(created.query.GetQueryContent().GetText())
	require.Len(t, value, 2)
	decoded, err := strconv.Unquote(value[1])
	require.NoError(t, err)
	require.Equal(t, created.ticket, decoded)
	require.NotContains(t, fmt.Sprint(d.State(), diags), token)
}

func iamWireData(t *testing.T, f *iamWireDB) *schema.ResourceData {
	t.Helper()
	return schema.TestResourceDataRaw(t, ResourceSchema(), map[string]interface{}{
		"connection_string":  fmt.Sprintf("grpc://%s/?database=/owner", f.addr),
		"path":               "eds",
		"source_type":        "Ydb",
		"location":           "target:2135",
		"database_name":      "/target",
		"auth_method":        "IAM",
		"service_account_id": "runtime-sa",
	})
}

func TestIAMUpdateUsesCurrentCallerToken(t *testing.T) {
	f := newIAMWireDB(t)
	d := iamWireData(t, f)
	token := "first-caller-token"
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: token}, nil
	}
	require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
	id := d.Id()
	token = "current-caller-token"
	require.NoError(t, d.Set("service_account_id", "updated-runtime-sa"))
	diags := ResourceUpdateFunc(cb)(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	require.Equal(t, id, d.Id())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.secrets)
	require.Equal(t, "updated-runtime-sa", f.properties["SERVICE_ACCOUNT_ID"])
	require.Equal(t, token, f.requests[len(f.requests)-1].ticket)
}

func TestIAMCreatePreservesForeignEDS(t *testing.T) {
	f := newIAMWireDB(t)
	f.exists = true
	f.properties = map[string]string{"SOURCE_TYPE": "Ydb", "AUTH_METHOD": "NONE", "LOCATION": "foreign"}
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	diags := ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.True(t, diags.HasError(), "%v", diags)
	require.Empty(t, d.Id())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, "foreign", f.properties["LOCATION"])
	require.Empty(t, f.secrets)
}

func TestIAMUnknownUpdateRetainsSecretAndBlocksOperations(t *testing.T) {
	f := newIAMWireDB(t)
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
	id := d.Id()
	f.mu.Lock()
	f.loseReply = "EXTERNAL DATA SOURCE"
	f.mu.Unlock()
	diags := ResourceUpdateFunc(cb)(context.Background(), d, nil)
	require.True(t, diags.HasError())
	require.Equal(t, id, d.Id())
	require.Equal(t, "UNKNOWN", d.Get("pending_iam_mutation"))
	f.mu.Lock()
	require.Len(t, f.secrets, 1, "the server may still need this secret")
	requests := len(f.requests)
	f.mu.Unlock()
	require.True(t, ResourceReadFunc(cb)(context.Background(), d, nil).HasError())
	require.True(t, ResourceUpdateFunc(cb)(context.Background(), d, nil).HasError())
	require.True(t, ResourceDeleteFunc(cb)(context.Background(), d, nil).HasError())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.requests, requests)
}

func TestIAMRejectedUpdateDoesNotFreezeEDS(t *testing.T) {
	f := newIAMWireDB(t)
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
	f.mu.Lock()
	f.mutationError = status.Error(codes.PermissionDenied, "request rejected")
	f.mu.Unlock()
	diags := ResourceUpdateFunc(cb)(context.Background(), d, nil)
	require.True(t, diags.HasError())
	require.Empty(t, d.Get("pending_iam_mutation"), "a terminal rejection has a known outcome")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.secrets, "terminal rejection no longer needs the owned bootstrap secret")
}

func TestIAMCleanupFailureRetainsIdentityAndRecoversOnRead(t *testing.T) {
	f := newIAMWireDB(t)
	f.failDrop = true
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	diags := ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	require.NotEmpty(t, diags, "cleanup failure must be visible as a warning")
	id := d.Id()
	require.NotEmpty(t, id)
	f.mu.Lock()
	require.Len(t, f.secrets, 1)
	require.Equal(t, f.properties["INITIAL_TOKEN_SECRET_PATH"], d.Get("pending_cleanup_path"))
	f.failDrop = false
	requests := len(f.requests)
	f.mu.Unlock()
	require.False(t, ResourceReadFunc(cb)(context.Background(), d, nil).HasError())
	require.Equal(t, id, d.Id())
	require.Empty(t, d.Get("pending_cleanup_path"))
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.secrets)
	require.Len(t, f.requests, requests+1, "refresh must only remove the owned temporary secret")
}

func TestIAMAcknowledgedCreateRetainsIdentityWhenReadBackFails(t *testing.T) {
	f := newIAMWireDB(t)
	f.describeStatus = Ydb.StatusIds_SCHEME_ERROR
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	diags := ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	require.NotEmpty(t, d.Id(), "an acknowledged Create owns the EDS even when Describe fails")
	require.NotEmpty(t, diags, "read-back failure must be visible as a warning")
}

func TestIAMCleanupAcceptsVerifiedAbsence(t *testing.T) {
	f := newIAMWireDB(t)
	f.loseReply = "DROP SECRET"
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	diags := ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.False(t, diags.HasError(), "%v", diags)
	require.Empty(t, d.Get("pending_cleanup_path"), "confirmed absence completes cleanup despite a lost DROP reply")
	require.Empty(t, diags)
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.secrets)
}

func TestIAMReadRequiresVerifiedAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "existing-path", "listing-denied"} {
		t.Run(mode, func(t *testing.T) {
			f := newIAMWireDB(t)
			d := iamWireData(t, f)
			cb := func(context.Context) (auth.YdbCredentials, error) {
				return auth.YdbCredentials{Token: "caller"}, nil
			}
			require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
			id := d.Id()
			f.mu.Lock()
			f.describeStatus = Ydb.StatusIds_SCHEME_ERROR
			f.exists = mode == "existing-path"
			if mode == "listing-denied" {
				f.listStatus = Ydb.StatusIds_UNAUTHORIZED
			}
			f.mu.Unlock()
			diags := ResourceReadFunc(cb)(context.Background(), d, nil)
			if mode == "absent" {
				require.False(t, diags.HasError(), "%v", diags)
				require.Empty(t, d.Id())
			} else {
				require.True(t, diags.HasError(), "an ambiguous Describe failure must preserve ownership")
				require.Equal(t, id, d.Id())
			}
		})
	}
}

func TestIAMCreateRejectsAlternateCredentials(t *testing.T) {
	for _, key := range []string{"service_account_id", "token_secret_path", "service_account_secret_path", "password_secret_path"} {
		t.Run(key, func(t *testing.T) {
			f := newIAMWireDB(t)
			d := iamWireData(t, f)
			value := "/foreign/secret"
			if key == "service_account_id" {
				value = ""
			}
			require.NoError(t, d.Set(key, value))
			cb := func(context.Context) (auth.YdbCredentials, error) {
				return auth.YdbCredentials{Token: "caller"}, nil
			}
			require.True(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
			require.Empty(t, d.Id())
			f.mu.Lock()
			defer f.mu.Unlock()
			require.False(t, f.exists)
			require.Empty(t, f.secrets)
		})
	}
}

func TestIAMRequiresCallerTokenBeforeConnecting(t *testing.T) {
	f := newIAMWireDB(t)
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{}, nil
	}
	diags := ResourceCreateFunc(cb)(context.Background(), d, nil)
	require.True(t, diags.HasError())
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.discoveryCalls, "IAM without a caller token must fail before contacting YDB")
}

func TestIAMExitWithUnknownOutcomeRetainsGuard(t *testing.T) {
	f := newIAMWireDB(t)
	d := iamWireData(t, f)
	cb := func(context.Context) (auth.YdbCredentials, error) {
		return auth.YdbCredentials{Token: "caller"}, nil
	}
	require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
	r := &schema.Resource{Schema: ResourceSchema(), CustomizeDiff: CustomizeDiff, UpdateContext: schema.UpdateContextFunc(ResourceUpdateFunc(cb))}
	cfg := map[string]interface{}{
		"connection_string": d.Get("connection_string"), "path": "eds", "source_type": "Ydb",
		"location": "target:2135", "database_name": "/target", "auth_method": "NONE",
	}
	previous := d.State()
	diff, err := iamResourceDiff(r, previous, cfg)
	require.NoError(t, err)
	f.mu.Lock()
	f.loseReply = "EXTERNAL DATA SOURCE"
	f.mu.Unlock()
	state, diags := r.Apply(context.Background(), previous, diff, nil)
	require.True(t, diags.HasError())
	require.Equal(t, d.Id(), state.ID)
	require.Equal(t, "UNKNOWN", state.Attributes["pending_iam_mutation"])
}

func TestIAMUnknownSecretCreateDoesNotFreezeEDS(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(fmt.Sprintf("update=%t", update), func(t *testing.T) {
			f := newIAMWireDB(t)
			d := iamWireData(t, f)
			cb := func(context.Context) (auth.YdbCredentials, error) {
				return auth.YdbCredentials{Token: "caller"}, nil
			}
			if update {
				require.False(t, ResourceCreateFunc(cb)(context.Background(), d, nil).HasError())
			}
			id := d.Id()
			f.mu.Lock()
			f.loseReply = "CREATE SECRET"
			before := len(f.requests)
			f.mu.Unlock()
			var diags interface{}
			if update {
				diags = ResourceUpdateFunc(cb)(context.Background(), d, nil)
			} else {
				diags = ResourceCreateFunc(cb)(context.Background(), d, nil)
			}
			require.Equal(t, id, d.Id())
			require.Empty(t, d.Get("pending_iam_mutation"), "EDS mutation was never submitted")
			f.mu.Lock()
			defer f.mu.Unlock()
			require.Len(t, f.requests, before+1)
			require.Len(t, f.secrets, 1)
			for p := range f.secrets {
				require.Contains(t, fmt.Sprint(diags), p, "unconfirmed secret path is required for manual recovery")
			}
		})
	}
}

type iamWireRequest struct {
	query  *Ydb_Query.ExecuteQueryRequest
	ticket string
	status Ydb.StatusIds_StatusCode
}

type iamWireDB struct {
	mu             sync.Mutex
	addr           string
	secrets        map[string]string
	exists         bool
	properties     map[string]string
	requests       []iamWireRequest
	loseReply      string
	failDrop       bool
	describeStatus Ydb.StatusIds_StatusCode
	discoveryCalls int
	listStatus     Ydb.StatusIds_StatusCode
	discoveryError error
	describeError  error
	deleteError    error
	mutationError  error
}

func newIAMWireDB(t *testing.T) *iamWireDB {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &iamWireDB{addr: lis.Addr().String(), secrets: map[string]string{}}
	server := grpc.NewServer()
	discovery.RegisterDiscoveryServiceServer(server, &iamWireDiscovery{f: f})
	queryv1.RegisterQueryServiceServer(server, &iamWireQuery{f: f})
	tablev1.RegisterTableServiceServer(server, &iamWireTable{f: f})
	schemev1.RegisterSchemeServiceServer(server, &iamWireScheme{f: f})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return f
}

func iamWireOperation(m proto.Message) *Ydb_Operations.Operation {
	a, _ := anypb.New(m)
	return &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: a}
}

type iamWireDiscovery struct {
	discovery.UnimplementedDiscoveryServiceServer
	f *iamWireDB
}

func (s *iamWireDiscovery) ListEndpoints(context.Context, *Ydb_Discovery.ListEndpointsRequest) (*Ydb_Discovery.ListEndpointsResponse, error) {
	s.f.mu.Lock()
	s.f.discoveryCalls++
	err := s.f.discoveryError
	s.f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	host, port, _ := net.SplitHostPort(s.f.addr)
	p, _ := strconv.Atoi(port)
	return &Ydb_Discovery.ListEndpointsResponse{Operation: iamWireOperation(&Ydb_Discovery.ListEndpointsResult{Endpoints: []*Ydb_Discovery.EndpointInfo{{Address: host, Port: uint32(p), NodeId: 1}}})}, nil
}

type iamWireQuery struct {
	queryv1.UnimplementedQueryServiceServer
	f *iamWireDB
}

func (s *iamWireQuery) CreateSession(context.Context, *Ydb_Query.CreateSessionRequest) (*Ydb_Query.CreateSessionResponse, error) {
	return &Ydb_Query.CreateSessionResponse{Status: Ydb.StatusIds_SUCCESS, SessionId: "ydb://session?node_id=1&id=test", NodeId: 1}, nil
}

func (s *iamWireQuery) DeleteSession(context.Context, *Ydb_Query.DeleteSessionRequest) (*Ydb_Query.DeleteSessionResponse, error) {
	return &Ydb_Query.DeleteSessionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (s *iamWireQuery) AttachSession(_ *Ydb_Query.AttachSessionRequest, stream queryv1.QueryService_AttachSessionServer) error {
	if err := stream.Send(&Ydb_Query.SessionState{Status: Ydb.StatusIds_SUCCESS}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

var (
	iamWireIdentifier  = regexp.MustCompile("`([^`]+)`")
	iamWireProperty    = regexp.MustCompile(`([A-Z_]+) = "([^"]*)"`)
	iamWireSecretValue = regexp.MustCompile(`value = ("(?:[^"\\]|\\.)*")`)
)

func (s *iamWireQuery) ExecuteQuery(req *Ydb_Query.ExecuteQueryRequest, stream queryv1.QueryService_ExecuteQueryServer) error {
	f := s.f
	f.mu.Lock()
	defer f.mu.Unlock()
	q := req.GetQueryContent().GetText()
	tickets := metadata.ValueFromIncomingContext(stream.Context(), "x-ydb-auth-ticket")
	ticket := ""
	if len(tickets) == 1 {
		ticket = tickets[0]
	}
	code := Ydb.StatusIds_SUCCESS
	p := ""
	if id := iamWireIdentifier.FindStringSubmatch(q); len(id) > 1 {
		p = id[1]
	}
	switch {
	case strings.HasPrefix(q, "DROP EXTERNAL DATA SOURCE"):
		if f.deleteError != nil {
			return f.deleteError
		}
		f.exists = false
	case strings.Contains(q, "CREATE SECRET"):
		var token string
		if value := iamWireSecretValue.FindStringSubmatch(q); len(value) == 2 {
			token, _ = strconv.Unquote(value[1])
		}
		switch {
		case token == "" || token != ticket:
			code = Ydb.StatusIds_BAD_REQUEST
		case f.secrets[p] != "":
			code = Ydb.StatusIds_ALREADY_EXISTS
		default:
			f.secrets[p] = token
		}
	case strings.HasPrefix(q, "DROP SECRET"):
		if f.failDrop {
			code = Ydb.StatusIds_UNAUTHORIZED
		} else {
			delete(f.secrets, p)
		}
	case strings.Contains(q, "EXTERNAL DATA SOURCE"):
		if f.mutationError != nil {
			return f.mutationError
		}
		properties := map[string]string{}
		for _, kv := range iamWireProperty.FindAllStringSubmatch(q, -1) {
			properties[kv[1]] = kv[2]
		}
		secret := properties["INITIAL_TOKEN_SECRET_PATH"]
		switch {
		case f.exists && !strings.HasPrefix(q, "CREATE OR REPLACE"):
			code = Ydb.StatusIds_ALREADY_EXISTS
		case properties["AUTH_METHOD"] == "IAM" && (ticket == "" || f.secrets[secret] != ticket):
			code = Ydb.StatusIds_BAD_REQUEST
		default:
			if properties["AUTH_METHOD"] == "IAM" && properties["RESOURCE_ID"] == "" {
				properties["RESOURCE_ID"] = "resolved:" + properties["DATABASE_NAME"]
			}
			f.exists = true
			f.properties = properties
		}
	default:
		code = Ydb.StatusIds_BAD_REQUEST
	}
	f.requests = append(f.requests, iamWireRequest{query: proto.Clone(req).(*Ydb_Query.ExecuteQueryRequest), ticket: ticket, status: code})
	if f.loseReply != "" && strings.Contains(q, f.loseReply) {
		f.loseReply = ""
		return status.Error(codes.Unavailable, "lost reply")
	}
	return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{Status: code})
}

type iamWireTable struct {
	tablev1.UnimplementedTableServiceServer
	f *iamWireDB
}

type iamWireScheme struct {
	schemev1.UnimplementedSchemeServiceServer
	f *iamWireDB
}

func (s *iamWireScheme) ListDirectory(_ context.Context, req *Ydb_Scheme.ListDirectoryRequest) (*Ydb_Scheme.ListDirectoryResponse, error) {
	f := s.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listStatus != Ydb.StatusIds_STATUS_CODE_UNSPECIFIED {
		return &Ydb_Scheme.ListDirectoryResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: f.listStatus}}, nil
	}
	result := &Ydb_Scheme.ListDirectoryResult{}
	for p := range f.secrets {
		if path.Dir(p) == req.GetPath() {
			result.Children = append(result.Children, &Ydb_Scheme.Entry{Name: path.Base(p)})
		}
	}
	if f.exists && req.GetPath() == "/owner" {
		result.Children = append(result.Children, &Ydb_Scheme.Entry{Name: "eds"})
	}
	return &Ydb_Scheme.ListDirectoryResponse{Operation: iamWireOperation(result)}, nil
}

func (s *iamWireTable) DescribeExternalDataSource(context.Context, *Ydb_Table.DescribeExternalDataSourceRequest) (*Ydb_Table.DescribeExternalDataSourceResponse, error) {
	f := s.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.describeError != nil {
		return nil, f.describeError
	}
	if !f.exists {
		return &Ydb_Table.DescribeExternalDataSourceResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SCHEME_ERROR}}, nil
	}
	if f.describeStatus != Ydb.StatusIds_STATUS_CODE_UNSPECIFIED {
		return &Ydb_Table.DescribeExternalDataSourceResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: f.describeStatus}}, nil
	}
	src, loc := f.properties["SOURCE_TYPE"], f.properties["LOCATION"]
	props := map[string]string{}
	for k, v := range f.properties {
		props[k] = v
	}
	return &Ydb_Table.DescribeExternalDataSourceResponse{Operation: iamWireOperation(&Ydb_Table.DescribeExternalDataSourceResult{Self: &Ydb_Scheme.Entry{Name: "eds"}, SourceType: &src, Location: &loc, Properties: props})}, nil
}
