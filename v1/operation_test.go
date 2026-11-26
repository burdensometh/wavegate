package v1

import "testing"

func TestNewOperationIdentityScopesExternalFlowByService(t *testing.T) {
	serviceA := Service{Namespace: "shop", Name: "checkout"}
	serviceB := Service{Namespace: "shop", Name: "catalog"}
	first, err := NewOperationIdentity(serviceA, "owner-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewOperationIdentity(serviceA, "owner-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	otherService, err := NewOperationIdentity(serviceB, "owner-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	otherOwner, err := NewOperationIdentity(serviceA, "owner-2", 7)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("identity minting is not deterministic: %+v != %+v", first, second)
	}
	if !ValidDigest(first.External) || first.External == otherService.External ||
		first.External == otherOwner.External {
		t.Fatalf("external flow must differ across service and owner: %+v", first)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("minted identity is invalid: %v", err)
	}
}

func TestSameOperationRepresentations(t *testing.T) {
	service := Service{Namespace: "shop", Name: "checkout"}
	identity := OperationIdentity{Service: service, OwnerUID: "owner-1", External: "flow-1"}
	peer := identity
	foreign := OperationIdentity{Service: Service{Namespace: "shop", Name: "catalog"}, OwnerUID: "owner-1", External: "flow-1"}

	cases := []struct {
		name      string
		identity  *OperationIdentity
		operation string
		other     *OperationIdentity
		otherOp   string
		want      bool
	}{
		{"composite equal", &identity, "flow-1", &peer, "flow-1", true},
		{"composite different service", &identity, "flow-1", &foreign, "flow-1", false},
		{"legacy equal tokens", nil, "bare-token", nil, "bare-token", true},
		{"legacy different tokens", nil, "bare-token", nil, "other-token", false},
		{"mixed v2 and legacy never match", &identity, "flow-1", nil, "flow-1", false},
		{"mixed legacy and v2 never match", nil, "flow-1", &identity, "flow-1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SameOperation(tc.identity, tc.operation, tc.other, tc.otherOp); got != tc.want {
				t.Fatalf("SameOperation = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOperationIdentityValidate(t *testing.T) {
	valid := OperationIdentity{Service: Service{Namespace: "shop", Name: "checkout"}, OwnerUID: "owner-1", External: "flow-1"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	cases := map[string]OperationIdentity{
		"bad service":  {Service: Service{Namespace: "BAD", Name: "checkout"}, OwnerUID: "owner-1", External: "flow-1"},
		"bad owner":    {Service: valid.Service, OwnerUID: "bad/uid", External: "flow-1"},
		"bad external": {Service: valid.Service, OwnerUID: "owner-1", External: "bad flow"},
	}
	for name, identity := range cases {
		t.Run(name, func(t *testing.T) {
			if err := identity.Validate(); err == nil {
				t.Fatalf("%s identity accepted", name)
			}
		})
	}
}
