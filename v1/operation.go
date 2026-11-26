package v1

// OperationIdentity is the composite operation identity introduced for new
// traffic operations. The opaque Operation token remains the external flow
// identifier; the composite identity scopes it to a service and owner so that
// equal external flows used by different services can never be acknowledged as
// each other's applied result.
//
// Bindings and intents written before the upgrade leave this nil: their
// identity is the legacy bare Operation token, and verification keeps
// replaying them under the v1 rule.
type OperationIdentity struct {
	Service  Service `json:"service"`
	OwnerUID string  `json:"ownerUid"`
	External string  `json:"external"`
}

// NewOperationIdentity mints a composite identity and its opaque external
// flow token. The digest is taken over the service, owner and sequence rather
// than only the owner and sequence, so two releases on different services
// never share an external flow even when their journal sequences coincide.
func NewOperationIdentity(service Service, ownerUID string, sequence uint64) (OperationIdentity, error) {
	identity := OperationIdentity{Service: service, OwnerUID: ownerUID}
	external, err := Digest(struct {
		Namespace string
		Name      string
		OwnerUID  string
		Sequence  uint64
	}{service.Namespace, service.Name, ownerUID, sequence})
	if err != nil {
		return OperationIdentity{}, err
	}
	identity.External = external
	return identity, nil
}

func (i OperationIdentity) Validate() error {
	if err := i.Service.Validate(); err != nil {
		return err
	}
	if !ValidToken(i.OwnerUID) || !ValidToken(i.External) {
		return Invalid("operation identity owner or external flow")
	}
	return nil
}

// SameOperation decides whether two operation references acknowledge the same
// applied operation under either representation.
//
// The v2 rule requires an equal composite identity (service, owner and
// external flow). A legacy bare token is only recognized against another
// legacy representation with the same token. A v2 identity never matches a
// bare token: mixing the two representations must not silently acknowledge an
// operation whose service and owner cannot be proven.
func SameOperation(identity *OperationIdentity, operation string, otherIdentity *OperationIdentity, otherOperation string) bool {
	if identity != nil || otherIdentity != nil {
		return identity != nil && otherIdentity != nil && *identity == *otherIdentity
	}
	return operation != "" && operation == otherOperation
}

// BindingAcknowledgesIntent reports whether an applied traffic binding is the
// durable result of a prepared intent, using the representation-aware
// operation rule.
func BindingAcknowledgesIntent(binding TrafficBinding, intent Intent) bool {
	return SameOperation(binding.Identity, binding.Operation, intent.Identity, intent.Operation)
}
