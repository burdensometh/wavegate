package v1

import "time"

const Version = "wavegate.io/v1"

type Service struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func (s Service) String() string { return s.Namespace + "/" + s.Name }

type ReleaseSpec struct {
	APIVersion      string       `json:"apiVersion"`
	Service         Service      `json:"service"`
	Name            string       `json:"name"`
	UID             string       `json:"uid"`
	ResourceVersion uint64       `json:"resourceVersion"`
	Stable          string       `json:"stable"`
	Candidate       string       `json:"candidate"`
	Steps           []int        `json:"steps"`
	Window          WindowPolicy `json:"window"`
	MinReady        int          `json:"minReady"`
	Paused          bool         `json:"paused"`
	Abort           bool         `json:"abort"`
}

func (r ReleaseSpec) Key() string { return r.Service.Namespace + "/" + r.Name }

type WindowPolicy struct {
	Seconds               int   `json:"seconds"`
	MinSamples            int   `json:"minSamples"`
	MaxErrorBPS           int   `json:"maxErrorBps"`
	MaxErrorDeltaBPS      int   `json:"maxErrorDeltaBps"`
	MaxMeanLatencyDeltaMS int64 `json:"maxMeanLatencyDeltaMs"`
}

type ReplicaObservation struct {
	APIVersion      string  `json:"apiVersion"`
	Service         Service `json:"service"`
	Node            string  `json:"node"`
	Name            string  `json:"name"`
	UID             string  `json:"uid"`
	Revision        string  `json:"revision"`
	Address         string  `json:"address"`
	Ready           bool    `json:"ready"`
	ResourceVersion uint64  `json:"resourceVersion"`
}

func (r ReplicaObservation) Key() string { return r.Service.String() + "/" + r.Node + "/" + r.Name }

type EndpointList struct {
	APIVersion      string               `json:"apiVersion"`
	ResourceVersion uint64               `json:"resourceVersion"`
	Items           []ReplicaObservation `json:"items"`
}

type EndpointEvent struct {
	APIVersion      string             `json:"apiVersion"`
	Type            string             `json:"type"`
	ResourceVersion uint64             `json:"resourceVersion"`
	Object          ReplicaObservation `json:"object"`
}

const (
	Added    = "ADDED"
	Modified = "MODIFIED"
	Deleted  = "DELETED"
)

type Target struct {
	Node string `json:"node"`
	Name string `json:"name"`
	UID  string `json:"uid"`
}

func (t Target) Key() string { return t.Node + "/" + t.Name }

type Rule struct {
	Service          Service   `json:"service"`
	Stable           string    `json:"stable"`
	Candidate        string    `json:"candidate,omitempty"`
	CandidateWeight  int       `json:"candidateWeight"`
	DiscoveryVersion uint64    `json:"discoveryVersion"`
	StableTargets    []Target  `json:"stableTargets"`
	CandidateTargets []Target  `json:"candidateTargets"`
	DrainTargets     []Target  `json:"drainTargets,omitempty"`
	DrainUntil       time.Time `json:"drainUntil,omitempty"`
}

type TrafficBinding struct {
	APIVersion      string             `json:"apiVersion"`
	OwnerUID        string             `json:"ownerUid"`
	ResourceVersion uint64             `json:"resourceVersion"`
	Operation       string             `json:"operation"`
	Identity        *OperationIdentity `json:"identity,omitempty"`
	AppliedAt       time.Time          `json:"appliedAt"`
	Rule            Rule               `json:"rule"`
}

type Sample struct {
	APIVersion     string    `json:"apiVersion"`
	Service        Service   `json:"service"`
	BindingVersion uint64    `json:"bindingVersion"`
	ID             string    `json:"id"`
	Role           string    `json:"role"`
	At             time.Time `json:"at"`
	LatencyMS      int64     `json:"latencyMs"`
	Failed         bool      `json:"failed"`
}

const (
	StableRole    = "stable"
	CandidateRole = "candidate"
)

type WindowKey struct {
	Service        Service   `json:"service"`
	BindingVersion uint64    `json:"bindingVersion"`
	Start          time.Time `json:"start"`
	End            time.Time `json:"end"`
}

type Measurement struct {
	Count          int   `json:"count"`
	Failed         int   `json:"failed"`
	TotalLatencyMS int64 `json:"totalLatencyMs"`
}

type WindowVerdict struct {
	APIVersion    string      `json:"apiVersion"`
	Key           WindowKey   `json:"key"`
	SampleVersion uint64      `json:"sampleVersion"`
	SampleDigest  string      `json:"sampleDigest"`
	Stable        Measurement `json:"stable"`
	Candidate     Measurement `json:"candidate"`
	Result        string      `json:"result"`
	Reason        string      `json:"reason"`
}

const (
	Pass    = "Pass"
	Fail    = "Fail"
	Pending = "Pending"
)

type NamespaceReservation struct {
	APIVersion      string    `json:"apiVersion"`
	Service         Service   `json:"service"`
	ReleaseUID      string    `json:"releaseUid"`
	ResourceVersion uint64    `json:"resourceVersion"`
	Units           int       `json:"units"`
	AcquiredAt      time.Time `json:"acquiredAt"`
}

type Condition struct {
	Type               string    `json:"type"`
	Status             bool      `json:"status"`
	Reason             string    `json:"reason"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

type ReleaseStatus struct {
	APIVersion       string         `json:"apiVersion"`
	Service          Service        `json:"service"`
	ReleaseUID       string         `json:"releaseUid"`
	ObservedVersion  uint64         `json:"observedVersion"`
	DecisionSequence uint64         `json:"decisionSequence"`
	Phase            string         `json:"phase"`
	Step             int            `json:"step"`
	BindingVersion   uint64         `json:"bindingVersion"`
	DiscoveryVersion uint64         `json:"discoveryVersion"`
	Conditions       []Condition    `json:"conditions"`
	Verdict          *WindowVerdict `json:"verdict,omitempty"`
}

type Intent struct {
	Operation       string             `json:"operation"`
	Identity        *OperationIdentity `json:"identity,omitempty"`
	ExpectedVersion uint64             `json:"expectedVersion"`
	Kind            string             `json:"kind"`
	Step            int                `json:"step"`
	Rule            Rule               `json:"rule"`
}

type ReleaseState struct {
	Spec        ReleaseSpec           `json:"spec"`
	Status      ReleaseStatus         `json:"status"`
	Binding     *TrafficBinding       `json:"binding,omitempty"`
	Intent      *Intent               `json:"intent,omitempty"`
	Window      *WindowKey            `json:"window,omitempty"`
	Reservation *NamespaceReservation `json:"reservation,omitempty"`
}

type DecisionRecord struct {
	APIVersion     string       `json:"apiVersion"`
	Sequence       uint64       `json:"sequence"`
	At             time.Time    `json:"at"`
	Kind           string       `json:"kind"`
	Reason         string       `json:"reason"`
	State          ReleaseState `json:"state"`
	PreviousDigest string       `json:"previousDigest,omitempty"`
	Digest         string       `json:"digest"`
}
