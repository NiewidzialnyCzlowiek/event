// This file contains an implementation of Gifford's static voting
// algorithm. The general idea is based on the description published
// in one of Doctor Michał Sychowiak's teaching materials found under this url:
// https://www.cs.put.poznan.pl/mszychowiak/dydaktyka/student/Slajdy-FT-07_Voting.pdf
// The file can be accessed with the term: student

package examples

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/niewidzialnyczlowiek/event"
	"github.com/niewidzialnyczlowiek/event/cluster"
	"github.com/niewidzialnyczlowiek/event/slices"
)

// Record represents a piece of data shared between the nodes.
// Besides the data each Record contains a version that counts
// the value modifications.
type Record struct {
	// Version indicates the version number of the record
	Version uint32
	// Data is the record's data field. For demonstration
	// purposes it's a simple string
	Data string
	// Locked indicates if the record is locked meaning
	// that some process wants to access it
	Locked bool
}

// algorithm phases
const (
	PhaseIdle = iota
	PhaseGatheringVotes
	PhaseAccessingRecord
)

// Props represent a single peer state. This struct contains
// all the variables that the event handlers share.
type Props struct {
	// VotingPower defines the importance of the current
	// process' vote
	VotingPower int
	// ReadQuorum specifies how many votes a proess has to
	// gather to read a record
	ReadQuorum int
	// WriteQuorum specifies how many votes a proess has to
	// gather to modify a record
	WriteQuorum int
	// Strage represents the replicated data storage. It is
	// a simple key-value store
	Storage map[uint32]Record
	// GatheredVotes is a slice that serves as a storage
	// for votes gathered during the voting phase
	GatheredVotes []LockRecordResponse
	// Phase defines the phase of the algorithm that
	// the current process is in
	Phase int
	// AccessedRecord specifies which record is currently
	// tired to be accessed by this process
	AccessedRecord uint32
	// PeerId specifies the Id of the current process
	PeerId int
}

// ******************************************
// **************** PROTOCOL ****************
// ******************************************
const (
	AccessRecordType       event.EventType = 100
	LockRecordRequestType  event.EventType = 101
	LockRecordResponseType event.EventType = 102
	LockReleasedType       event.EventType = 103
	RecordRequestType      event.EventType = 104
	RecordResponseType     event.EventType = 105
	RecordUpdateType       event.EventType = 106
)

// AccessRecord means that this node wants to access
// the specified shared record
type AccessRecord struct {
	RecId uint32
}

// LockRecordRequest is used to indicate that someone
// tries to put a lock on a record with specified id.
type LockRecordRequest struct {
	RecId  uint32
	PeerId int
}

// LockRecordResponse is a response from nodes that
// do not want to put a lock on the record specified
// by the recId.
type LockRecordResponse struct {
	RecId          uint32
	ReplicaVersion uint32
	Votes          int
	ToPeerId       int
}

// LockReleased indicates that a the record with recId
// should be unlocked
type LockReleased struct {
	RecId uint32
}

// RecordRequest is used to get replicas from peers
type RecordRequest struct {
	RecId   uint32
	Version uint32
}

// RecordResponse is used to propagate replica from local
// storage to other peers
type RecordResponse struct {
	RecId uint32
	Rec   Record
}

// RecordUpdate is sent by a node that succesfully locked
// a record and modified it. This message suggests that
// a local replica should be updated.
type RecordUpdate struct {
	RecId      uint32
	Rec        Record
	OldVersion uint32
}

// The fallowing events represent internal changes of state.
// They should only be published to self using the
// Publisher.ToSelf() or Publisher.BackgroundTast()
const (
	countVotesType     event.EventType = 200
	accessGrantedType  event.EventType = 201
	recordImportedType event.EventType = 202
)

// countVotes indicates that the timeout for gathering
// votes has passed and the votes should be counted
type countVotes struct {
	RecId uint32
}

// accessGranted indicates that the access to the record
// identified by RecId has been granted
type accessGranted struct {
	RecId uint32
}

// recordImported indicates that the process received
// event with the version of replica that it has been
// waiting to update
type recordImported struct {
	RecId uint32
}

var allMessages = []any{
	&AccessRecord{},
	&LockRecordRequest{},
	&LockRecordResponse{},
	&LockReleased{},
	&RecordRequest{},
	&RecordResponse{},
	&RecordUpdate{},
	&countVotes{},
	&accessGranted{},
	&recordImported{},
}

const (
	AccessDenied = iota
	ReadAllowed
	WriteAllowed
)

// ******************************************
// ************ HELPER FUNCTIONS ************
// ******************************************
func formatPrivilage(p int) string {
	switch p {
	case AccessDenied:
		return "Access denied"
	case ReadAllowed:
		return "Read allowed"
	case WriteAllowed:
		return "Write allowed"
	}
	return "Unknown privilage"
}

func getLatestVersion(replicas []LockRecordResponse) uint32 {
	var newest uint32 = 0
	if len(replicas) > 0 {
		newest = replicas[0].ReplicaVersion
	}
	for _, lrr := range replicas {
		if lrr.ReplicaVersion > newest {
			newest = lrr.ReplicaVersion
		}
	}
	return newest
}

func countReadVotes(votes []LockRecordResponse) int {
	rv := 0
	for _, lrr := range votes {
		rv += lrr.Votes
	}
	return rv
}

func countWriteVotes(votes []LockRecordResponse) int {
	latestVer := getLatestVersion(votes)
	v := 0
	for _, lrr := range votes {
		if lrr.ReplicaVersion == latestVer {
			v += lrr.Votes
		}
	}
	return v
}

func voteForSelf(recId uint32, state *Props) LockRecordResponse {
	return LockRecordResponse{
		RecId:          recId,
		ReplicaVersion: state.Storage[recId].Version,
		Votes:          state.VotingPower,
	}
}

func updateRecord(rec *Record) {
	rec.Version += 1
	if strings.HasSuffix(rec.Data, "+") {
		rec.Data = strings.TrimSuffix(rec.Data, "+")
	} else {
		rec.Data = strings.Join([]string{rec.Data, "+"}, "")
	}
}

func (state *Props) lock(recId uint32) {
	rec := state.Storage[recId]
	rec.Locked = true
	state.Storage[recId] = rec
}

func (state *Props) unlock(recId uint32) {
	rec := state.Storage[recId]
	rec.Locked = false
	state.Storage[recId] = rec
}

// ******************************************
// *************** ALGORITHM ****************
// ******************************************
func setupAppHandlers(state *Props) *event.AppHandlers {
	lf := event.NewDefaultLoggerFactory()
	log := lf.NewLogger()
	ser := event.NewSerializer()
	ser.Register(allMessages)
	rights := AccessDenied
	handlers := event.NewAppHandlers(lf, false)
	handlers.Register(AccessRecordType, func(e event.Event, publisher event.Publisher) {
		// Make sure the algorithm is not already running
		if state.Phase != PhaseIdle {
			return
		}
		lockRecord := new(AccessRecord)
		event.Deserialize(e, lockRecord, ser)
		// Check if the record is already locked
		if !state.Storage[lockRecord.RecId].Locked {
			// If not locked then start the algorithm
			log.Infof("Peer %d - launching the algorithm to access record %d", state.PeerId, lockRecord.RecId)
			// Lock the local replica of the record
			state.lock(lockRecord.RecId)
			// Update algorithm phase
			state.Phase = PhaseGatheringVotes
			// Set id of the accessed record
			state.AccessedRecord = lockRecord.RecId
			// Init gathering votes
			state.GatheredVotes = state.GatheredVotes[:0]
			state.GatheredVotes = append(state.GatheredVotes, voteForSelf(lockRecord.RecId, state))
			// Publish event that the record was locked
			publisher.ToAll(*event.NewEvent(LockRecordRequestType, &LockRecordRequest{
				RecId:  lockRecord.RecId,
				PeerId: state.PeerId,
			}, ser))
			// Create a notification to count votes after the timeout for gathering votes passes
			notification := *event.NewEvent(countVotesType, &countVotes{RecId: lockRecord.RecId}, ser)
			publisher.BackgroundTask(notification, func() { time.Sleep(time.Second * time.Duration(3)) })
		} else {
			// If the record is already locked we resign
			log.Infof("Peer %d - tried to launch accessing record but the record %d is already locked", state.PeerId, lockRecord.RecId)
			return
		}
	})

	handlers.Register(countVotesType, func(e event.Event, publisher event.Publisher) {
		msg := new(countVotes)
		event.Deserialize(e, msg, ser)
		state.Phase = PhaseAccessingRecord
		readVotes := countReadVotes(state.GatheredVotes)
		writeVotes := countWriteVotes(state.GatheredVotes)
		if readVotes >= state.ReadQuorum {
			rights = ReadAllowed
		}
		if writeVotes >= state.WriteQuorum {
			rights = WriteAllowed
		}
		if rights > AccessDenied {
			log.Infof("Peer %d - ACCESS TO RECORD %d WITH PRIVILAGE %s GRANTED. Got %d read votes and %d write votes. Required %d for read and %d for write", state.PeerId, msg.RecId, formatPrivilage(rights), readVotes, writeVotes, state.ReadQuorum, state.WriteQuorum)
			newestVersion := getLatestVersion(state.GatheredVotes)
			if state.Storage[msg.RecId].Version < newestVersion {
				log.Infof("Peer %d - got %s privilage for %d. Updating value before use.", state.PeerId, formatPrivilage(rights), msg.RecId)
				req := &RecordRequest{
					RecId:   msg.RecId,
					Version: newestVersion,
				}
				publisher.ToAll(*event.NewEvent(RecordRequestType, req, ser))
			} else {
				nextStep := &accessGranted{
					RecId: msg.RecId,
				}
				publisher.ToSelf(*event.NewEvent(accessGrantedType, nextStep, ser))
			}
		} else {
			log.Infof("Peer %d - failed to access object %d. Got %d read votes and %d write votes. Required %d for read and %d for write", state.PeerId, msg.RecId, readVotes, writeVotes, state.ReadQuorum, state.WriteQuorum)
			state.unlock(msg.RecId)
			publisher.ToAll(*event.NewEvent(LockReleasedType, &LockReleased{RecId: msg.RecId}, ser))
		}
	})

	handlers.Register(recordImportedType, func(e event.Event, publisher event.Publisher) {
		msg := new(recordImported)
		event.Deserialize(e, msg, ser)
		granted := &accessGranted{RecId: msg.RecId}
		log.Infof("Peer %d - imported record. Passing access granted with id: %d", state.PeerId, msg.RecId)
		publisher.ToSelf(*event.NewEvent(accessGrantedType, granted, ser))
	})

	handlers.Register(accessGrantedType, func(e event.Event, publisher event.Publisher) {
		msg := new(accessGranted)
		event.Deserialize(e, msg, ser)
		if rights == ReadAllowed {
			log.Infof("Peer %d - reading record %d: %s", state.PeerId, msg.RecId, state.Storage[msg.RecId].Data)
		}
		if rights == WriteAllowed {
			rec := state.Storage[msg.RecId]
			oldVersion := rec.Version
			updateRecord(&rec)
			state.Storage[msg.RecId] = rec
			log.Infof("Peer %d - modified record %d: %s", state.PeerId, msg.RecId, state.Storage[msg.RecId].Data)
			updateNotif := &RecordUpdate{
				RecId:      msg.RecId,
				Rec:        rec,
				OldVersion: oldVersion,
			}
			log.Infof("Peer %d - sending update notification for %d", state.PeerId, msg.RecId)
			publisher.ToAll(*event.NewEvent(RecordUpdateType, updateNotif, ser))
		}
		state.unlock(msg.RecId)
		state.Phase = PhaseIdle
		publisher.ToAll(*event.NewEvent(LockReleasedType, &LockReleased{RecId: msg.RecId}, ser))
	})

	handlers.Register(LockRecordRequestType, func(e event.Event, publisher event.Publisher) {
		lockRequest := new(LockRecordRequest)
		event.Deserialize(e, lockRequest, ser)
		// If can lock reply with own replica
		if !state.Storage[lockRequest.RecId].Locked {
			state.lock(lockRequest.RecId)
			resp := &LockRecordResponse{
				RecId:          lockRequest.RecId,
				ReplicaVersion: state.Storage[lockRequest.RecId].Version,
				Votes:          state.VotingPower,
				ToPeerId:       lockRequest.PeerId,
			}
			log.Infof("Peer %d - received lock request on %d. Local lock acquired - sending response %v", state.PeerId, lockRequest.RecId, resp)
			sndMsg := event.NewEvent(LockRecordResponseType, resp, ser)
			publisher.ToAll(*sndMsg)
		}
		// otherwise ignore the event
	})

	handlers.Register(LockRecordResponseType, func(e event.Event, publisher event.Publisher) {
		lockResp := new(LockRecordResponse)
		event.Deserialize(e, lockResp, ser)
		// If in PhaseGatheringVotes then save the vote
		if state.Phase == PhaseGatheringVotes && state.AccessedRecord == lockResp.RecId && state.PeerId == lockResp.ToPeerId {
			log.Infof("Peer %d - saving vote %v", state.PeerId, lockResp)
			state.GatheredVotes = append(state.GatheredVotes, *lockResp)
		}
		// If not in PhaseGatheringVotes then ignore
	})

	handlers.Register(LockReleasedType, func(e event.Event, publisher event.Publisher) {
		lockReleased := new(LockReleased)
		event.Deserialize(e, lockReleased, ser)
		if state.Phase == PhaseGatheringVotes && state.AccessedRecord == lockReleased.RecId {
			log.Errorf("Peer %d - INVALID STATE", state.PeerId)
		}
		log.Infof("Peer %d - freeing lock on %d", state.PeerId, lockReleased.RecId)
		rec := state.Storage[lockReleased.RecId]
		rec.Locked = false
		state.Storage[lockReleased.RecId] = rec
	})

	handlers.Register(RecordRequestType, func(e event.Event, publisher event.Publisher) {
		req := new(RecordRequest)
		err := event.Deserialize(e, req, ser)
		if err != nil {
			// log.Errorf("Error when deserializing: %s", err.Error())
			return
		}
		if state.Storage[req.RecId].Version == req.Version {
			log.Infof("Peer %d - broadcasting version %d of record %d", state.PeerId, req.Version, req.RecId)
			resp := &RecordResponse{
				RecId: req.RecId,
				Rec:   state.Storage[req.RecId],
			}
			publisher.ToAll(*event.NewEvent(RecordResponseType, resp, ser))
		}
	})

	handlers.Register(RecordResponseType, func(e event.Event, publisher event.Publisher) {
		replica := new(RecordResponse)
		err := event.Deserialize(e, replica, ser)
		if err != nil {
			// log.Errorf("Error when deserializing: %s", err.Error())
			return
		}
		if state.Phase == PhaseAccessingRecord && state.AccessedRecord == replica.RecId {
			if replica.Rec.Version > state.Storage[replica.RecId].Version {
				log.Infof("Peer %d - got version %d of record %d", state.PeerId, replica.Rec.Version, replica.RecId)
				state.Storage[replica.RecId] = replica.Rec
				nextStep := &recordImported{
					RecId: replica.RecId,
				}
				publisher.ToSelf(*event.NewEvent(recordImportedType, nextStep, ser))
			}
		}
	})

	handlers.Register(RecordUpdateType, func(e event.Event, publisher event.Publisher) {
		update := new(RecordUpdate)
		err := event.Deserialize(e, update, ser)
		if err != nil {
			// log.Errorf("Error when deserializing: %s", err.Error())
			return
		}
		if state.Storage[update.RecId].Version == update.OldVersion {
			log.Infof("Peer %d - updating record %d from version %d to %d", state.PeerId, update.RecId, update.OldVersion, update.Rec.Version)
			state.Storage[update.RecId] = update.Rec
		}
	})

	return handlers
}

func CreateStaticVotingNode(role int, addr, appPort, svcPort string, seeds []string, initialState Props) *cluster.Node {
	config := cluster.Config{
		Role:            role,
		Addr:            addr,
		ServicePort:     svcPort,
		ApplicationPort: appPort,
		SeedsAddr:       seeds,
		LogerFactory:    event.NewDefaultLoggerFactory(),
	}
	node, _ := cluster.NewNode(config, setupAppHandlers(&initialState))
	return node
}

func RunRecordAccessLoop(node *cluster.Node, props Props) {
	ser := event.NewSerializer()
	go func() {
		for {
			req := &AccessRecord{
				RecId: rand.Uint32() % uint32(len(props.Storage)),
			}
			node.Handle(*event.NewEvent(AccessRecordType, req, ser))
			time.Sleep(time.Second * (1 + time.Duration(rand.Intn(5))))
		}
	}()
}

func createSampleStorage() map[uint32]Record {
	return map[uint32]Record{
		1: {
			Version: 0,
			Data:    "Record1",
			Locked:  false,
		},
		2: {
			Version: 0,
			Data:    "Record2",
			Locked:  false,
		},
		3: {
			Version: 0,
			Data:    "Record3",
			Locked:  false,
		},
	}
}

func RunStaticVotingExample() {
	votingPowers := []int{1, 2, 1, 2, 1}
	addresses := []string{"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"}
	appPorts := []string{"3000", "3002", "3004", "3006", "3008"}
	servicePorts := []string{"3001", "3003", "3005", "3007", "3009"}
	writeQuorum := slices.Sum(votingPowers)/2 + 2
	readQuorum := slices.Sum(votingPowers) - writeQuorum + 1
	seeds := []string{"127.0.0.1:3001", "127.0.0.1:3003"}
	instances := []*cluster.Node{}
	var state Props
	for i := range addresses {
		role := cluster.NormalNode
		if _, contains := slices.Contains(seeds, net.JoinHostPort(addresses[i], servicePorts[i])); contains {
			role = cluster.SeedNode
		}
		state = Props{
			VotingPower:    votingPowers[i],
			ReadQuorum:     readQuorum,
			WriteQuorum:    writeQuorum,
			Storage:        createSampleStorage(),
			GatheredVotes:  []LockRecordResponse{},
			Phase:          PhaseIdle,
			AccessedRecord: 0,
			PeerId:         i,
		}
		instances = append(instances, CreateStaticVotingNode(role, addresses[i], appPorts[i], servicePorts[i], seeds, state))
	}
	for i := range instances {
		instances[i].Run()
	}
	time.Sleep(time.Second)
	for i, n := range instances {
		for p := range instances {
			if p != i {
				err := n.Connect(addresses[i], appPorts[p], servicePorts[p])
				if err != nil {
					fmt.Printf("Cannot connect to peer: %s", err.Error())
				}
			}
		}
		time.Sleep(time.Second * 5)
		RunRecordAccessLoop(n, state)
	}
}
