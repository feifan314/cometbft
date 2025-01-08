package mempool

import (
	"context"
	"errors"
	"sync"
	"time"

	"fmt"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/clist"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
	"golang.org/x/sync/semaphore"
)

// Reactor handles mempool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *CListMempool
	ids     *mempoolIDs

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(config *cfg.MempoolConfig, mempool *CListMempool) *Reactor {
	memR := &Reactor{
		config:  config,
		mempool: mempool,
		ids:     newMempoolIDs(),
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// InitPeer implements Reactor by creating a state for the peer.
func (memR *Reactor) InitPeer(peer p2p.Peer) p2p.Peer {
	memR.ids.ReserveForPeer(peer)
	return peer
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *Reactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart() error {
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}
	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *Reactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// 始终将交易转发给无条件的对等方。Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				//根据对等体的类型，我们选择一个信号量来限制八卦的对等体。
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err != nil {
							continue
						}

						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
						break
					}
				}
			}

			memR.mempool.metrics.ActiveOutboundConnections.Add(1)
			defer memR.mempool.metrics.ActiveOutboundConnections.Add(-1)
			//xiugai添加
			memR.postTx(peer) //xiugai
			memR.broadcastTxRoutine(peer)
		}()
		fmt.Println("----启动postTx------")
	}
}

// RemovePeer implements Reactor.
func (memR *Reactor) RemovePeer(peer p2p.Peer, _ interface{}) {
	memR.ids.Reclaim(peer)
	// broadcast routine checks if peer is gone and returns
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.Txs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("received empty txs from peer", "src", e.Src)
			return
		}
		//xiugai 高速，提前交易
		txInfo := TxInfo{SenderID: memR.ids.GetForPeer(e.Src)}
		if e.Src != nil {
			txInfo.SenderP2PID = e.Src.ID()
		}

		var err error
		for _, tx := range protoTxs {

			ntx := types.Tx(tx)
			//xiugai
			//if bytes.Contains(tx, MsgAA) == true { //即使流动LP也有这个
			//	fmt.Println("Tx信息", fmt.Sprintf("%X", ntx.Hash()), time.Now().UnixMilli(), string(tx), "多余", string(ntx))
			//}
			//fmt.Println("详细数据", ntx.String())
			err = memR.mempool.CheckTx(ntx, nil, txInfo)
			if err != nil {
				switch {
				case errors.Is(err, ErrTxInCache):
					memR.Logger.Debug("Tx already exists in cache", "tx", ntx.String())
				case errors.As(err, &ErrMempoolIsFull{}):
					// using debug level to avoid flooding when traffic is high
					memR.Logger.Debug(err.Error())
				default:
					memR.Logger.Info("Could not check tx", "tx", ntx.String(), "err", err)
				}
			}
		}
	default:
		memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

var PostTxChan = make(chan struct{})
var PostByteLock sync.RWMutex
var PostByte []byte

func (memR *Reactor) postTx(peer p2p.Peer) {
	//xiugai
	//GetForPeer返回为对等方保留的ID。
	//memR.postTxChan = make(chan struct{})
	//peerIdString := peer.ID()
	//peerID := memR.ids.GetForPeer(peer) //GetForPeer返回为对等方保留的ID。
	//var myByte []byte
	//fmt.Println("自己增加节点 启动", peer.String(), peerIdString, peerID)
	//fmt.Println("增加节点 启动", peer.String(), peer.ID())
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		//如果两种情况都是下一种情况。NextWaithan（）和peer。Quit（）同时是变量
		if !memR.IsRunning() || !peer.IsRunning() {
			//if !memR.IsRunning() && !peer.IsRunning() {
			//	fmt.Println("======自己节点返回可能关闭AB", peer.ID(), peer.RemoteAddr().String())
			//} else if !memR.IsRunning() {
			//	fmt.Println("======自己节点返回可能关闭A", peer.ID(), peer.RemoteAddr().String())
			//} else {
			//	fmt.Println("======自己节点返回可能关闭B", peer.ID(), peer.RemoteAddr().String())
			//}
			//fmt.Println("======自己节点返回可能关闭B", peer.ID(), peer.RemoteAddr().String())
			return
		}
		//fmt.Println("======自己节点", peer.String(), peerIdString, peerID)

		select {
		//WaitChan可用于等待Front或Back变为非零。一旦发生，频道将被关闭。
		case <-PostTxChan: // Wait until a tx is available//等待TX可用
			//////yongTime := (time.Now().UnixNano() - StartTime) / 1000
			//myByte = PostByte
			//fmt.Println("自己_收到信息", string(myByte)[4:], peer.ID(), yongTime)
			//////fmt.Println("自己_收到信息", yongTime, peer.ID(), peer.RemoteAddr().String())
			//if yongTime < 50 {
			//	zongPeers := memR.Switch.Peers().List()
			//	fmt.Println("数量", len(zongPeers))
			//
			//}
			fmt.Println("发出", time.Microsecond)
			success := peer.Send(p2p.Envelope{
				ChannelID: MempoolChannel,
				Message:   &protomem.Txs{Txs: [][]byte{PostByte}},
			})
			if !success {
				time.Sleep(100 * time.Millisecond)
				fmt.Println("======发送给48通道失败", peer.ID(), peer.RemoteAddr().String())
				continue
			}
			//success := peer.Send(byte(0x30), PostByte) //myByte = PostByte
			//if !success {
			//	time.Sleep(100 * time.Millisecond)
			//	//fmt.Println("======发送给48通道失败", peer.ID(), peer.RemoteAddr().String())
			//	continue
			//}
			time.Sleep(100 * time.Millisecond) //cuo
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}

func ZhiXingFaSong(postByte []byte) {
	fmt.Println("发送", time.Microsecond)
	PostByteLock.Lock()
	defer PostByteLock.Unlock()
	//全局
	PostByte = postByte
	close(PostTxChan)
	time.Sleep(50 * time.Millisecond) //毫秒
	PostTxChan = make(chan struct{})
}

// Send new mempool txs to peer.
func (memR *Reactor) broadcastTxRoutine(peer p2p.Peer) {
	peerID := memR.ids.GetForPeer(peer)
	var next *clist.CElement

	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-memR.mempool.TxsWaitChan(): // Wait until a tx is available
				if next = memR.mempool.TxsFront(); next == nil {
					continue
				}
			case <-peer.Quit():
				return
			case <-memR.Quit():
				return
			}
		}

		// Make sure the peer is up to date.
		peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
		if !ok {
			// Peer does not have a state yet. We set it in the consensus reactor, but
			// when we add peer in Switch, the order we call reactors#AddPeer is
			// different every time due to us using a map. Sometimes other reactors
			// will be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// Allow for a lag of 1 block.
		memTx := next.Value.(*mempoolTx)
		if peerState.GetHeight() < memTx.Height()-1 {
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// NOTE: Transaction batching was disabled due to
		// https://github.com/tendermint/tendermint/issues/5796

		if !memTx.isSender(peerID) {
			success := peer.Send(p2p.Envelope{
				ChannelID: MempoolChannel,
				Message:   &protomem.Txs{Txs: [][]byte{memTx.tx}},
			})
			if !success {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}
		}

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}

// TxsMessage is a Message containing transactions.
type TxsMessage struct {
	Txs []types.Tx
}

// String returns a string representation of the TxsMessage.
func (m *TxsMessage) String() string {
	return fmt.Sprintf("[TxsMessage %v]", m.Txs)
}
