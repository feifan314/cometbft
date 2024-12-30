package mempool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/feifan314/zhongzhuan/zhongzhuan"
	"sync"
	"sync/atomic"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/clist"
	"github.com/cometbft/cometbft/libs/log"
	cmtmath "github.com/cometbft/cometbft/libs/math"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

// CListMempool is an ordered in-memory pool for transactions before they are
// proposed in a consensus round. Transaction validity is checked using the
// CheckTx abci message before the transaction is added to the pool. The
// mempool uses a concurrent list structure for storing transactions that can
// be efficiently accessed by multiple concurrent readers.
type CListMempool struct {
	height   atomic.Int64 // the last block Update()'d to
	txsBytes atomic.Int64 // total size of mempool, in bytes

	// notify listeners (ie. consensus) when txs are available
	notifiedTxsAvailable atomic.Bool
	txsAvailable         chan struct{} // fires once for each height, when the mempool is not empty

	config *config.MempoolConfig

	// Exclusive mutex for Update method to prevent concurrent execution of
	// CheckTx or ReapMaxBytesMaxGas(ReapMaxTxs) methods.
	updateMtx cmtsync.RWMutex
	preCheck  PreCheckFunc
	postCheck PostCheckFunc

	txs          *clist.CList // concurrent linked-list of good txs
	proxyAppConn proxy.AppConnMempool

	// Keeps track of the rechecking process.
	recheck *recheck

	// Map for quick access to txs to record sender in CheckTx.
	// txsMap: txKey -> CElement
	txsMap sync.Map

	// Keep a cache of already-seen txs.
	// This reduces the pressure on the proxyApp.
	cache TxCache

	logger  log.Logger
	metrics *Metrics
}

var _ Mempool = &CListMempool{}

// CListMempoolOption sets an optional parameter on the mempool.
type CListMempoolOption func(*CListMempool)

// NewCListMempool returns a new mempool with the given configuration and
// connection to an application.
func NewCListMempool(
	cfg *config.MempoolConfig,
	proxyAppConn proxy.AppConnMempool,
	height int64,
	options ...CListMempoolOption,
) *CListMempool {
	mp := &CListMempool{
		config:       cfg,
		proxyAppConn: proxyAppConn,
		txs:          clist.New(),
		recheck:      newRecheck(),
		logger:       log.NewNopLogger(),
		metrics:      NopMetrics(),
	}
	mp.height.Store(height)

	if cfg.CacheSize > 0 {
		mp.cache = NewLRUTxCache(cfg.CacheSize)
	} else {
		mp.cache = NopTxCache{}
	}

	proxyAppConn.SetResponseCallback(mp.globalCb)

	for _, option := range options {
		option(mp)
	}

	return mp
}

func (mem *CListMempool) getCElement(txKey types.TxKey) (*clist.CElement, bool) {
	if e, ok := mem.txsMap.Load(txKey); ok {
		return e.(*clist.CElement), true
	}
	return nil, false
}

func (mem *CListMempool) getMemTx(txKey types.TxKey) *mempoolTx {
	if e, ok := mem.getCElement(txKey); ok {
		return e.Value.(*mempoolTx)
	}
	return nil
}

func (mem *CListMempool) removeAllTxs() {
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		mem.txs.Remove(e)
		e.DetachPrev()
	}

	mem.txsMap.Range(func(key, _ interface{}) bool {
		mem.txsMap.Delete(key)
		return true
	})
}

// NOTE: not thread safe - should only be called once, on startup
func (mem *CListMempool) EnableTxsAvailable() {
	mem.txsAvailable = make(chan struct{}, 1)
}

// SetLogger sets the Logger.
func (mem *CListMempool) SetLogger(l log.Logger) {
	mem.logger = l
}

// WithPreCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran before CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithPreCheck(f PreCheckFunc) CListMempoolOption {
	return func(mem *CListMempool) { mem.preCheck = f }
}

// WithPostCheck sets a filter for the mempool to reject a tx if f(tx) returns
// false. This is ran after CheckTx. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithPostCheck(f PostCheckFunc) CListMempoolOption {
	return func(mem *CListMempool) { mem.postCheck = f }
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *Metrics) CListMempoolOption {
	return func(mem *CListMempool) { mem.metrics = metrics }
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Lock() {
	if mem.recheck.setRecheckFull() {
		mem.logger.Debug("the state of recheckFull has flipped")
	}
	mem.updateMtx.Lock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Unlock() {
	mem.updateMtx.Unlock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Size() int {
	return mem.txs.Len()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) SizeBytes() int64 {
	return mem.txsBytes.Load()
}

// Lock() must be help by the caller during execution.
func (mem *CListMempool) FlushAppConn() error {
	err := mem.proxyAppConn.Flush(context.TODO())
	if err != nil {
		return ErrFlushAppConn{Err: err}
	}

	return nil
}

// XXX: Unsafe! Calling Flush may leave mempool in inconsistent state.
func (mem *CListMempool) Flush() {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	mem.txsBytes.Store(0)
	mem.cache.Reset()

	mem.removeAllTxs()
}

// TxsFront returns the first transaction in the ordered list for peer
// goroutines to call .NextWait() on.
// FIXME: leaking implementation details!
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) TxsFront() *clist.CElement {
	return mem.txs.Front()
}

// TxsWaitChan returns a channel to wait on transactions. It will be closed
// once the mempool is not empty (ie. the internal `mem.txs` has at least one
// element)
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) TxsWaitChan() <-chan struct{} {
	return mem.txs.WaitChan()
}

// xiugai
func TxKeys(tx types.Tx) [32]byte {
	return sha256.Sum256(tx)
}

var YiqianTx = map[[32]byte]int64{} //xiugai6

var MsgExecuteContract []byte = []byte("/cosmwasm.wasm.v1.MsgExecuteContract")
var MsgUom []byte = []byte("\"pool_identifier\":\"o.uom.uusdc\"")

func (mem *CListMempool) CheckTx(
	tx types.Tx,
	cb func(*abci.ResponseCheckTx),
	txInfo TxInfo,
) error {
	mem.updateMtx.RLock()
	// use defer to unlock mutex because application (*local client*) might panic
	defer mem.updateMtx.RUnlock()

	txSize := len(tx)

	if err := mem.isFull(txSize); err != nil {
		mem.metrics.RejectedTxs.Add(1)
		return err
	}

	if txSize > mem.config.MaxTxBytes {
		return ErrTxTooLarge{
			Max:    mem.config.MaxTxBytes,
			Actual: txSize,
		}
	}

	if mem.preCheck != nil {
		if err := mem.preCheck(tx); err != nil {
			return ErrPreCheck{Err: err}
		}
	}

	// NOTE: proxyAppConn may error if tx buffer is full
	if err := mem.proxyAppConn.Error(); err != nil {
		return ErrAppConnMempool{Err: err}
	}

	if !mem.cache.Push(tx) { // if the transaction already exists in the cache 如果事务已存在于缓存中
		// Record a new sender for a tx we've already seen.
		// Note it's possible a tx is still in the cache but no longer in the mempool
		// (eg. after committing a block, txs are removed from mempool but not cache),
		// so we only record the sender for txs still in the mempool.
		if memTx := mem.getMemTx(tx.Key()); memTx != nil {
			memTx.addSender(txInfo.SenderID)
			// TODO: consider punishing peer for dups,
			// its non-trivial since invalid txs can become valid,
			// but they can spam the same tx with little cost to them atm.
		}
		return ErrTxInCache
	}

	linHash := TxKeys(tx) //linHash := tx.Key() 都可以

	if YiqianTx[linHash] > 0 {
		fmt.Println("以前有hash", fmt.Sprintf("%X", tx.Hash()), YiqianTx[linHash])
		return nil
	} else {
		YiqianTx[linHash] = 1 //记录hash 高度
		//-------一旦发现有新的 并且符合我们的标准 就写入数组
		fuhe := 0
		if bytes.Contains(tx, MsgExecuteContract) == true { //即使流动LP也有这个
			fmt.Println("Tx信息", fmt.Sprintf("%X", tx.Hash()), time.Now().UnixMilli())
			//ShuChu(tx)
			FenJie(tx)
			if bytes.Contains(tx, MsgUom) == true { //即使流动LP也有这个
				//fmt.Println("Tx信息", fmt.Sprintf("%X", tx.Hash()), time.Now().UnixMilli(), string(tx))
				fuhe = 1

			}
		}
		var shuzu [][]byte //xiugai6  前边判断过到这里绝对行
		if fuhe == 1 {     //只有符合新变动才写入 所有本区块交易 否则 删除
			geshu := cmtmath.MinInt(mem.txs.Len(), 999)
			for e := mem.txs.Front(); e != nil && geshu <= 999; e = e.Next() {
				shuzu = append(shuzu, e.Value.(*mempoolTx).tx) //xiugai6 以前剩余的
				//fmt.Println("一次") 8D5C96E7B2265BA1AC24E215B9DD1A52AEB6C4934F82C74EE3E85D5F96FF1FF2 两个
			}
		}
		zhongzhuan.Xietxs(shuzu)
		//tx.Hash() 还未在mem.txs.Front() 		//xiugai
		//xiugai
		fmt.Println("起点写入txs", "len", len(shuzu), "hash", fmt.Sprintf("%X", tx.Hash()), "见过len", len(YiqianTx), "是否符合", fuhe, time.Now().UnixMilli()) //xiugai200
	}

	//if bytes.Contains(tx, MsgExecuteContract) == true { //即使流动LP也有这个
	//	fmt.Println("Tx信息", fmt.Sprintf("%X", tx.Hash()), time.Now().UnixMilli())
	//	//ShuChu(tx)
	//	FenJie(tx)
	//	if bytes.Contains(tx, MsgUom) == true { //即使流动LP也有这个
	//		//fmt.Println("Tx信息", fmt.Sprintf("%X", tx.Hash()), time.Now().UnixMilli(), string(tx))
	//	}
	//}

	reqRes, err := mem.proxyAppConn.CheckTxAsync(context.TODO(), &abci.RequestCheckTx{Tx: tx})
	if err != nil {
		panic(fmt.Errorf("CheckTx request for tx %s failed: %w", log.NewLazySprintf("%v", tx.Hash()), err))
	}

	reqRes.SetCallback(mem.reqResCb(tx, txInfo, cb))

	if len(YiqianTx) >= 9000 { //xiugai
		fmt.Println("太多了，清除YiqianTx")
		YiqianTx = map[[32]byte]int64{} //如果清除后 又遇到
	}
	return nil
}

var ChiZiTxt = "mantra1466nf3zuxpya8q9emxukd7vftaf6h4psr0a07srl5zw74zh84yjqagspfm"

func FenJie(txString []byte) string {
	//[]byte{10,202,2,10,199,2,10,36,47,99,111,115,109,119,97,115,109,46,119,97,115,109,46,118,49,46,77,115,103,69,120,101,99,117,116,101,67,111,110,116,114,97,99,116,18,158,2,10,45,109,97,110,116,114,97,49,108,119,100,52,48,57,51,97,109,54,110,114,99,48,121,53,48,57,55,116,52,102,54,103,108,117,103,100,54,121,115,120,53,99,120,110,101,104,18,65,109,97,110,116,114,97,49,52,54,54,110,102,51,122,117,120,112,121,97,56,113,57,101,109,120,117,107,100,55,118,102,116,97,102,54,104,52,112,115,114,48,97,48,55,115,114,108,53,122,119,55,52,122,104,56,52,121,106,113,97,103,115,112,102,109,26,152,1,123,34,115,119,97,112,34,58,123,34,97,115,107,95,97,115,115,101,116,95,100,101,110,111,109,34,58,34,105,98,99,47,54,53,68,48,66,69,67,54,68,65,68,57,54,67,55,70,53,48,52,51,68,49,69,53,52,69,53,52,66,54,66,66,53,68,53,66,51,65,69,67,51,70,70,54,67,69,66,66,55,53,66,57,69,48,53,57,70,51,53,56,48,69,65,51,34,44,34,109,97,120,95,115,112,114,101,97,100,34,58,34,48,46,48,48,53,34,44,34,112,111,111,108,95,105,100,101,110,116,105,102,105,101,114,34,58,34,111,46,117,111,109,46,117,117,115,100,99,34,125,125,42,15,10,3,117,111,109,18,8,55,51,48,48,48,48,48,48,18,101,10,80,10,70,10,31,47,99,111,115,109,111,115,46,99,114,121,112,116,111,46,115,101,99,112,50,53,54,107,49,46,80,117,98,75,101,121,18,35,10,33,2,119,50,134,255,73,38,253,135,187,7,231,224,200,188,227,132,17,172,14,245,101,188,44,219,75,248,133,14,57,105,0,5,18,4,10,2,8,127,24,2,18,17,10,11,10,3,117,111,109,18,4,53,53,49,52,16,183,133,28,26,64,133,34,132,53,184,90,61,248,215,223,103,126,193,61,70,136,95,85,87,105,92,218,221,62,242,105,156,158,185,125,222,198,119,20,65,76,240,28,43,150,193,235,44,63,166,176,83,181,194,77,171,203,0,32,240,102,230,101,179,181,209,106,37,84}
	qi := 3                                                                            //10,202,2,
	yiCeng := (int(txString[qi-1 : qi][0])-1)*128 + int(txString[qi-2 : qi-1][0]) + qi //带上车头qi
	qi += 3                                                                            // 10,199,2,
	qi += 2                                                                            //10,36,
	nnn := int(txString[qi-1 : qi][0])                                                 ///cosmwasm.wasm.v1.MsgExecuteContract
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn                                                                           //a
	qi += 3                                                                             //18,158,2,
	sanCeng := (int(txString[qi-1 : qi][0])-1)*128 + int(txString[qi-2 : qi-1][0]) + qi //带上车头qi

	qi += 2                           //10,45,
	nnn = int(txString[qi-1 : qi][0]) //mantra1lwd4093am6nrc0y5097t4f6glugd6ysx5cxneh
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn                         //a
	qi += 2                           //18,65,
	nnn = int(txString[qi-1 : qi][0]) //mantra1466nf3zuxpya8q9emxukd7vftaf6h4psr0a07srl5zw74zh84yjqagspfm
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	if ChiZiTxt != string(txString[qi:qi+nnn]) {
		fmt.Println("别的池子")
		return "别的池子"
	}
	qi += nnn //a
	//看长短 短的是一位 长的两位
	if int(txString[qi+2 : qi+3][0]) == 123 {
		qi += 2                           //26,87
		nnn = int(txString[qi-1 : qi][0]) //{"swap":{"ask_asset_denom":"uom","max_spread":"0.005","pool_identifier":"o.uom.uusdc"}}
	} else if int(txString[qi+3 : qi+4][0]) == 123 {
		qi += 3 //26,152,1,
		//138,5 = 650   128*4+138  并非*256 真实*128
		nnn = (int(txString[qi-1 : qi][0])-1)*128 + int(txString[qi-2 : qi-1][0]) //{"swap":{"ask_asset_denom":"ibc/65D0BEC6DAD96C7F5043D1E54E54B6BB5D5B3AEC3FF6CEBB75B9E059F3580EA3","max_spread":"0.005","pool_identifier":"o.uom.uusdc"}}
	} else {

		fmt.Println("错误解析", ShuChu(txString))
		return "错误解析"
	}
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn //a
	//fmt.Println("后边", txString[qi:qi+10], "后边")
	qi += 2                           //42,15,
	qi += 2                           //10,3,|10,68,
	nnn = int(txString[qi-1 : qi][0]) //uom|ibc/65D0BEC6DAD96C7F5043D1E54E54B6BB5D5B3AEC3FF6CEBB75B9E059F3580EA3
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn                         //a
	qi += 2                           //18,8,
	nnn = int(txString[qi-1 : qi][0]) //73000000
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn //a

	//流动provide_liquidity
	if sanCeng > qi+2 && int(txString[qi : qi+1][0]) == 42 { //至少多个42,91,
		qi += 2 //多42,91,
		qi += 2 //多10,68
		nnn = int(txString[qi-1 : qi][0])
		fmt.Println("累计", string(txString[qi:qi+nnn]), "流动2币名")
		qi += nnn //a
		qi += 2   //多18,19
		nnn = int(txString[qi-1 : qi][0])
		fmt.Println("累计", string(txString[qi:qi+nnn]), "流动2数量")
		qi += nnn //a
	} else if sanCeng != qi {
		fmt.Println("异常整理")
	}

	//可能有Memo备注
	if yiCeng > qi+2 { //至少多个10,2
		qi += 2 //多18,20,
		nnn = int(txString[qi-1 : qi][0])
		fmt.Println("累计", string(txString[qi:qi+nnn]), "Memo备注结束")
		qi += nnn //a
	} else if yiCeng != qi {
		fmt.Println("异常整理")
	}
	//fmt.Println("一层", yiCeng, qi)
	qi += 2 //18,101,
	qi += 2 //10,80,
	zong := int(txString[qi-1 : qi][0])
	qi += 2 //10,70,
	qi += 2 //10,31,
	zong -= 4
	nnn = int(txString[qi-1 : qi][0]) ///cosmos.crypto.secp256k1.PubKey
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn //a
	zong -= nnn
	qi += 2 //18,35,
	qi += 2 //10,33,
	zong -= 4
	nnn = int(txString[qi-1 : qi][0]) //乱码
	//fmt.Println("累计", string(txString[qi:qi+nnn]), "乱码")
	qi += nnn //a
	zong -= nnn
	qi += 2 //18,4,
	qi += 2 //10,2,
	zong -= 4
	//fmt.Println("累计", string(txString[qi:qi+2]), "乱码")
	qi += 2 //a
	zong -= 2
	qi += zong //24,2,改正
	qi += 2    //18,17,
	//---------------分割
	qi += 2                           //10,11,
	qi += 2                           //10,3,
	nnn = int(txString[qi-1 : qi][0]) //uom
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn                         //a
	qi += 2                           //18,4,
	nnn = int(txString[qi-1 : qi][0]) //[5514]
	fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn                         //a
	qi += 2                           //16,183,
	qi += 2                           //133,28,
	qi += 2                           //26,64,
	nnn = int(txString[qi-1 : qi][0]) //乱码
	//fmt.Println("累计", string(txString[qi:qi+nnn]), "结束")
	qi += nnn //a
	if qi == len(txString) {
		//ShuChu(txString)
		fmt.Println("正确解析")
		return "正确解析"
	} else {
		fmt.Println("错误解析", ShuChu(txString))
		return "错误解析"
	}
}

func ShuChu(txString []byte) string {
	lin := "[]byte{"
	i := 0
	for _, lin_byte := range txString {
		i += 1
		if i == len(txString) {
			lin += fmt.Sprintf("%d", lin_byte) + "}"
		} else {
			lin += fmt.Sprintf("%d", lin_byte) + ","
		}

	}
	//fmt.Println("总数据", lin)
	return lin
}

// Global callback that will be called after every ABCI response.
// Having a single global callback avoids needing to set a callback for each request.
// However, processing the checkTx response requires the peerID (so we can track which txs we heard from who),
// and peerID is not included in the ABCI request, so we have to set request-specific callbacks that
// include this information. If we're not in the midst of a recheck, this function will just return,
// so the request specific callback can do the work.
//
// When rechecking, we don't need the peerID, so the recheck callback happens
// here.
func (mem *CListMempool) globalCb(req *abci.Request, res *abci.Response) {
	switch r := req.Value.(type) {
	case *abci.Request_CheckTx:
		// Process only Recheck responses.
		if r.CheckTx.Type != abci.CheckTxType_Recheck {
			return
		}
	default:
		// ignore other type of requests
		return
	}

	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		tx := types.Tx(req.GetCheckTx().Tx)
		if mem.recheck.done() {
			mem.logger.Error("rechecking has finished; discard late recheck response",
				"tx", log.NewLazySprintf("%v", tx.Key()))
			return
		}
		mem.metrics.RecheckTimes.Add(1)
		mem.resCbRecheck(tx, r.CheckTx)

		// update metrics
		mem.metrics.Size.Set(float64(mem.Size()))

	default:
		// ignore other messages
	}
}

// 请求特定的回调，该回调应设置在单个reqRes对象上，以便在处理响应时包含本地信息。
// 这允许我们跟踪向我们发送此tx的对等方，这样我们就可以避免将其发送回给他们。
// 注意：或者，我们也可以将此信息包含在ABCI请求中。
// CheckTx的外部调用者，如RPC，也可以通过这里传递一个外部Cb，当所有其他响应处理完成时调用。
// 在CheckTx中用于记录向我们发送tx的PeerID。
func (mem *CListMempool) reqResCb(
	tx []byte,
	txInfo TxInfo,
	externalCb func(*abci.ResponseCheckTx),
) func(res *abci.Response) {
	return func(res *abci.Response) {
		if !mem.recheck.done() {
			panic(log.NewLazySprintf("rechecking has not finished; cannot check new tx %v",
				types.Tx(tx).Hash()))
		}

		mem.resCbFirstTime(tx, txInfo, res)

		// update metrics
		mem.metrics.Size.Set(float64(mem.Size()))
		mem.metrics.SizeBytes.Set(float64(mem.SizeBytes()))

		// passed in by the caller of CheckTx, eg. the RPC
		if externalCb != nil {
			externalCb(res.GetCheckTx())
		}
	}
}

// Called from:
//   - resCbFirstTime (lock not held) if tx is valid
func (mem *CListMempool) addTx(memTx *mempoolTx) {
	e := mem.txs.PushBack(memTx)
	mem.txsMap.Store(memTx.tx.Key(), e)
	mem.txsBytes.Add(int64(len(memTx.tx)))
	mem.metrics.TxSizeBytes.Observe(float64(len(memTx.tx)))
}

// RemoveTxByKey removes a transaction from the mempool by its TxKey index.
// Called from:
//   - Update (lock held) if tx was committed
//   - resCbRecheck (lock not held) if tx was invalidated
func (mem *CListMempool) RemoveTxByKey(txKey types.TxKey) error {
	if elem, ok := mem.getCElement(txKey); ok {
		mem.txs.Remove(elem)
		elem.DetachPrev()
		mem.txsMap.Delete(txKey)
		tx := elem.Value.(*mempoolTx).tx
		mem.txsBytes.Add(int64(-len(tx)))
		return nil
	}
	return ErrTxNotFound
}

func (mem *CListMempool) isFull(txSize int) error {
	memSize := mem.Size()
	txsBytes := mem.SizeBytes()
	if memSize >= mem.config.Size || int64(txSize)+txsBytes > mem.config.MaxTxsBytes {
		return ErrMempoolIsFull{
			NumTxs:      memSize,
			MaxTxs:      mem.config.Size,
			TxsBytes:    txsBytes,
			MaxTxsBytes: mem.config.MaxTxsBytes,
		}
	}

	if mem.recheck.consideredFull() {
		return ErrRecheckFull
	}

	return nil
}

// callback, which is called after the app checked the tx for the first time.
//
// The case where the app checks the tx for the second and subsequent times is
// handled by the resCbRecheck callback.
func (mem *CListMempool) resCbFirstTime(
	tx []byte,
	txInfo TxInfo,
	res *abci.Response,
) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckTx:
		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(tx, r.CheckTx)
		}
		if (r.CheckTx.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Check mempool isn't full again to reduce the chance of exceeding the
			// limits.
			if err := mem.isFull(len(tx)); err != nil {
				// remove from cache (mempool might have a space later)
				mem.cache.Remove(tx)
				// use debug level to avoid spamming logs when traffic is high
				mem.logger.Debug(err.Error())
				mem.metrics.RejectedTxs.Add(1)
				return
			}

			// Check transaction not already in the mempool
			if e, ok := mem.txsMap.Load(types.Tx(tx).Key()); ok {
				memTx := e.(*clist.CElement).Value.(*mempoolTx)
				memTx.addSender(txInfo.SenderID)
				mem.logger.Debug(
					"transaction already there, not adding it again",
					"tx", types.Tx(tx).Hash(),
					"res", r,
					"height", mem.height.Load(),
					"total", mem.Size(),
				)
				mem.metrics.RejectedTxs.Add(1)
				return
			}

			memTx := &mempoolTx{
				height:    mem.height.Load(),
				gasWanted: r.CheckTx.GasWanted,
				tx:        tx,
			}
			memTx.addSender(txInfo.SenderID)
			mem.addTx(memTx)
			mem.logger.Debug(
				"added good transaction",
				"tx", types.Tx(tx).Hash(),
				"res", r,
				"height", mem.height.Load(),
				"total", mem.Size(),
			)
			mem.notifyTxsAvailable()
		} else {
			// ignore bad transaction
			mem.logger.Debug(
				"rejected bad transaction",
				"tx", types.Tx(tx).Hash(),
				"peerID", txInfo.SenderP2PID,
				"res", r,
				"err", postCheckErr,
			)
			mem.metrics.FailedTxs.Add(1)

			if !mem.config.KeepInvalidTxsInCache {
				// remove from cache (it might be good later)
				mem.cache.Remove(tx)
			}
		}

	default:
		// ignore other messages
	}
}

// callback, which is called after the app rechecked the tx.
//
// The case where the app checks the tx for the first time is handled by the
// resCbFirstTime callback.
func (mem *CListMempool) resCbRecheck(tx types.Tx, res *abci.ResponseCheckTx) {
	// Check whether tx is still in the list of transactions that can be rechecked.
	if !mem.recheck.findNextEntryMatching(&tx) {
		// Reached the end of the list and didn't find a matching tx; rechecking has finished.
		return
	}

	var postCheckErr error
	if mem.postCheck != nil {
		postCheckErr = mem.postCheck(tx, res)
	}

	if (res.Code != abci.CodeTypeOK) || postCheckErr != nil {
		// Tx became invalidated due to newly committed block.
		mem.logger.Debug("tx is no longer valid", "tx", tx.Hash(), "res", res, "postCheckErr", postCheckErr)
		if err := mem.RemoveTxByKey(tx.Key()); err != nil {
			mem.logger.Debug("Transaction could not be removed from mempool", "err", err)
		}
		if !mem.config.KeepInvalidTxsInCache {
			mem.cache.Remove(tx)
			mem.metrics.EvictedTxs.Add(1)
		}
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) TxsAvailable() <-chan struct{} {
	return mem.txsAvailable
}

func (mem *CListMempool) notifyTxsAvailable() {
	if mem.Size() == 0 {
		panic("notified txs available but mempool is empty!")
	}
	if mem.txsAvailable != nil && mem.notifiedTxsAvailable.CompareAndSwap(false, true) {
		// channel cap is 1, so this will send once
		select {
		case mem.txsAvailable <- struct{}{}:
		default:
		}
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Txs {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	var (
		totalGas    int64
		runningSize int64
	)

	// TODO: we will get a performance boost if we have a good estimate of avg
	// size per tx, and set the initial capacity based off of that.
	// txs := make([]types.Tx, 0, cmtmath.MinInt(mem.txs.Len(), max/mem.avgTxSize))
	txs := make([]types.Tx, 0, mem.txs.Len())
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		memTx := e.Value.(*mempoolTx)

		txs = append(txs, memTx.tx)

		dataSize := types.ComputeProtoSizeForTxs([]types.Tx{memTx.tx})

		// Check total size requirement
		if maxBytes > -1 && runningSize+dataSize > maxBytes {
			return txs[:len(txs)-1]
		}

		runningSize += dataSize

		// Check total gas requirement.
		// If maxGas is negative, skip this check.
		// Since newTotalGas < masGas, which
		// must be non-negative, it follows that this won't overflow.
		newTotalGas := totalGas + memTx.gasWanted
		if maxGas > -1 && newTotalGas > maxGas {
			return txs[:len(txs)-1]
		}
		totalGas = newTotalGas
	}
	return txs
}

// Safe for concurrent use by multiple goroutines.可供多个goroutines同时使用。
func (mem *CListMempool) ReapMaxTxs(max int) types.Txs {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	if max < 0 {
		max = mem.txs.Len()
	}

	txs := make([]types.Tx, 0, cmtmath.MinInt(mem.txs.Len(), max))
	for e := mem.txs.Front(); e != nil && len(txs) <= max; e = e.Next() {
		memTx := e.Value.(*mempoolTx)
		txs = append(txs, memTx.tx)
		fmt.Println("[未确认]tx 暂时不写入", len(txs), fmt.Sprintf("%x", memTx.tx)) //xiugai
	}
	return txs
}

var OldTx = map[[32]byte]int64{} //xiugai

// Lock() must be help by the caller during execution.
func (mem *CListMempool) Update(
	height int64,
	txs types.Txs,
	txResults []*abci.ExecTxResult,
	preCheck PreCheckFunc,
	postCheck PostCheckFunc,
) error {
	mem.logger.Debug("Update", "height", height, "len(txs)", len(txs))

	// Set height
	mem.height.Store(height)
	mem.notifiedTxsAvailable.Store(false)

	if preCheck != nil {
		mem.preCheck = preCheck
	}
	if postCheck != nil {
		mem.postCheck = postCheck
	}
	//numberPush := 0
	//numberRemove := 0
	numberError := 0
	for i, tx := range txs {
		if txResults[i].Code == abci.CodeTypeOK {
			// 将有效的已提交tx添加到缓存中（如果缺少）。
			// Add valid committed tx to the cache (if missing).
			_ = mem.cache.Push(tx)
			//numberPush += 1
		} else if !mem.config.KeepInvalidTxsInCache {
			//允许重新提交无效交易。
			// Allow invalid transactions to be resubmitted.
			mem.cache.Remove(tx)
			//numberRemove += 1
		}

		// Remove committed tx from the mempool.
		//
		// Note an evil proposer can drop valid txs!
		// Mempool before:
		//   100 -> 101 -> 102
		// Block, proposed by an evil proposer:
		//   101 -> 102
		// Mempool after:
		//   100
		// https://github.com/tendermint/tendermint/issues/3322.
		//新的删除方法
		if err := mem.RemoveTxByKey(tx.Key()); err != nil {
			numberError += 1
			//已提交的事务不在本地内存池中（不是错误）
			mem.logger.Debug("Committed transaction not in local mempool (not an error)",
				"key", tx.Key(),
				"error", err.Error())
		}
	}

	//tian
	var shuzu [][]byte       //xiugai5
	var shanShuZu [][32]byte //xiugai6
	geshu := cmtmath.MinInt(mem.txs.Len(), 999)
	for e := mem.txs.Front(); e != nil && geshu <= 999; e = e.Next() {
		linHash := TxKey(e.Value.(*mempoolTx).tx) //xiugai
		if OldTx[linHash] > 0 {
			if height-OldTx[linHash] >= 10 { //十个区块
				//mem.txs.Remove(e)
				fmt.Println("[很早就有]", "hash", fmt.Sprintf("%x", e.Value.(*mempoolTx).tx.Hash()), "height", OldTx[linHash])
				shanShuZu = append(shanShuZu, linHash)
				continue
			}
		} else {
			OldTx[linHash] = height //记录hash 高度//fmt.Println("OldTx", len(OldTx))
		}
		shuzu = append(shuzu, e.Value.(*mempoolTx).tx) //xiugai
	}
	//xiugai6
	if len(shanShuZu) > 0 {
		ii := 0
		for _, shanTx := range shanShuZu {
			if err := mem.RemoveTxByKey(shanTx); err != nil {
				ii += 1
				fmt.Println("删除成功", fmt.Sprintf("%x", shanTx))
			}
		}
		fmt.Println("强制删除", "数量", ii)
	} else {
		if len(OldTx) >= 9000 {
			fmt.Println("太多了，清除OldTx")
			OldTx = map[[32]byte]int64{}
		}
	}
	zhongzhuan.Xietxs(shuzu) //xiugai
	//fmt.Println("更新pool", height, "总txs", len(txs), "删除", int(len(txs)-numberPush-numberRemove), "缓存添加", numberPush, "缓存移除", numberRemove, "异常", numberError)
	fmt.Println("更新pool", "新块有_总删除", len(txs), "删除主私", numberError, "强制剩余", len(shuzu))
	// Recheck txs left in the mempool to remove them if they became invalid in the new state.
	if mem.config.Recheck {
		mem.recheckTxs()
	}

	// Notify if there are still txs left in the mempool.
	if mem.Size() > 0 {
		mem.notifyTxsAvailable()
	}

	// Update metrics
	mem.metrics.Size.Set(float64(mem.Size()))
	mem.metrics.SizeBytes.Set(float64(mem.SizeBytes()))

	return nil
}

// recheckTxs sends all transactions in the mempool to the app for re-validation. When the function
// returns, all recheck responses from the app have been processed.
func (mem *CListMempool) recheckTxs() {
	mem.logger.Debug("recheck txs", "height", mem.height.Load(), "num-txs", mem.Size())

	if mem.Size() <= 0 {
		return
	}

	mem.recheck.init(mem.txs.Front(), mem.txs.Back())

	// NOTE: globalCb may be called concurrently, but CheckTx cannot be executed concurrently
	// because this function has the lock (via Update and Lock).
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		tx := e.Value.(*mempoolTx).tx
		mem.recheck.numPendingTxs.Add(1)

		// Send a CheckTx request to the app. If we're using a sync client, the resCbRecheck
		// callback will be called right after receiving the response.
		_, err := mem.proxyAppConn.CheckTxAsync(context.TODO(), &abci.RequestCheckTx{
			Tx:   tx,
			Type: abci.CheckTxType_Recheck,
		})
		if err != nil {
			panic(fmt.Errorf("(re-)CheckTx request for tx %s failed: %w", log.NewLazySprintf("%v", tx.Hash()), err))
		}
	}

	// Flush any pending asynchronous recheck requests to process.
	mem.proxyAppConn.Flush(context.TODO())

	// Give some time to finish processing the responses; then finish the rechecking process, even
	// if not all txs were rechecked.
	select {
	case <-time.After(mem.config.RecheckTimeout):
		mem.recheck.setDone()
		mem.logger.Error("timed out waiting for recheck responses")
	case <-mem.recheck.doneRechecking():
	}

	if n := mem.recheck.numPendingTxs.Load(); n > 0 {
		mem.logger.Error("not all txs were rechecked", "not-rechecked", n)
	}
	mem.logger.Debug("done rechecking txs", "height", mem.height.Load(), "num-txs", mem.Size())
}

// The cursor and end pointers define a dynamic list of transactions that could be rechecked. The
// end pointer is fixed. When a recheck response for a transaction is received, cursor will point to
// the entry in the mempool corresponding to that transaction, thus narrowing the list. Transactions
// corresponding to entries between the old and current positions of cursor will be ignored for
// rechecking. This is to guarantee that recheck responses are processed in the same sequential
// order as they appear in the mempool.
type recheck struct {
	cursor        *clist.CElement // next expected recheck response
	end           *clist.CElement // last entry in the mempool to recheck
	doneCh        chan struct{}   // to signal that rechecking has finished successfully (for async app connections)
	numPendingTxs atomic.Int32    // number of transactions still pending to recheck
	isRechecking  atomic.Bool     // true iff the rechecking process has begun and is not yet finished
	recheckFull   atomic.Bool     // whether rechecking TXs cannot be completed before a new block is decided
}

func newRecheck() *recheck {
	return &recheck{
		doneCh: make(chan struct{}, 1),
	}
}

func (rc *recheck) init(first, last *clist.CElement) {
	if !rc.done() {
		panic("Having more than one rechecking process at a time is not possible.")
	}
	rc.cursor = first
	rc.end = last
	rc.numPendingTxs.Store(0)
	rc.isRechecking.Store(true)
}

// done returns true when there is no recheck response to process.
// Safe for concurrent use by multiple goroutines.
func (rc *recheck) done() bool {
	return !rc.isRechecking.Load()
}

// setDone registers that rechecking has finished.
func (rc *recheck) setDone() {
	rc.cursor = nil
	rc.recheckFull.Store(false)
	rc.isRechecking.Store(false)
}

// setNextEntry sets cursor to the next entry in the list. If there is no next, cursor will be nil.
func (rc *recheck) setNextEntry() {
	rc.cursor = rc.cursor.Next()
}

// tryFinish will check if the cursor is at the end of the list and notify the channel that
// rechecking has finished. It returns true iff it's done rechecking.
func (rc *recheck) tryFinish() bool {
	if rc.cursor == rc.end {
		// Reached end of the list without finding a matching tx.
		rc.setDone()
	}
	if rc.done() {
		// Notify that recheck has finished.
		select {
		case rc.doneCh <- struct{}{}:
		default:
		}
		return true
	}
	return false
}

// findNextEntryMatching searches for the next transaction matching the given transaction, which
// corresponds to the recheck response to be processed next. Then it checks if it has reached the
// end of the list, so it can finish rechecking.
//
// The goal is to guarantee that transactions are rechecked in the order in which they are in the
// mempool. Transactions whose recheck response arrive late or don't arrive at all are skipped and
// not rechecked.
func (rc *recheck) findNextEntryMatching(tx *types.Tx) bool {
	found := false
	for ; !rc.done(); rc.setNextEntry() {
		expectedTx := rc.cursor.Value.(*mempoolTx).tx
		if bytes.Equal(*tx, expectedTx) {
			// Found an entry in the list of txs to recheck that matches tx.
			found = true
			rc.numPendingTxs.Add(-1)
			break
		}
	}

	if !rc.tryFinish() {
		// Not finished yet; set the cursor for processing the next recheck response.
		rc.setNextEntry()
	}
	return found
}

// doneRechecking returns the channel used to signal that rechecking has finished.
func (rc *recheck) doneRechecking() <-chan struct{} {
	return rc.doneCh
}

// setRecheckFull sets recheckFull to true if rechecking is still in progress. It returns true iff
// the value of recheckFull has changed.
func (rc *recheck) setRecheckFull() bool {
	rechecking := !rc.done()
	recheckFull := rc.recheckFull.Swap(rechecking)
	return rechecking != recheckFull
}

// consideredFull returns true iff the mempool should be considered as full while rechecking is in
// progress.
func (rc *recheck) consideredFull() bool {
	return rc.recheckFull.Load()
}
