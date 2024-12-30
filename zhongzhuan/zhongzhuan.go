package zhongzhuan

//xiugai
import (
	"sync"
)

var suo sync.RWMutex
var suotxt sync.RWMutex
var suokuai sync.RWMutex

var ChaXun bool
var XvJiaHash string
var ZhenShiHash string

// 数组类
var shuzu [][]byte

func Dutxs() [][]byte {
	suo.RLock()         //别人不能写
	defer suo.RUnlock() //别人不能写
	return shuzu
}

func Xietxs(xie [][]byte) {
	suo.Lock()         //别人不能读写
	defer suo.Unlock() //别人不能读写
	shuzu = xie
}

//fmt.Printf("时间戳（毫秒）：%v;\n",time.Now().UnixNano() / 1e6)
//fmt.Printf("时间戳（秒）：%v;\n", time.Now().Unix())
//fmt.Printf("时间戳（纳秒）：%v;\n",time.Now().UnixNano())
//fmt.Printf("时间戳（毫秒）：%v;\n",time.Now().UnixNano() / 1e6)
//fmt.Printf("时间戳（纳秒转换为秒）：%v;\n",time.Now().UnixNano() / 1e9)

//string转成int：		int, _ := strconv.Atoi(string)
//int转成string：		string := strconv.Itoa(int)
//string转成int64：	int64, _:= strconv.ParseInt(string, 10, 64)
//int64转成string：	string := strconv.FormatInt(int64,10)

var kuaishuzu [][]byte
var zuixinqukuai int64

func KuaiDutxs() ([][]byte, int64) {
	suokuai.RLock()         //别人不能写
	defer suokuai.RUnlock() //别人不能写
	return kuaishuzu, zuixinqukuai
}

func KuaiXietxs(xie [][]byte, qukuai int64) {
	suokuai.Lock()         //别人不能读写
	defer suokuai.Unlock() //别人不能读写
	kuaishuzu = xie
	zuixinqukuai = qukuai
}
