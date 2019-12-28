package mcpa

import (
    "sync"
    "time"
)

var (
    lastTime     int64 = time.Now().UnixNano() / 1000000 //上次时间戳， 毫秒，占41位
    serialNum    int64 = 0                               //序列号 占12位
    maxSerialNum int64 = 4095                            //序列号最大值
    lock         sync.Mutex
)

//生成函数ID
func genFuncId() (funcId int64, err error) {
    lock.Lock()
    defer lock.Unlock()

    curTime := time.Now().UnixNano() / 1000000
    if curTime == lastTime {
        serialNum++
        if serialNum > maxSerialNum {
            time.Sleep(time.Microsecond)
            curTime = time.Now().UnixNano() / 1000000
            lastTime = curTime
            serialNum = 0
        }
    } else if curTime > lastTime {
        lastTime = curTime
        serialNum = 0
    } else {
        return 0, CLOCK_CALLBACK_ERROR
    }

    funcId = ((curTime & 0x1FFFFFFFFFF) << 12) | serialNum
    return funcId, nil
}