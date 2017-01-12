/* Description
*  基于连接的服务节点管理，在直接随机访问集群服务时定期检查服务节点是否可用，及时把不可用服务节点从配置中移除。
*  针对被判断为不可用的服务节点，定期做重试测试，通过建立TCP连接来验证服务节点是否恢复可用，如果可用，则
*  将其放入可用节点列表。
*  可以注册两组服务节点，一组为primary，默认被使用；一组为备份，当primary服务节点不可用时，使用备份节点。
*  检查和重试的周期可配置
*
*  注：节点必须包括host + port
*
 */

package servernodemanager

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	NODESCNT = 2
	TIMEOUT  = 100
	DEBUG    = false
)

type NodeMgrCfg struct {
	FailedCnt   int
	MinNeed     int
	CheckPeriod int
	RetryPeriod int
	Hosts       []string
	Backup      []string
}

type ServerNodes struct {
	Usables   []string          //可用节点列表
	Unusables []string          //不可用节点列表
	FailedCnt map[string]*int32 // 节点错误数统计
}

type NodeManager struct {
	Nodes       [NODESCNT]*ServerNodes
	Primary     int
	MinNeed     int           //服务需要的最少node数
	CheckPeriod time.Duration // 失败检查的周期和失败数标准
	FailedCnt   int           // 设定的不可用错误数标准
	RetryPeriod time.Duration // 重试的周期间隔
	lock        *sync.RWMutex
	retrylock   *sync.Mutex
}

func Debug(v ...interface{}) {
	if DEBUG {
		str := fmt.Sprint(v...)
		fmt.Println(str)
	}
}

// 对外接口，创建一个node管理器
// 参数:
//     cfg *NodeMgrCfg --节点管理相关的配置
// 返回:
//    节点管理器指针
func CreateNodeManager(cfg *NodeMgrCfg) *NodeManager {
	nm := NewNodeManager(cfg.FailedCnt, cfg.MinNeed, time.Duration(cfg.CheckPeriod), time.Duration(cfg.RetryPeriod))
	nm.RegisterNode(cfg.Hosts, true)
	nm.RegisterNode(cfg.Backup, false)
	return nm
}

// 对外接口，创建一个node管理器
// 参数:
//     checkPeriod --对节点的检查更新周期, 为了保证能及时摘除坏节点，该参数应比较小，默认值1s
//     retryPeriod --对坏节点的重试周期，默认值10s
//     failedCnt   -- checkPeriod时间段内出现failedCnt次失败就认为该节点不可用
//     minNeed     -- 支持服务需要的最小节点数，如果可用节点数小于该值，使用不可用节点补充
// 返回:
//    节点管理器指针
func NewNodeManager(failedCnt, minNeed int, checkPeriod, retryPeriod time.Duration) *NodeManager {
	if checkPeriod < 1 {
		checkPeriod = 1
	}

	if retryPeriod < 1 {
		retryPeriod = 10
	}

	sm := new(NodeManager)
	sm.Primary = 0
	sm.CheckPeriod = checkPeriod
	sm.RetryPeriod = retryPeriod
	sm.FailedCnt = failedCnt
	sm.MinNeed = minNeed
	sm.lock = new(sync.RWMutex)
	sm.retrylock = new(sync.Mutex)

	go run(sm)

	return sm
}
func (sm *NodeManager) UpdateNodes(updateNodes []string, is_primary bool) (is_suc bool, err error) {

	sm.lock.RLock()
	primary := sm.Primary
	nodes := sm.Nodes[primary]
	sm.lock.RUnlock()
	if nodes == nil {
		sm.RegisterNode(updateNodes, is_primary)
		is_suc = true
		return
	}

	UsablesMap := make(map[string]bool)
	for _, node := range nodes.Usables {
		UsablesMap[node] = true
	}
	UnusablesMap := make(map[string]bool)
	for _, node := range nodes.Unusables {
		UnusablesMap[node] = true
	}

	/**
	1. remainNodes是待更新节点updateNodes与Usables的交集
	2. failNodes是待更新节点updateNodes与Unusables的交集
	3. incNodes是待更新节点updateNodes与Usables的差集
	updateNodes = remainNodes+failNodes+incNodes
	*/
	var remainNodes []string
	var incNodes []string
	var failNodes []string
	for _, updateNode := range updateNodes {
		if _, ok := UnusablesMap[updateNode]; ok {
			failNodes = append(failNodes, updateNode)
			continue
		}
		if _, ok := UsablesMap[updateNode]; ok {
			remainNodes = append(remainNodes, updateNode)
			continue
		}
		incNodes = append(incNodes, updateNode)
	}
	if len(incNodes) > 0 {
		//1. 只有当有新增节点时才会进行更新，只是failNodes有变化时不会进行更新
		//2. 并且由于RegisterNode是将nodes作为新的节点列表，因此要进行append
		newnodes := append(remainNodes, incNodes...)
		sm.updateUsableFailNodes(newnodes, failNodes, is_primary)
	}
	is_suc = true

	return
}

// 与RegisterNode相比，唯一的不同是RegisterNode会将原有不可用的节点置为空
// 而OnlyUpdateUsableNodes会将原有不可用的节点保留，只更新Usables
func (sm *NodeManager) updateUsableFailNodes(newnodes []string, failNodes []string, primary bool) {
	if len(newnodes) == 0 {
		return
	}

	var index int = 0
	// primary 节点放在index=0的ServerNodes中
	// 双活备份节点方在index=1的ServerNodes中
	if primary == false {
		index = 1
	}

	sn := new(ServerNodes)
	sn.Usables = make([]string, 0, 1)
	sn.Unusables = make([]string, 0, 1)
	sn.FailedCnt = make(map[string]*int32)

	for _, node := range newnodes {
		sn.Usables = append(sn.Usables, node)
		sn.FailedCnt[node] = new(int32)
	}

	//与RegisterNode的唯一区别是：新的sn的不可用节点来自failNodes，并且FailedCnt维持不变
	for _, node := range failNodes {
		sn.Unusables = append(sn.Unusables, node)
		sn.FailedCnt[node] = sm.Nodes[index].FailedCnt[node]
	}

	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.Nodes[index] = sn
}

// 在生成Nodemanager后，需要注册所管理的节点
// 目前能注册一份主节点 primary=true, 一份备用节点 primary=false
// 参数：
//	    nodes --需要被管理的ip地址
//
func (sm *NodeManager) RegisterNode(nodes []string, primary bool) {
	if len(nodes) == 0 {
		return
	}

	var index int = 0
	// primary 节点放在index=0的ServerNodes中
	// 双活备份节点方在index=1的ServerNodes中
	if primary == false {
		index = 1
	}

	sn := new(ServerNodes)
	sn.Usables = make([]string, 0, 1)
	sn.Unusables = make([]string, 0, 1)
	sn.FailedCnt = make(map[string]*int32)

	for _, node := range nodes {
		sn.Usables = append(sn.Usables, node)
		sn.FailedCnt[node] = new(int32)
	}

	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.Nodes[index] = sn
}

// 对外接口，获取一个节点
// 返回:
//    节点和错误，使用节点前需要判断err
func (sm *NodeManager) GetNode() (string, error) {
	var err error
	var node string
	sm.lock.RLock()
	primary := sm.Primary
	nodes := sm.Nodes[primary]
	sm.lock.RUnlock()

	if nodes == nil {
		err = errors.New(fmt.Sprintf("No host registered"))
		return "", err
	}

	if size := len(nodes.Usables); size > 0 {
		node = nodes.Usables[rand.Intn(size)]
	} else {
		err = errors.New(fmt.Sprintf("No usable host!"))
	}

	return node, err
}

func (sm *NodeManager) SetValid(node string, valid bool) error {
	if !valid {
		return sm.SetInvalid(node)
	}
	return nil
}

// 对外接口，设置一个节点是否可用
// 需要在节点服务返回处执行
// 该函数需要写锁，需要尽量保证写锁早释放
func (sm *NodeManager) SetInvalid(node string) error {
	var err error
	if len(node) == 0 {
		err = errors.New(fmt.Sprint("invalid node"))
		return err
	}

	sm.lock.RLock()
	nodes := sm.Nodes[sm.Primary]
	sm.lock.RUnlock()

	if cnt, ok := nodes.FailedCnt[node]; ok {
		atomic.AddInt32(cnt, 1)
	} else {
		err = errors.New(fmt.Sprint("the node not registered"))
	}

	return err
}

func (sm *NodeManager) nodeServerCheck(primary int) (int, bool) {
	var hasUnusable bool = false
	Debug("nodeServerCheck, index = ", primary)
	currentNodes := sm.Nodes[primary]
	if currentNodes == nil {
		return 0, hasUnusable
	}

	cmpCnt := int32(sm.FailedCnt)

	nodes := new(ServerNodes)
	nodes.FailedCnt = make(map[string]*int32)
	nodes.Usables = make([]string, 0, 1)
	nodes.Unusables = make([]string, 0, 1)

	for k, v := range currentNodes.FailedCnt {
		value := atomic.LoadInt32(v)
		Debug(k, " FailedCnt = ", value)
		if value > cmpCnt {
			nodes.Unusables = append(nodes.Unusables, k)
		} else {
			nodes.Usables = append(nodes.Usables, k)
		}
		nodes.FailedCnt[k] = new(int32)
		*(nodes.FailedCnt[k]) = value
	}

	// if the usable nodes is not enough, add some unusable ones to balance the traffic
	usables := len(nodes.Usables)
	if usables < sm.MinNeed {
		hasUnusable = true
	}
	Debug("index = ", primary, ", usables = ", usables)
	for usables < sm.MinNeed {
		if len(nodes.Unusables) == 0 {
			break
		}
		i := rand.Intn(len(nodes.Unusables))
		node := nodes.Unusables[i]
		nodes.Unusables = append(nodes.Unusables[:i], nodes.Unusables[i+1:]...)
		nodes.Usables = append(nodes.Usables, node)
		usables++
	}

	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.Nodes[primary] = nodes

	return usables, hasUnusable
}

// 通过check节点的FailedCnt确定节点是否可用
// 当可用节点数小于minNeed时，将部分不可用节点补充到可用节点列表
// 主节点和备份节点分开检查
func (sm *NodeManager) nodeCheck() {
	sm.retrylock.Lock()
	defer sm.retrylock.Unlock()
	//var validCnt    [NODESCNT]int
	var hasUnusable [NODESCNT]bool
	var usables [NODESCNT]int
	for i := 0; i < NODESCNT; i++ {
		usables[i], hasUnusable[i] = sm.nodeServerCheck(i)
	}
	sm.lock.Lock()
	defer sm.lock.Unlock()
	// 选择节点的策略是优先使用primary节点，如果primary节点不够用则使用备份节点
	switch {
	case usables[0] > 0 && hasUnusable[0] == false:
		sm.Primary = 0
		break
	case usables[1] > 0 && hasUnusable[1] == false:
		sm.Primary = 1
		break
	default:
		sm.Primary = 0
	}
	Debug("sm.Primary = ", sm.Primary)
}

// 超时模式建立TCP连接
func setupConnect(node string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", node, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

// 重试不可用节点，如果能建立TCP连接，则认为节点可用，将其FailedCnt设置为0
func (sm *NodeManager) invalidNodeRetry(primary int) {
	Debug("invalidNodeRetry, index = ", primary)
	nodes := sm.Nodes[primary]
	if nodes == nil || len(nodes.Unusables) == 0 {
		return
	}

	for _, node := range nodes.Unusables {
		cnt := nodes.FailedCnt[node]
		if ok := setupConnect(node, TIMEOUT*time.Millisecond); ok {
			value := atomic.LoadInt32(cnt)
			atomic.AddInt32(cnt, -1*value)
		}

	}
}

// 定期重试被置失效的节点，如果重试结果OK，下一个nodeCheck周期内恢复为valid
// 重试的方式是建立一条TCP连接，如果能成功，则认为server是可用的
func (sm *NodeManager) nodeRetry() {
	sm.retrylock.Lock()
	defer sm.retrylock.Unlock()
	for i := 0; i < NODESCNT; i++ {
		sm.invalidNodeRetry(i)
	}
}

func run(sm *NodeManager) {
	timerCheck := time.NewTicker(time.Second * sm.CheckPeriod)
	timerRetry := time.NewTicker(time.Second * sm.RetryPeriod)

	for {
		select {
		case <-timerRetry.C:
			go sm.nodeRetry()

		case <-timerCheck.C:
			go sm.nodeCheck()
		}
	}
}
