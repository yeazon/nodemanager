package servernodemanager

import (
	"fmt"
//	"servernodemanager"
	"time"
	_ "errors"
	"sync"
	"math/rand"
	"testing"
)

var nodes []string
var nodes1 []string

func init() {
	nodes = append(nodes, "0.0.0.1")
	nodes = append(nodes, "0.0.0.2")
	nodes = append(nodes, "0.0.0.3")
	nodes = append(nodes, "0.0.0.4")
	nodes = append(nodes, "0.0.0.5")

	nodes1 = append(nodes1, "1.1.1.1")
	nodes1 = append(nodes1, "1.1.1.2")
	nodes1 = append(nodes1, "1.1.1.3")
	nodes1 = append(nodes1, "1.1.1.4")
	nodes1 = append(nodes1, "1.1.1.5")
}

//对外接口，打印可用节点列表和不可用节点列表
func PrintUsableNode(sm *NodeManager) {
	fmt.Println("Usable nodes:")
	for i := 0; i < 2; i++ {
		nodes := sm.Nodes[i]
		if nodes == nil {
			continue
		}
		for index, v := range nodes.Usables {
			fmt.Println(index, ": ", v)
		}
	}
}

func PrintUnusableNode(sm *NodeManager) {
	fmt.Println("Unusable nodes:")
	for i := 0; i < 2; i++ {
		nodes := sm.Nodes[i]
		if nodes == nil {
			continue
		}
		for index, v := range nodes.Unusables {
			fmt.Println(index, ": ", v)
		}
	}
}

func Test(t *testing.T) {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
	}()

	sm := NewNodeManager(0, 3, 5, 30)
	sm.RegisterNode(nodes, true)
	sm.RegisterNode(nodes1,false)
	for i := 0; i < 100; i++ {
		go dotest(sm)
	}

	wg.Wait()
	PrintUsableNode(sm)
	PrintUnusableNode(sm)
	t.Log("Testing done")
}

func dotest(sm *NodeManager) {
	size := 10000
	for cnt := 0; cnt < 10; cnt++ {
		ip, err := sm.GetNode()
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("step 1 ip gotten: ", ip)
		start := time.Now().UnixNano() / 1000
		rd := rand.Intn(size)
		if rd > 800 {
			sm.SetValid(ip, false)
		}
		end := time.Now().UnixNano() / 1000
		cost := end - start
		fmt.Printf("step 1 cost: %dus\n", cost)
		time.Sleep(time.Second * 1)

		fmt.Println("")
		ip, err = sm.GetNode()
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("step 2 ip gotten: ", ip)
		start = time.Now().UnixNano() / 1000
		rd = rand.Intn(size)
		if rd > 800 {
			sm.SetValid(ip, false)
		}
		end = time.Now().UnixNano() / 1000
		cost = end - start
		fmt.Printf("step 2 cost: %dus\n", cost)
		time.Sleep(time.Second * 1)

		fmt.Println("")
		ip, err = sm.GetNode()
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("step 3 ip gotten: ", ip)
		start = time.Now().UnixNano() / 1000
		rd = rand.Intn(size)
		if rd > 800 {
			sm.SetValid(ip, false)
		}
		end = time.Now().UnixNano() / 1000
		cost = end - start
		fmt.Printf("step 3 cost: %dus\n", cost)
		time.Sleep(time.Second * 1)

		fmt.Println("")
		ip, err = sm.GetNode()
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("step 4 ip gotten: ", ip)
		start = time.Now().UnixNano() / 1000
		rd = rand.Intn(size)
		if rd > 500 {
			sm.SetValid(ip, false)
		}
		end = time.Now().UnixNano() / 1000
		cost = end - start
		fmt.Printf("step 4 cost: %dus\n", cost)
		time.Sleep(time.Second * 1)
		PrintUsableNode(sm)
		PrintUnusableNode(sm)
		fmt.Println("")
	}
	//PrintUsableNode(sm)
	//PrintUnusableNode(sm)
	//fmt.Println("")
}

