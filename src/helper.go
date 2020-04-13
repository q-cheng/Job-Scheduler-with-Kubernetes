package allocate

import (
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"math/rand"
	"sort"
)

func GPUJobs(job *api.JobInfo, nodes []*api.NodeInfo) (bool, map[*api.TaskInfo]*api.NodeInfo) {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	i := 0
	fastFlag := true
	for _, task := range job.TaskStatusIndex[api.Pending] {
		for i < len(nodes) && len(nodes[i].Tasks) > 0 {
			i++
		}
		if i>=len(nodes) {
			// out of nodes
			break
		}
		if nodes[i].GPU == true {
			allocation[task] = nodes[i]
		}
		i++
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		fastFlag = false
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
		i = 0
		for _, task := range job.TaskStatusIndex[api.Pending] {
			for i < len(nodes) && len(nodes[i].Tasks) > 0 {
				i++
			}
			if i>=len(nodes) {
				// out of nodes
				break
			}
			if nodes[i].GPU == false {
				allocation[task] = nodes[i]
			}
			i++
		}
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		fastFlag = false
		allocation = randomAllocation(job, nodes)
	}
	return fastFlag, allocation
}

func MPIJobs(job *api.JobInfo, nodes []*api.NodeInfo) (bool, map[*api.TaskInfo]*api.NodeInfo) {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	i := 0
	fastFlag := true
	for j:=1; j <= 4; j++ {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			for i < len(nodes) && len(nodes[i].Tasks) > 0 {
				i++
			}
			if i >= len(nodes) {
				// out of nodes
				break
			}
			if nodes[i].Rack == j {
				allocation[task] = nodes[i]
			}
			i++
		}
		if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
			allocation = make(map[*api.TaskInfo]*api.NodeInfo)
			i = 0
		} else {
			break
		}
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		fastFlag = false
		allocation = randomAllocation(job, nodes)
	}
	return fastFlag, allocation
}

func randomAllocation(job *api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo  {
		allocation := make(map[*api.TaskInfo]*api.NodeInfo)
		tmp := 0
		var nodeArray []int
		for tmp <= len(nodes) - 1 {
			nodeArray = append(nodeArray, tmp)
			tmp = tmp + 1
		}
		i := rand.Intn(len(nodeArray))
		for _, task := range job.TaskStatusIndex[api.Pending] {
			for len(nodes[nodeArray[i]].Tasks) > 0 {
				nodeArray[i] = nodeArray[len(nodeArray)-1]
				nodeArray[len(nodeArray)-1] = 0
				nodeArray = nodeArray[:len(nodeArray)-1]
				if len(nodeArray) >= 1 {
					i = rand.Intn(len(nodeArray))
				} else {
					break
				}
			}
			if len(nodeArray) == 0 {
				break
			}
			allocation[task] = nodes[nodeArray[i]]
			nodeArray[i] = nodeArray[len(nodeArray)-1]
			nodeArray[len(nodeArray)-1] = 0
			nodeArray = nodeArray[:len(nodeArray)-1]
			if len(nodeArray) == 0 {
				break
			}
			i = rand.Intn(len(nodeArray))
		}
		if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		// could not allocate all the tasks, return empty allocation
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
	}
	return allocation
}

func sortJobTimeList(jobTimeMap map[*api.JobInfo]int) []jobTimeBind {
	var retBindList []jobTimeBind
	for job, time := range jobTimeMap {
		retBindList = append(retBindList, jobTimeBind{job, time})
	}
	sort.Slice(retBindList, func(i, j int) bool {
		return retBindList[i].Time < retBindList[j].Time
	})
	return retBindList
}


type jobTimeBind struct {
	Job   *api.JobInfo
	Time int
}
