package allocate

import (
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"sort"
)

func GPUJobs(job *api.JobInfo, nodes []*api.NodeInfo) (bool, map[*api.TaskInfo]*api.NodeInfo) {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	i := 0
	fastFlag := true
	for _, task := range job.TaskStatusIndex[api.Pending] {
		for i < len(nodes) && (len(nodes[i].Tasks) > 0 || nodes[i].GPU == false) {
			i++
		}
		if i>=len(nodes) {
			// out of nodes
			break
		}
		allocation[task] = nodes[i]
		i++
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		fastFlag = false
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
		i = 0
		for _, task := range job.TaskStatusIndex[api.Pending] {
			for i < len(nodes) && (len(nodes[i].Tasks) > 0 || nodes[i].GPU == true) {
				i++
			}
			if i>=len(nodes) {
				// out of nodes
				break
			}
			allocation[task] = nodes[i]
			i++
		}
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
			allocation[task] = nodes[i]
			i++
		}
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		fastFlag = false
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
	}
	return fastFlag, allocation
}

func MPIJobs(job *api.JobInfo, nodes []*api.NodeInfo) (bool, map[*api.TaskInfo]*api.NodeInfo) {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	i := 0
	fastFlag := true
	for j:=1; j <= 4; j++ {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			for i < len(nodes) && (len(nodes[i].Tasks) > 0 || nodes[i].Rack != j) {
				i++
			}
			if i >= len(nodes) {
				// out of nodes
				break
			}
			allocation[task] = nodes[i]
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
			allocation[task] = nodes[i]
			i++
		}
	}
	if len(job.TaskStatusIndex[api.Pending]) != len(allocation) {
		fastFlag = false
		allocation = make(map[*api.TaskInfo]*api.NodeInfo)
	}
	return fastFlag, allocation
}

func sortJobTimeList(nodeTimeMap map[*api.JobInfo]int) []jobTimeBind {
	var retBindList []jobTimeBind
	for node, time := range nodeTimeMap {
		retBindList = append(retBindList, jobTimeBind{node, time})
	}
	sort.SliceStable(retBindList, func(i, j int) bool {
		return retBindList[i].Time < retBindList[j].Time
	})
	return retBindList
}


type jobTimeBind struct {
	Job   *api.JobInfo
	Time int
}
