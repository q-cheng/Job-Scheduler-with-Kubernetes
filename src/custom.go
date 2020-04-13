package allocate

import (
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"time"
)

func customFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	jobTimeDic := make(map[*api.JobInfo]int)

	var flag bool
	var jobTimeBindArray []jobTimeBind

	for _, job := range jobs {
		if job.Type == "GPU" {
			flag, _ = GPUJobs(job, nodes)
		} else {
			flag, _ = MPIJobs(job, nodes)
		}
		if flag == false {
			temp := job.SlowDuration + int(time.Now().Unix()) - int(job.CreationTime.ProtoTime().Seconds)
			if temp < 200 {
				jobTimeDic[job] = temp
			}
		} else {
			temp := job.FastDuration + int(time.Now().Unix()) - int(job.CreationTime.ProtoTime().Seconds)
			if temp < 200 {
				jobTimeDic[job] = temp
			}
		}
	}
	if len(jobTimeDic) > 0 {
		jobTimeBindArray = sortJobTimeList(jobTimeDic)
	}
	for len(jobTimeBindArray) > 0 {
		job := jobTimeBindArray[0].Job
		if job.Type == "GPU" {
			_, allocation = GPUJobs(job, nodes)
		} else {
			_, allocation = MPIJobs(job, nodes)
		}
		if len(allocation) == len(job.TaskStatusIndex[api.Pending]) {
			break
		} else {
			jobTimeBindArray = jobTimeBindArray[1:]
			allocation = make(map[*api.TaskInfo]*api.NodeInfo)
		}
	}
	return allocation
}
