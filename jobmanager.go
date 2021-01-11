package jobmanager

import (
	"errors"
	"sync"
)

var (
	// ErrJobCancelled job is cancelled
	ErrJobCancelled = errors.New("jobCancelled")
	// ErrGroupExists job group exists
	ErrGroupExists = errors.New("groupExists")
	// ErrGroupNotExists job group does not exists
	ErrGroupNotExists = errors.New("groupNotExists")
)

// JobsManager struct
type JobsManager struct {
	m              sync.Mutex
	fullJobList    map[string]*Job
	groupedJobList map[string][]*Job
	workerChannel  chan *Job
	cancelChannel  chan *Job
	workerSize     int
}

// NewJobManager method
func NewJobManager(workerSize int) *JobsManager {
	j := &JobsManager{
		fullJobList:    make(map[string]*Job),
		groupedJobList: make(map[string][]*Job),
		workerChannel:  make(chan *Job),
		cancelChannel:  make(chan *Job),
		workerSize:     workerSize,
	}

	j.startManager()
	return j
}

// startManager method
func (j *JobsManager) startManager() {
	for i := 0; i < j.workerSize; i++ {
		go j.registerWorker()
	}
}

// Run method
func (j *JobsManager) Run(jobFun interface{}, params ...interface{}) (*Job, error) {
	job, err := NewJob(jobFun, params...)
	if err != nil {
		return nil, err
	}

	j.RunJob(job)

	return job, nil
}

// RunAndWait method
func (j *JobsManager) RunAndWait(jobFun interface{}, params ...interface{}) (*Job, error) {
	job, err := NewJob(jobFun, params...)
	if err != nil {
		return nil, err
	}

	j.RunJobAndWait(job)

	return job, nil
}

// RunJob method
func (j *JobsManager) RunJob(job *Job) *Job {
	job.resetState()

	j.m.Lock()
	j.fullJobList[job.ID] = job
	j.m.Unlock()

	if !job.isCancelled() {
		j.workerChannel <- job
	}

	return job
}

// RunJobAndWait method
func (j *JobsManager) RunJobAndWait(job *Job) *Job {
	j.RunJob(job)
	job.wait()

	return job
}

// RunJobsInSerial method
func (j *JobsManager) RunJobsInSerial(jobs ...*Job) []*Job {
	for _, job := range jobs {
		j.RunJobAndWait(job)
	}

	return jobs
}

// RunJobsInParallel method
func (j *JobsManager) RunJobsInParallel(jobs ...*Job) []*Job {
	var wg sync.WaitGroup
	wg.Add(len(jobs))

	for _, job := range jobs {
		go func(job *Job) {
			j.RunJobAndWait(job)
			wg.Done()
		}(job)
	}

	wg.Wait()

	return jobs
}

// StopJob method
func (j *JobsManager) StopJob(job *Job) *Job {
	j.m.Lock()
	defer j.m.Unlock()

	j.cancelChannel <- job

	return job
}

// WaitForJobs method
func (j *JobsManager) WaitForJobs(jobs ...*Job) []*Job {
	for _, job := range jobs {
		job.wait()
	}

	return jobs
}

// GetJobs method
func (j *JobsManager) GetJobs() []*Job {
	result := make([]*Job, 0, len(j.fullJobList))

	for _, job := range j.fullJobList {
		result = append(result, job)
	}

	return result
}

// CreateGroup method
func (j *JobsManager) CreateGroup(tag string, jobs ...*Job) error {

	if j.groupedJobList[tag] != nil {
		return ErrGroupExists
	}

	for _, job := range jobs {
		j.groupedJobList[tag] = append(j.groupedJobList[tag], job)
	}

	return nil
}

// GetJobsByGroup method
func (j *JobsManager) GetJobsByGroup(tag string) ([]*Job, error) {

	if j.groupedJobList[tag] == nil {
		return nil, ErrGroupNotExists
	}

	result := make([]*Job, 0, len(j.groupedJobList[tag]))

	for _, job := range j.groupedJobList[tag] {
		result = append(result, job)
	}

	return result, nil
}

func (j *JobsManager) registerWorker() {
	for {
		select {
		case job := <-j.workerChannel:
			job.Status = Running
			job.result.value, job.result.err = job.run()
			job.Status = Done
			job.closeDoneChannel()

		case job := <-j.cancelChannel:
			job.Status = Cancelled
			job.result = JobResult{
				err: ErrJobCancelled,
			}
			job.closeDoneChannel()
		}
	}
}
