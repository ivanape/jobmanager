package jobmanager

import (
	"errors"
	"github.com/google/uuid"
	"sync"
)

var (
	// ErrJobCancelled job is cancelled
	ErrJobCancelled = errors.New("jobCancelled")
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

// NewJob method
func (j *JobsManager) NewJob(jobFun interface{}, params ...interface{}) (*Job, error) {
	job := &Job{
		ID:      uuid.New().String(),
		Status:  Pending,
		funcs:   make(map[string]interface{}),
		fparams: make(map[string][]interface{}),
		done:    make(chan interface{}),
	}

	err := job.do(jobFun, params...)

	return job, err
}

// AddJobTag method
func (j *JobsManager) AddJobTag(job *Job, tag string) *Job {
	return job.addTag(tag)
}

// JobContainsTag method
func (j *JobsManager) JobContainsTag(job *Job, tag string) bool {
	for _, jobTag := range job.Tags {
		if tag == jobTag {
			return true
		}
	}

	return false
}

// Run method
func (j *JobsManager) Run(jobFun interface{}, params ...interface{}) (*Job, error) {
	job, err := j.NewJob(jobFun, params...)
	if err != nil {
		return nil, err
	}

	j.RunJob(job)

	return job, nil
}

// RunAndWait method
func (j *JobsManager) RunAndWait(jobFun interface{}, params ...interface{}) (*Job, error) {
	job, err := j.NewJob(jobFun, params...)
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

// AddJobsToGroup method
func (j *JobsManager) AddJobsToGroup(tag string, jobs ...*Job) error {

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

// StopJobsByGroup method
func (j *JobsManager) StopJobsByGroup(tag string) error {

	if j.groupedJobList[tag] == nil {
		return ErrGroupNotExists
	}

	for _, job := range j.groupedJobList[tag] {
		j.StopJob(job)
	}

	return nil
}

func (j *JobsManager) registerWorker() {
	for {
		select {
		case job := <-j.workerChannel:
			job.Status = Running
			job.Result.Value, job.Result.Err = job.run()
			job.Status = Done
			job.closeDoneChannel()

		case job := <-j.cancelChannel:
			job.Status = Cancelled
			job.Result = JobResult{
				Err: ErrJobCancelled,
			}
			job.closeDoneChannel()
		}
	}
}

// startManager method
func (j *JobsManager) startManager() {
	for i := 0; i < j.workerSize; i++ {
		go j.registerWorker()
	}
}
