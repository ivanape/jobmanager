package jobmanager

import (
	"errors"
	"sync"
)

var (
	errCancelled = errors.New("jobCancelled")
)

// JobsManager struct
type JobsManager struct {
	m             sync.Mutex
	jobList       map[string]*Job
	workerChannel chan *Job
	cancelChannel chan *Job
	workerSize    int
}

// NewJobManager method
func NewJobManager(workerSize int) *JobsManager {
	j := &JobsManager{
		jobList:       make(map[string]*Job),
		workerChannel: make(chan *Job),
		cancelChannel: make(chan *Job),
		workerSize:    workerSize,
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
	job := NewJob()
	err := job.Do(jobFun, params...)
	if err != nil {
		return nil, err
	}

	j.RunJob(job)

	return job, nil
}

// RunAndWait method
func (j *JobsManager) RunAndWait(jobFun interface{}, params ...interface{}) (*Job, error) {
	job := NewJob()
	err := job.Do(jobFun, params...)
	if err != nil {
		return nil, err
	}

	j.RunJobAndWait(job)

	return job, nil
}

// RunJob method
func (j *JobsManager) RunJob(job *Job) *Job {
	j.m.Lock()
	j.jobList[job.ID] = job
	j.m.Unlock()

	j.workerChannel <- job

	return job
}

// RunJobAndWait method
func (j *JobsManager) RunJobAndWait(job *Job) *Job {
	j.m.Lock()
	j.jobList[job.ID] = job
	j.m.Unlock()

	j.workerChannel <- job
	job.Wait()

	return job
}

// RunJobsInSequence method
func (j *JobsManager) RunJobsInSequence(jobs ...*Job) []*Job {
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
		j.jobList[job.ID] = job
		go func(job *Job) {
			j.RunJobAndWait(job)
			wg.Done()
		}(job)
	}

	wg.Wait()

	return jobs
}

// StopJob method
func (j *JobsManager) StopJob(id string) *Job {
	j.m.Lock()
	defer j.m.Unlock()
	job := j.jobList[id]

	j.cancelChannel <- job

	return job
}

// GetJobs method
func (j *JobsManager) GetJobs() map[string]*Job {
	return j.jobList
}

func (j *JobsManager) registerWorker() {
	for {
		select {
		case job := <-j.workerChannel:
			job.Status = Running
			job.result.value, job.result.err = job.run()
			job.Status = Done

			close(job.done)

		case job := <-j.cancelChannel:
			job.Status = Cancelled
			job.result = JobResult{
				err: errCancelled,
			}
			close(job.done)
		}
	}
}
