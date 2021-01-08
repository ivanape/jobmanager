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
func NewJobManager() *JobsManager {
	return &JobsManager{
		jobList:       make(map[string]*Job),
		workerChannel: make(chan *Job),
		cancelChannel: make(chan *Job),
		workerSize:    100, //By default allow 100 concurrent tasks
	}
}

// StartManager method
func (j *JobsManager) StartManager() {
	for i := 0; i < j.workerSize; i++ {
		go j.registerWorker()
	}
}

// RunJob method
func (j *JobsManager) RunJob(job *Job) (*Job, error) {
	j.m.Lock()
	j.jobList[job.ID] = job
	j.m.Unlock()

	j.workerChannel <- job

	return job, nil
}

// RunJobAndWait method
func (j *JobsManager) RunJobAndWait(job *Job) (*Job, error) {
	j.m.Lock()
	j.jobList[job.ID] = job
	j.m.Unlock()

	j.workerChannel <- job
	job.wait()

	return job, nil
}

// RunJobsInSequence method
func (j *JobsManager) RunJobsInSequence(jobs ...*Job) error {
	for _, job := range jobs {
		j.RunJobAndWait(job)
	}

	return nil
}

// RunJobsInParallel method
func (j *JobsManager) RunJobsInParallel(jobs ...*Job) error {
	// run jobs in parallel
	jobsRunning := 0
	done := make(chan *Job, len(jobs))
	defer close(done)

	j.m.Lock()
	for _, job := range jobs {
		jobsRunning++
		j.jobList[job.ID] = job

		// run the job in it's own goroutine
		go func(job *Job) {
			defer func() {
				job.Status = Done
				done <- job
			}()
			job.run()
		}(job)
	}
	j.m.Unlock()

	for jobsRunning > 0 {
		select {
		case <-done:
			jobsRunning--
		}
	}

	return nil
}

// StopJob method
func (j *JobsManager) StopJob(id string) (*Job, error) {
	j.m.Lock()
	defer j.m.Unlock()
	job := j.jobList[id]

	j.cancelChannel <- job

	return job, nil
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
