package jobmanager

import (
	"errors"
	"log"
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
	doneChannel   chan *Job
	cancelChannel chan *Job
	workerSize    int
}

// NewJobManager method
func NewJobManager() *JobsManager {
	return &JobsManager{
		jobList:       make(map[string]*Job),
		workerChannel: make(chan *Job),
		doneChannel:   make(chan *Job),
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
	defer j.m.Unlock()

	j.jobList[job.ID] = job
	j.workerChannel <- job

	return job, nil
}

// RunJobAndWait method
func (j *JobsManager) RunJobAndWait(job *Job) (*Job, error) {
	j.m.Lock()
	defer j.m.Unlock()

	j.jobList[job.ID] = job
	j.workerChannel <- job

	job.Wait()

	return job, nil
}

// RunJobsInSequence method
func (j *JobsManager) RunJobsInSequence(jobs ...*Job) error {
	for _, job := range jobs {
		j.RunJob(job)
		job.Wait()
	}

	return nil
}

// RunJobsInParallel method
func (j *JobsManager) RunJobsInParallel(jobs ...*Job) error {
	// Run jobs in parallel
	jobsRunning := 0
	done := make(chan *Job, len(jobs))
	defer close(done)

	j.m.Lock()
	for _, job := range jobs {
		jobsRunning++
		j.jobList[job.ID] = job

		// Run the job in it's own goroutine
		go func(job *Job) {
			defer func() { done <- job }()
			job.Run()
		}(job)
	}
	j.m.Unlock()

	for jobsRunning > 0 {
		select {
		case job := <-done:
			jobsRunning--
			j.doneChannel <- job
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
	log.Printf("%v", j.jobList)
	return j.jobList
}

func (j *JobsManager) registerWorker() {
	for {
		select {
		case job := <-j.workerChannel:
			job.Status = Running
			value, err := job.Run()

			log.Printf("%v %v", value, err)

			if job.result.err != errCancelled {
				j.doneChannel <- job
			}

		case job := <-j.doneChannel:
			job.Status = Done
			close(job.done)
			log.Printf("Job %s is done\n", job.ID)

		case job := <-j.cancelChannel:
			job.Status = Cancelled
			job.result = JobResult{
				err: errCancelled,
			}
			close(job.done)
			log.Printf("Job %s is cancelled\n", job.ID)
		}
	}
}
