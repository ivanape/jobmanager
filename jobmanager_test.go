package jobmanager

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestJobsManager_RunJobAndWait(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job := createBasicJob("1")
	job, _ = jobsManager.RunJobAndWait(job)

	assert.Equal(t, job.Status, Done)
}

func TestJobsManager_RunJobsInSequence(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job1 := createBasicJob("1")
	job2 := createBasicJob("2")
	_ = jobsManager.RunJobsInSequence(job1, job2)

	assert.Equal(t, job1.Status, Done)
	assert.Equal(t, job2.Status, Done)
}

func TestJobsManager_RunJobsInParallel(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job1 := createBasicJob("1")
	job2 := createBasicJob("2")
	_ = jobsManager.RunJobsInParallel(job1, job2)

	assert.Equal(t, job1.Status, Done)
	assert.Equal(t, job2.Status, Done)
}

func TestJobsManager_RunJobAndWaitError(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()
	errorJob := createErrorJob("1")

	_, err := jobsManager.RunJobAndWait(errorJob)

	fmt.Printf("%v\n", err)
}

func createBasicJob(id string) *Job {
	job := NewJob(id)
	_ = job.Do(
		func(message string) {
			fmt.Printf("Hello %s\n", message)
			time.Sleep(2 * time.Second)
		}, "world!")
	return job
}

func createErrorJob(id string) *Job {
	job := NewJob(id)
	_ = job.Do(
		func(message string) error {
			time.Sleep(2 * time.Second)
			return errors.New(fmt.Sprintf("error in the job with message: %s", message))
		}, "error function")
	return job
}
