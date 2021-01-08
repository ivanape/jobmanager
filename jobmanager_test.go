package jobmanager

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// StructResult struct
type StructResult struct {
	Name     string
	LastName string
}

var (
	defaultError        = errors.New("default error message")
	defaultStringResult = "hello world!"
	anotherError        = errors.New("another error message")
	anotherStringResult = "bye bye world!"
	defaultStructResult = StructResult{
		Name:     "Name",
		LastName: "LastName",
	}
)

func TestJobsManager_RunJobAndWait(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job := createBasicJob()
	job, _ = jobsManager.RunJobAndWait(job)

	assert.Equal(t, Done, job.Status)
}

func TestJobsManager_RunJobsInSequence(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job1 := createBasicJob()
	job2 := createBasicJob()
	_ = jobsManager.RunJobsInSequence(job1, job2)

	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobsInParallel(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job1 := createBasicJob()
	job2 := createBasicJob()
	_ = jobsManager.RunJobsInParallel(job1, job2)

	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobAndWaitString(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()
	job := createBasicJob()

	job, _ = jobsManager.RunJobAndWait(job)

	assert.Equal(t, defaultStringResult, job.result.value.(string))
}

func TestJobsManager_RunJobAndWaitError(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()
	job := createErrorJob()

	job, _ = jobsManager.RunJobAndWait(job)

	assert.Equal(t, false, errors.Is(job.result.err, defaultError))
}

func TestJobsManager_RunJobAndWaitStringError(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()
	errorJob := createJobStringError()

	job, _ := jobsManager.RunJobAndWait(errorJob)

	assert.Equal(t, defaultStringResult, job.result.value.(string))
	assert.NotEqual(t, anotherStringResult, job.result.value.(string))
	assert.Equal(t, true, errors.Is(job.result.err, defaultError))
	assert.Equal(t, false, errors.Is(job.result.err, anotherError))
}

func TestJobsManager_RunJobAndWaitStructError(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()
	errorJob := createJobStructError()

	job, _ := jobsManager.RunJobAndWait(errorJob)

	assert.EqualValues(t, defaultStructResult, job.result.value.(StructResult))
	assert.Equal(t, true, errors.Is(job.result.err, anotherError))
}

func createBasicJob() *Job {
	job := newJob()
	_ = job.do(
		func(message string) string {
			fmt.Printf("Hello %s\n", message)
			time.Sleep(2 * time.Second)
			return defaultStringResult
		}, "world!")
	return job
}

func createJobStringError() *Job {
	job := newJob()
	_ = job.do(
		func(message string) (string, error) {
			time.Sleep(2 * time.Second)
			return defaultStringResult, defaultError
		}, "error function")
	return job
}

func createJobStructError() *Job {
	job := newJob()
	_ = job.do(
		func(message string) (StructResult, error) {
			time.Sleep(2 * time.Second)
			return defaultStructResult, anotherError
		}, "error function")
	return job
}

func createErrorJob() *Job {
	job := newJob()
	_ = job.do(
		func(message string) error {
			time.Sleep(2 * time.Second)
			return anotherError
		}, "error function")
	return job
}
