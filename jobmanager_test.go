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
	defaultWorkerSize   = 100
	errDefault          = errors.New("default error message")
	defaultStringResult = "hello world!"
	errAnother          = errors.New("another error message")
	anotherStringResult = "bye bye world!"
	defaultStructResult = StructResult{
		Name:     "Name",
		LastName: "LastName",
	}
)

func TestJobsManager_Run(t *testing.T) {
	jobsManager := NewJobManager(defaultWorkerSize)

	f := func(message string) string {
		fmt.Printf("Hello %s\n", message)
		time.Sleep(2 * time.Second)
		return defaultStringResult
	}

	job, err := jobsManager.Run(f, "world!")
	job.wait()

	assert.Nil(t, err)
	assert.Equal(t, Done, job.Status)
}

func TestJobsManager_RunAndWait(t *testing.T) {
	jobsManager := NewJobManager(defaultWorkerSize)

	f := func(message string) string {
		fmt.Printf("Hello %s\n", message)
		time.Sleep(2 * time.Second)
		return defaultStringResult
	}

	job, err := jobsManager.RunAndWait(f, "world!")

	assert.Nil(t, err)
	assert.Equal(t, Done, job.Status)
}

func TestJobsManager_RunJobAndWait(t *testing.T) {

	jobsManager := NewJobManager(defaultWorkerSize)

	job := createBasicJob()
	job = jobsManager.RunJobAndWait(job)

	assert.Equal(t, Done, job.Status)
}

func TestJobsManager_RunJobsInSequence(t *testing.T) {

	jobsManager := NewJobManager(defaultWorkerSize)

	job1 := createBasicJob()
	job2 := createBasicJob()
	jobs := jobsManager.RunJobsInSequence(job1, job2)

	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobsInParallel(t *testing.T) {

	jobsManager := NewJobManager(defaultWorkerSize)

	job1 := createBasicJob()
	job2 := createBasicJob()
	jobs := jobsManager.RunJobsInParallel(job1, job2)

	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobAndWaitString(t *testing.T) {

	jobsManager := NewJobManager(defaultWorkerSize)

	job := createBasicJob()
	job = jobsManager.RunJobAndWait(job)

	assert.Equal(t, defaultStringResult, job.result.value.(string))
}

func TestJobsManager_RunJobAndWaitError(t *testing.T) {

	jobsManager := NewJobManager(defaultWorkerSize)

	job := createErrorJob()
	job = jobsManager.RunJobAndWait(job)

	assert.Equal(t, false, errors.Is(job.result.err, errDefault))
}

func TestJobsManager_RunJobAndWaitStringError(t *testing.T) {

	jobsManager := NewJobManager(defaultWorkerSize)

	errorJob := createJobStringError()

	job := jobsManager.RunJobAndWait(errorJob)

	assert.Equal(t, defaultStringResult, job.result.value.(string))
	assert.NotEqual(t, anotherStringResult, job.result.value.(string))
	assert.Equal(t, true, errors.Is(job.result.err, errDefault))
	assert.Equal(t, false, errors.Is(job.result.err, errAnother))
}

func TestJobsManager_ReRunSameJob(t *testing.T) {
	jobsManager := NewJobManager(defaultWorkerSize)

	job, err := NewJob(func() {
		fmt.Println("Hello world!")
	})
	if err != nil {
		panic(err)
	}

	jobsManager.RunJobAndWait(job)
	job2, err := jobsManager.RunAndWait(func() { fmt.Println("Hello world!") })
	jobsManager.RunJobsInSequence(job, job2, job, job2)
	jobsManager.RunJobsInParallel(job, job2, job, job2)

	assert.Equal(t, 2, len(jobsManager.jobList))
	assert.Equal(t, Done, job.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobAndWaitStructError(t *testing.T) {
	jobsManager := NewJobManager(defaultWorkerSize)
	errorJob := createJobStructError()

	job := jobsManager.RunJobAndWait(errorJob)

	assert.EqualValues(t, defaultStructResult, job.result.value.(StructResult))
	assert.Equal(t, true, errors.Is(job.result.err, errAnother))
}

func createBasicJob() *Job {
	job, _ := NewJob(func(message string) string {
		fmt.Printf("Hello %s\n", message)
		time.Sleep(2 * time.Second)
		return defaultStringResult
	}, "world!")

	return job
}

func createJobStringError() *Job {
	job, _ := NewJob(
		func(message string) (string, error) {
			time.Sleep(2 * time.Second)
			return defaultStringResult, errDefault
		}, "error function")
	return job
}

func createJobStructError() *Job {
	job, _ := NewJob(
		func(message string) (StructResult, error) {
			time.Sleep(2 * time.Second)
			return defaultStructResult, errAnother
		}, "error function")
	return job
}

func createErrorJob() *Job {
	job, _ := NewJob(
		func(message string) error {
			time.Sleep(2 * time.Second)
			return errAnother
		}, "error function")
	return job
}
