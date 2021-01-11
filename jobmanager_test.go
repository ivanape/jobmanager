package jobmanager

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Customer struct
type Customer struct {
	Name     string
	LastName string
}

var (
	defaultWorkerSize   = 100
	errDefault          = errors.New("default error message")
	defaultStringResult = "hello world!"
	errAnother          = errors.New("another error message")
	anotherStringResult = "bye bye world!"
	defaultStructValue  = Customer{
		Name:     "Name",
		LastName: "LastName",
	}
	jobsManager = NewJobManager(defaultWorkerSize)
)

func TestJobsManager_Run(t *testing.T) {

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
	job := createBasicJob()
	jobsManager.RunJobAndWait(job)

	job2 := createJobStructParam()
	jobsManager.RunJobAndWait(job2)

	assert.Equal(t, Done, job.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobsInSerial(t *testing.T) {
	job1 := createBasicJob()
	job2 := createBasicJob()
	jobs := jobsManager.RunJobsInSerial(job1, job2)

	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_StopJobsInSerial(t *testing.T) {
	job1 := createBasicJob()
	job2 := createBasicJob()
	go jobsManager.RunJobsInSerial(job1, job2)

	jobsManager.StopJob(job2)

	time.Sleep(5 * time.Second)

	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Cancelled, job2.Status)
}

func TestJobsManager_RunJobsInParallel(t *testing.T) {
	job1 := createBasicJob()
	job2 := createBasicJob()
	jobs := jobsManager.RunJobsInParallel(job1, job2)

	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, Done, job1.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobAndWaitString(t *testing.T) {
	job := createBasicJob()
	job = jobsManager.RunJobAndWait(job)

	assert.Equal(t, defaultStringResult, job.result.value.(string))
}

func TestJobsManager_RunJobAndWaitError(t *testing.T) {
	job := createErrorJob()
	job = jobsManager.RunJobAndWait(job)

	assert.Equal(t, false, errors.Is(job.result.err, errDefault))
}

func TestJobsManager_RunJobAndWaitStringError(t *testing.T) {
	errorJob := createJobStringError()

	job := jobsManager.RunJobAndWait(errorJob)

	assert.Equal(t, defaultStringResult, job.result.value.(string))
	assert.NotEqual(t, anotherStringResult, job.result.value.(string))
	assert.Equal(t, true, errors.Is(job.result.err, errDefault))
	assert.Equal(t, false, errors.Is(job.result.err, errAnother))
}

func TestJobsManager_ReRunSameJob(t *testing.T) {
	newJobManager := NewJobManager(1)
	job, err := NewJob(func() {
		fmt.Println("Hello world!")
	})
	if err != nil {
		panic(err)
	}

	newJobManager.RunJobAndWait(job)
	job2, err := newJobManager.RunAndWait(func() { fmt.Println("Hello world!") })
	newJobManager.RunJobsInSerial(job, job2, job, job2)
	newJobManager.RunJobsInParallel(job, job2, job, job2)

	assert.Equal(t, 2, len(newJobManager.jobList))
	assert.Equal(t, Done, job.Status)
	assert.Equal(t, Done, job2.Status)
}

func TestJobsManager_RunJobAndWaitStructError(t *testing.T) {
	errorJob := createJobStructError()

	job := jobsManager.RunJobAndWait(errorJob)

	assert.EqualValues(t, defaultStructValue, job.result.value.(Customer))
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

func createJobStructParam() *Job {
	job, _ := NewJob(func(values Customer) Customer {
		fmt.Printf("Hello %s\n", values.Name)
		time.Sleep(2 * time.Second)
		return values
	}, defaultStructValue)

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
		func(message string) (Customer, error) {
			time.Sleep(2 * time.Second)
			return defaultStructValue, errAnother
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
