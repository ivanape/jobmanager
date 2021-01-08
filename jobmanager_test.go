package jobmanager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestJobsManager_RunJobAndWait(t *testing.T) {

	jobsManager := NewJobManager()
	jobsManager.StartManager()

	job := NewJob("1")
	_ = job.Do(
		func(message string) {
			fmt.Printf("Hello %s", message)
			time.Sleep(2 * time.Second)
		}, "world!")

	job, _ = jobsManager.RunJobAndWait(job)

	assert.Equal(t, job.Status, Done)

}
