package jobmanager

import (
	"errors"
	"reflect"
	"runtime"
)

var (
	// ErrParamsNotAdapted error
	ErrParamsNotAdapted = errors.New("the number of params is not adapted")
	// ErrNotAFunction error
	ErrNotAFunction = errors.New("only functions can be schedule into the job queue")
	//ErrParameterCannotBeNil = errors.New("nil paramaters cannot be used with reflection")
)

// Status Job Status
type Status int

// Job struct
type Job struct {
	ID      string
	Status  Status
	result  JobResult
	funcs   map[string]interface{}   // Map for the function task store
	fparams map[string][]interface{} // Map for function and  params of function
	jobFunc string
	done    chan interface{}
}

// JobResult struct
type JobResult struct {
	message string
	err     error
}

const (
	// Pending state
	Pending Status = iota
	// Running state
	Running
	// Done state
	Done
	// Cancelled state
	Cancelled
)

// NewJob method
func NewJob(id string) *Job {
	return &Job{
		ID:      id,
		Status:  Pending,
		funcs:   make(map[string]interface{}),
		fparams: make(map[string][]interface{}),
		done:    make(chan interface{}),
	}
}

// Do method
func (j *Job) Do(jobFun interface{}, params ...interface{}) error {
	typ := reflect.TypeOf(jobFun)
	if typ.Kind() != reflect.Func {
		return ErrNotAFunction
	}
	fname := getFunctionName(jobFun)
	j.funcs[fname] = jobFun
	j.fparams[fname] = params
	j.jobFunc = fname

	return nil
}

// Run method
func (j *Job) Run() ([]reflect.Value, error) {
	result, err := callJobFuncWithParams(j.funcs[j.jobFunc], j.fparams[j.jobFunc])
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Wait method
func (j *Job) Wait() {
	<-j.done
}

func getFunctionName(fn interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func callJobFuncWithParams(jobFunc interface{}, params []interface{}) ([]reflect.Value, error) {
	f := reflect.ValueOf(jobFunc)
	if len(params) != f.Type().NumIn() {
		return nil, ErrParamsNotAdapted
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	return f.Call(in), nil
}
