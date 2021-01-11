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
	Result  JobResult
	funcs   map[string]interface{}   // Map for the function task store
	fparams map[string][]interface{} // Map for function and  params of function
	jobFunc string
	done    chan interface{}
}

// JobResult struct
type JobResult struct {
	Value interface{}
	Err   error
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

// do method
func (j *Job) do(jobFun interface{}, params ...interface{}) error {
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

// wait method
func (j *Job) wait() {
	<-j.done
}

func (j *Job) resetState() {
	if j.Status == Done {
		j.Status = Pending
		j.done = make(chan interface{})
	}
}

// run method
func (j *Job) run() (interface{}, error) {
	return callJobFuncWithParams(j.funcs[j.jobFunc], j.fparams[j.jobFunc])
}

func getFunctionName(fn interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

func callJobFuncWithParams(jobFunc interface{}, params []interface{}) (interface{}, error) {
	f := reflect.ValueOf(jobFunc)
	if len(params) != f.Type().NumIn() {
		return nil, ErrParamsNotAdapted
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}

	res := f.Call(in)

	if len(res) == 1 {
		value := res[0].Interface()
		if checkIfIsError(value) {
			return nil, value.(error)
		}
		return value, nil
	}

	if len(res) >= 2 {
		value1 := res[0].Interface()
		value2 := res[1].Interface()

		if checkIfIsError(value1) {
			return value2, value1.(error)
		}

		if checkIfIsError(value2) {
			return value1, value2.(error)
		}
	}

	return nil, nil
}

func checkIfIsError(value interface{}) bool {
	ok := false
	if value != nil {
		_, ok = value.(error)
	}
	return ok
}

func (j *Job) closeDoneChannel() {
	channelIsOpen := true
	select {
	case _, channelIsOpen = <-j.done:
	default:
	}

	if channelIsOpen {
		close(j.done)
	}
}

func (j *Job) isCancelled() bool {
	return errors.Is(j.Result.Err, ErrJobCancelled)
}
