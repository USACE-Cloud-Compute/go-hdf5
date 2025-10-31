package util

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
)

var PipeRoot = "/workspaces/pipes"

type AsyncInput struct {
	Filepath  string
	Datapath  string
	Namedpipe string
	Vars      map[string]string
}

func ClosePipes(pipes []string) {
	for _, p := range pipes {
		os.Remove(p)
	}
}

func GetPipe() (string, error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", PipeRoot, uuid.String()), err
}

func RunAsync(hdfType string, input AsyncInput, dset interface{}, wg *sync.WaitGroup) error {
	//var raslibCmd string = "/workspaces/model-library/go-raslib/raslib"
	hdfutilCmd := os.Getenv("HDFDUTILCMD")
	if hdfutilCmd == "" {
		log.Println("Missing path to raslib executable")
		return errors.New("Missing path to raslib executable")
	}

	os.Remove(input.Namedpipe)
	var err error
	err = syscall.Mkfifo(input.Namedpipe, 0666)
	if err != nil {
		log.Printf("Failed to create pipe %s: %s\n", input.Namedpipe, err) //@TODO  might want to bail at this point!!!!!
	}

	env := []string{
		fmt.Sprintf("AWS_REGION=%s", os.Getenv("AWS_REGION")),
		fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", os.Getenv("AWS_ACCESS_KEY_ID")),
		fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", os.Getenv("AWS_SECRET_ACCESS_KEY")),
		fmt.Sprintf("LD_LIBRARY_PATH=%s", os.Getenv("LD_LIBRARY_PATH")),
		fmt.Sprintf("NAMEDPIPE=%s", input.Namedpipe),
		fmt.Sprintf("FILEPATH=%s", input.Filepath),
		fmt.Sprintf("DATAPATH=%s", input.Datapath),
	}

	var proc *exec.Cmd
	switch hdfType {
	case "DSET": //dataset
		env = append(env, varsToEnv(input.Vars)...)
		proc = exec.Command(hdfutilCmd, hdfType)
	case "ATTR": //dataset attributes
		env = append(env, varsToEnv(input.Vars)...)
		proc = exec.Command(hdfutilCmd, hdfType)
	}

	proc.Stdout = os.Stdout
	proc.Stderr = os.Stderr
	proc.Env = env
	err = proc.Start()
	if err != nil {
		log.Println(err)
	}

	file, err := os.OpenFile(input.Namedpipe, os.O_CREATE, os.ModeNamedPipe)
	if err != nil {
		log.Println(err)
	}
	log.Printf("Opened Pipe %s\n", input.Namedpipe)
	dec := gob.NewDecoder(file)
	err = dec.Decode(dset)
	if err == io.EOF {
		log.Println("Finished with EOF")
	} else if err != nil {
		log.Printf("Finished with err: %s\n", err)
	}
	log.Printf("Finished async call for pipe %s\n", input.Namedpipe)
	if wg != nil {
		wg.Done()
	}
	proc.Wait()

	return err
}

func varsToEnv(vars map[string]string) []string {
	env := make([]string, len(vars))
	for k, v := range vars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	return env
}

func handleAsync() {
	args := os.Args
	hdfType := args[1]
	namedpipe := os.Getenv("NAMEDPIPE")
	hdfFilePath := os.Getenv("FILEPATH")
	hdfDataPath := os.Getenv("DATAPATH")

	fmt.Printf("Filepath: %s\n", hdfFilePath)
	fmt.Printf("Datapath: %s\n", hdfDataPath)
	fmt.Println(os.Environ())

	switch hdfType {
	case "DSET":
		fmt.Println("Starting HDFDataset Read")
		AsyncHdfRead(hdfFilePath, namedpipe, hdfDataPath)
		//@TODO enable async attr reading....
		// case "ATTR":
		// 	layer := os.Getenv("RASLAYER")
		// 	version := os.Getenv("RASVERSION")
		// 	flowArea := os.Getenv("RASFLOWAREA")
		// 	fmt.Println("-------------------------------------------")
		// 	fmt.Println(hdfFilePath)
		// 	fmt.Println(layer)
		// 	fmt.Println(version)
		// 	fmt.Println("-------------------------------------------")
		// 	fmt.Println("Starting DA Read")
		// 	AsyncHdfReadAttr(hdfFilePath, namedpipe, version, layer, flowArea)
	}
}

// @TODO force errors in async reads and make sure child process is fully closed and errors and somehow handled
func AsyncHdfRead(url string, namedpipe string, datapath string) {
	options, err := getOptions()
	if err != nil {
		log.Fatal(err)
	}

	//options.ReadOnCreate = true

	data, err := NewHdfDataset(datapath, options)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	dims := os.Getenv("HDFDIMSONLY")
	if dims != "true" {
		subset := os.Getenv("HDFSUBSET")
		if subset != "" {
			rowrange, colrange, err := csvToRanges(subset)
			err = data.ReadSubset(rowrange, colrange)
			if err != nil {
				log.Fatalf("error reading data subset: %s\n", err)
			}

		} else {
			err := data.Read()
			if err != nil {
				log.Fatalf("error reading data: %s\n", err)
			}
		}
	}

	pf, err := os.OpenFile(namedpipe, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Fatalf("error opening file: %v\n", err)
	}
	enc := gob.NewEncoder(pf)
	enc.Encode(data.Data)
	pf.Close()
}

/*
func AsyncHdfReadAttr(url string, datapath string, namedpipe string) {
	f, err := OpenFileFromEnv(url)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	data, err := ReadCompoundAttributes(f, datapath)

	pf, err := os.OpenFile(namedpipe, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Fatalf("error opening file: %s", err)
	}
	defer pf.Close()

	enc := gob.NewEncoder(pf)
	enc.Encode(data)

}
*/

func getOptions() (HdfReadOptions, error) {

	typeInt, err := strconv.Atoi(os.Getenv("HDFOPTTYPE"))
	if err != nil {
		return HdfReadOptions{}, err
	}

	options := HdfReadOptions{
		Dtype:        reflect.Kind(typeInt),
		ReadOnCreate: true,
	}
	return options, nil
}

func csvToRanges(csv string) ([]int, []int, error) {
	ranges := make([]int, 4)
	vals := strings.Split(csv, ",")
	if len(vals) == 4 {
		for i, v := range vals {
			vi, err := strconv.Atoi(v)
			if err != nil {
				return nil, nil, err
			}
			ranges[i] = vi
		}
		fmt.Println(ranges)
		return ranges[0:2], ranges[2:4], nil
	}
	return nil, nil, errors.New("Invalid range")
}
