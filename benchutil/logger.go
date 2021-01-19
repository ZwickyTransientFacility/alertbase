package benchutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type BenchmarkLogger struct {
	Env BenchmarkEnvironment

	logsLock sync.RWMutex
	logs     map[string]*fileSyncWriter
	dir      string

	closed bool

	extra interface{}
}

func NewBenchmarkLogger(env BenchmarkEnvironment, dst string) (*BenchmarkLogger, error) {
	err := os.MkdirAll(dst, 0755)
	if err != nil {
		return nil, err
	}

	l := &BenchmarkLogger{
		Env:  env,
		dir:  dst,
		logs: make(map[string]*fileSyncWriter),
	}

	err = l.WriteMeta("META.json", nil)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (bl *BenchmarkLogger) WriteMeta(label string, data interface{}) error {
	meta, err := os.Create(filepath.Join(bl.dir, label))
	if err != nil {
		return err
	}
	defer meta.Close()

	bl.extra = data
	metaContent, err := json.MarshalIndent(&bl.Env, "", "  ")
	if err != nil {
		return err
	}
	_, err = meta.Write(metaContent)
	if err != nil {
		return err
	}
	return nil
}

func (bl *BenchmarkLogger) ObserveInt(qty int, label string) error {
	return bl.write([]byte(strconv.Itoa(qty)), label)
}

func (bl *BenchmarkLogger) ObserveDuration(dur time.Duration, label string) error {
	return bl.write([]byte(strconv.FormatInt(int64(dur), 10)+"ns"), label)
}

func (bl *BenchmarkLogger) StartObservation(obsLabel, label string) (stopper func() error, err error) {
	start := time.Now()
	err = bl.write([]byte("start "+obsLabel), label)
	if err != nil {
		return nil, err
	}
	return func() error {
		dur := time.Since(start)
		msg := fmt.Sprintf("end   %s\t%d", dur)
		return bl.write([]byte(msg), label)
	}, nil
}

func (bl *BenchmarkLogger) write(msg []byte, label string) error {
	var err error
	bl.logsLock.RLock()
	f := bl.logs[label]
	bl.logsLock.RUnlock()

	if f == nil {
		// Relock, this time exclusively, because we'll want to write.
		bl.logsLock.Lock()
		if bl.closed {
			return errors.New("closed")
		}
		f = bl.logs[label]
		if f == nil {
			f, err = newFileSyncWriter(filepath.Join(bl.dir, label))
			if err != nil {
				bl.logsLock.Unlock()
				return err
			}
			bl.logs[label] = f
		}
		bl.logsLock.Unlock()
	}
	msg = append(msg, '\n')
	msg = append([]byte(time.Now().Format(time.RFC3339Nano)+"\t"), msg...)
	return f.write(msg)
}

func (bl *BenchmarkLogger) Close() error {
	bl.logsLock.Lock()
	defer bl.logsLock.Unlock()
	for _, w := range bl.logs {
		w.Close()
	}
	return nil
}

type fileSyncWriter struct {
	msgs chan []byte
	f    *os.File

	exitSig chan struct{}
	exited  chan struct{}
}

func newFileSyncWriter(path string) (*fileSyncWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	w := &fileSyncWriter{
		msgs:    make(chan []byte),
		f:       f,
		exitSig: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	go w.handleWrites()
	return w, nil
}

func (w *fileSyncWriter) write(msg []byte) error {
	select {
	case w.msgs <- msg:
		return nil
	case <-w.exited:
		return errors.New("shut down before write")
	}
}

func (w *fileSyncWriter) Close() error {
	close(w.exitSig)
	<-w.exited
	return w.f.Close()
}

func (w *fileSyncWriter) handleWrites() {
	defer func() {
		close(w.exited)
	}()

	for {
		select {
		case msg := <-w.msgs:
			w.f.Write(msg)
		case <-w.exitSig:
			return
		}
	}
}
