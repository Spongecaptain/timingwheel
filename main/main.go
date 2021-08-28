package main

import (
	"fmt"
	"time"
	"timingwheel"
)

type mySchedule struct{}

// 这里 t 默认传入的就是当前时间
func (s *mySchedule) Next(t time.Time) time.Time {
	return t.Add(time.Second * 1)
}

func main() {
	tw := timingwheel.NewTimingWheel(time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()
	tw.ScheduleFunc(&mySchedule{}, func() {
		fmt.Println("hello world")
	})
	select {}
}
