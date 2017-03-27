package main

import "time"
import "fmt"

func main() {
    time1 := 10
    timeChan := time.NewTimer(time.Millisecond*time1).C
    
    tickChan := time.NewTicker(time.Millisecond * 400).C
    
    doneChan := make(chan bool)
    go func() {
        time.Sleep(time.Second * 2)
        doneChan <- true
    }()
    
    for {
        select {
        case <- timeChan:
            fmt.Println("Timer expired")
            time1 = 500
        case <- tickChan:
            fmt.Println("Ticker ticked")
        case <- doneChan:
            fmt.Println("Done")
            return
      }
    }
}