package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	messageSize    = 4096
	workers        = 150
	total_messages = 10000000
	msg_per_worker = total_messages / workers
	natsURL        = "nats://localhost:4222"
	streamName     = "benchstream"
	subject        = "benchsubject"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	_ = js.DeleteStream(streamName)
	// Create Stream (if it doesn't exist)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Consume and verify messages
	sub, err := js.SubscribeSync(subject)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	expected_msgs := int64(workers * msg_per_worker)

	var received int64
	startTime := time.Now()

	timeoutDuration := 10 * time.Second
	timeout := time.After(timeoutDuration)

	var subWg sync.WaitGroup
	subWg.Add(1)
	go func() {
		defer subWg.Done()
		for {
			select {
			case <-timeout:
				fmt.Println("Timed out after 10 seconds of inactivity")
				return // or break, depending on your needs
			default:
				msg, err := sub.NextMsg(1 * time.Second)
				if err == nats.ErrTimeout {
					fmt.Println("Received NATS Timeout!")
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				msg.Ack()
				received++
				if received == expected_msgs {
					fmt.Println("Received all messages")
					return
				}

				// reset timeout
				timeout = time.After(timeoutDuration)
				fmt.Printf("Received message #%d\n", received)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)

	pubStart := time.Now()
	// Produce messages
	for i := 0; i < workers; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < msg_per_worker; j++ {
				msg := generateLargeMessage(j, goroutineID, messageSize)
				_, err := js.Publish(subject, []byte(msg))
				if err != nil {
					log.Printf("Error publishing message: %v", err)
				}
			}
		}(i)
	}

	wg.Wait() // Wait for all publishing goroutines to finish
	pubDuration := time.Since(pubStart)
	subWg.Wait()
	//	for {
	//		msg, err := sub.NextMsg(1 * time.Second)
	//		//if err == nats.ErrTimeout {
	//		//	break // Exit loop after 1 second of inactivity
	//		//}
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//		msg.Ack()
	//		received++
	//		if received == int64(total_messages) {
	//			fmt.Println("Received all messages")
	//			break
	//		}
	//		fmt.Printf("Received message\n") //: %s\n", string(msg.Data))
	//	}

	duration := time.Since(startTime)

	fmt.Printf("Configuration: [size:%d] [workers:%d] [total_messages:%d]\n", messageSize, workers, total_messages)
	fmt.Printf("Published %d messages in %v\n", expected_msgs, pubDuration)
	fmt.Printf("Received %d messages in %v\n", received, duration)

	if received != expected_msgs {
		log.Fatalf("Some messages were lost. Expected: %d, Received: %d", expected_msgs, received)
	} else {
		fmt.Println("All messages were received successfully.")
	}
}

func generateLargeMessage(id int, goroutine int, size int) string {
	base := fmt.Sprintf("Message %d/%d: ", id, goroutine)
	padding := make([]byte, size-len(base))
	for i := range padding {
		padding[i] = 'x'
	}
	return base + string(padding)
}
