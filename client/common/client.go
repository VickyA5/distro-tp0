package common

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	BatchMaxAmount int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{config: config}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Infof("action: signal_received | result: success | client_id: %v | shutdown: graceful", client.config.ID)
		client.cleanup()
		os.Exit(0)
	}()

	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	log.Infof("action: connect | result: success | client_id: %v", c.config.ID)
	return nil
}

func (c *Client) cleanup() {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			log.Errorf("action: close_connection | result: fail | client_id: %v | error: %v", c.config.ID, err)
		} else {
			log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
		}
	}
}

// StartClientLoop Load bets from CSV file and send them in batches using streaming approach
func (c *Client) StartClientLoop() {
	filename := fmt.Sprintf("/.data/agency-%s.csv", c.config.ID)
	file, err := os.Open(filename)
	if err != nil {
		log.Errorf("action: open_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	batchSize := c.config.BatchMaxAmount
	batchCount := 0
	totalBetsSent := 0

	log.Infof("action: start_streaming | result: success | client_id: %v | batch_size: %d", c.config.ID, batchSize)

	for {
		batch, err := c.readNextBatch(reader, batchSize)
		if err != nil {
			log.Errorf("action: read_batch | result: fail | client_id: %v | batch_count: %d | error: %v", 
				c.config.ID, batchCount, err)
			return
		}

		if len(batch) == 0 {
			break // No more data to read
		}

		err = c.sendBatch(batch)
		if err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | batch_count: %d | batch_size: %d | error: %v", 
				c.config.ID, batchCount, len(batch), err)
			return
		}

		batchCount++
		totalBetsSent += len(batch)

		if len(batch) == batchSize {
			time.Sleep(c.config.LoopPeriod)
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v | total_batches_sent: %d | total_bets_sent: %d", 
		c.config.ID, batchCount, totalBetsSent)
}

// readNextBatch reads the next batch of bets from the CSV reader
func (c *Client) readNextBatch(reader *csv.Reader, batchSize int) ([]Bet, error) {
	var batch []Bet
	
	for len(batch) < batchSize {
		record, err := reader.Read()
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			return nil, err
		}
		
		if len(record) != 5 {
			continue // Skip malformed records
		}
		
		bet := Bet{
			Agency:    c.config.ID,
			FirstName: strings.Split(record[0], " ")[0],
			LastName:  strings.Join(strings.Split(record[0], " ")[1:], " ") + " " + record[1],
			Document:  record[2],
			Birthdate: record[3],
			Number:    record[4],
		}
		batch = append(batch, bet)
	}
	
	return batch, nil
}

// sendBatch sends a batch of bets to the server without waiting for response
func (c *Client) sendBatch(bets []Bet) error {
	err := c.createClientSocket()
	if err != nil {
		return err
	}
	defer c.cleanup()

	proto := Protocol{}
	batchMessage := proto.SerializeBatch(bets)
	messageBytes := []byte(batchMessage)
	
	totalWritten := 0
	for totalWritten < len(messageBytes) {
		n, err := c.conn.Write(messageBytes[totalWritten:])
		if err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
			return err
		}
		totalWritten += n
	}

	log.Infof("action: batch_enviado | result: success | cantidad: %d", len(bets))
	return nil
}


// receiveResponse waits for server response
func (c *Client) receiveResponse() (string, error) {
	buffer := make([]byte, 1024)
	n, err := c.conn.Read(buffer)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(buffer[:n])), nil
}