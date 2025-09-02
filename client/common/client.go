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

// StartClientLoop Load bets from CSV file and send them in batches
func (c *Client) StartClientLoop() {
	bets, err := c.loadBetsFromCSV()
	if err != nil {
		log.Errorf("action: load_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: load_csv | result: success | client_id: %v | total_bets: %d", c.config.ID, len(bets))

	batchSize := c.config.BatchMaxAmount
	totalBets := len(bets)
	
	for i := 0; i < totalBets; i += batchSize {
		end := i + batchSize
		if end > totalBets {
			end = totalBets
		}
		
		batch := bets[i:end]
		
		err := c.sendBatch(batch)
		if err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | batch_start: %d | batch_size: %d | error: %v", 
				c.config.ID, i, len(batch), err)
			return
		}
		
		if end < totalBets {
			time.Sleep(c.config.LoopPeriod)
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v | total_batches_sent: %d", 
		c.config.ID, (totalBets+batchSize-1)/batchSize)

	// Notify server that this agency finished sending bets
	err = c.notifyFinishBets()
	if err != nil {
		log.Errorf("action: notify_finish_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Query winners for this agency
	err = c.queryWinners()
	if err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
}

// loadBetsFromCSV loads bets from the CSV file for this agency
func (c *Client) loadBetsFromCSV() ([]Bet, error) {
	filename := fmt.Sprintf("/.data/agency-%s.csv", c.config.ID)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var bets []Bet
	reader := csv.NewReader(file)
	
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
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
		bets = append(bets, bet)
	}
	
	return bets, nil
}

// sendBatch sends a batch of bets to the server and waits for response
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

	response, err := c.receiveResponse()
	if err != nil {
		log.Errorf("action: receive_response | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
		return err
	}

	if response == "OK" {
		log.Infof("action: batch_enviado | result: success | cantidad: %d", len(bets))
	} else {
		log.Errorf("action: batch_enviado | result: fail | cantidad: %d | response: %s", len(bets), response)
	 	return fmt.Errorf("server rejected batch: %s", response)
	}

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

// notifyFinishBets sends a FINISH_BETS message to notify the server
// that this agency has finished sending all its bets
func (c *Client) notifyFinishBets() error {
	err := c.createClientSocket()
	if err != nil {
		return err
	}
	defer c.cleanup()

	proto := Protocol{}
	finishMessage := proto.SerializeFinishBets(c.config.ID)
	messageBytes := []byte(finishMessage)

	totalWritten := 0
	for totalWritten < len(messageBytes) {
		n, err := c.conn.Write(messageBytes[totalWritten:])
		if err != nil {
			log.Errorf("action: notify_finish_bets | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
			return err
		}
		totalWritten += n
	}

	response, err := c.receiveResponse()
	if err != nil {
		log.Errorf("action: receive_finish_response | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
		return err
	}

	if response == "OK" {
		log.Infof("action: notify_finish_bets | result: success | client_id: %v", c.config.ID)
	} else {
		log.Errorf("action: notify_finish_bets | result: fail | client_id: %v | response: %s", c.config.ID, response)
		return fmt.Errorf("server rejected finish notification: %s", response)
	}

	return nil
}

// queryWinners sends a QUERY_WINNERS message to get the list of winners
// for this agency and logs the result
func (c *Client) queryWinners() error {
	err := c.createClientSocket()
	if err != nil {
		return err
	}
	defer c.cleanup()

	proto := Protocol{}
	queryMessage := proto.SerializeQueryWinners(c.config.ID)
	messageBytes := []byte(queryMessage)

	totalWritten := 0
	for totalWritten < len(messageBytes) {
		n, err := c.conn.Write(messageBytes[totalWritten:])
		if err != nil {
			log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
			return err
		}
		totalWritten += n
	}

	response, err := c.receiveResponse()
	if err != nil {
		log.Errorf("action: receive_winners_response | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
		return err
	}

	// Parse winners response - format: "WINNERS#count#doc1#doc2#..."
	parts := strings.Split(response, "#")
	if len(parts) < 2 || parts[0] != "WINNERS" {
		log.Errorf("action: query_winners | result: fail | client_id: %v | invalid_response: %s", c.config.ID, response)
		return fmt.Errorf("invalid winners response: %s", response)
	}

	winnersCount := parts[1]
	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %s", winnersCount)

	return nil
}